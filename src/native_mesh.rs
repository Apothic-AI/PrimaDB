use crate::{
    ChangeSubscription, IceServerConfig, MeshConfig, MeshSignal, MeshSignalingMode,
    PeerRecommendation, Primadb, PrimadbError, Result, RouteEnvelope, RoutePayload, RouteTarget,
    Router, RouterConfig, SyncEnvelope, SyncFrame,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use webrtc::api::API;
use webrtc::api::APIBuilder;
use webrtc::data_channel::RTCDataChannel;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

#[derive(Debug, Clone)]
struct MeshOutbound {
    encoding: String,
    payload: JsonValue,
    awaiting: BTreeMap<String, bool>,
}

#[derive(Default)]
struct NativeMeshPeer {
    connection: Option<Arc<RTCPeerConnection>>,
    channel: Option<Arc<RTCDataChannel>>,
    pending_remote_ice: Vec<RTCIceCandidateInit>,
}

struct NativeWebRtcMeshState {
    db: Primadb,
    api: Arc<API>,
    router: Router,
    room: String,
    peer_id: String,
    relay_url: String,
    rtc_configuration: RTCConfiguration,
    relay_connected: AtomicBool,
    next_message_seq: AtomicU64,
    outbound: UnboundedSender<Message>,
    peers: Mutex<BTreeMap<String, NativeMeshPeer>>,
    inflight: Mutex<BTreeMap<String, MeshOutbound>>,
    recommendations: Mutex<BTreeMap<String, PeerRecommendation>>,
}

pub struct NativeWebRtcMesh {
    state: Arc<NativeWebRtcMeshState>,
    change_subscription: Option<ChangeSubscription>,
    writer_task: Option<JoinHandle<()>>,
    reader_task: Option<JoinHandle<()>>,
    change_task: Option<JoinHandle<()>>,
    retry_task: Option<JoinHandle<()>>,
}

impl NativeWebRtcMesh {
    pub async fn connect_with_config(db: Primadb, config: MeshConfig) -> Result<Self> {
        if !matches!(config.signaling, MeshSignalingMode::Relay) {
            return Err(PrimadbError::Message(
                "native mesh currently requires relay signaling".to_owned(),
            ));
        }
        let relay_url = config.relay_url.clone().ok_or_else(|| {
            PrimadbError::Message("native mesh requires a relay_url".to_owned())
        })?;

        let rtc_configuration = build_rtc_configuration(&config.effective_ice_servers());
        let api = Arc::new(APIBuilder::new().build());
        let (socket, _) = connect_async(&relay_url)
            .await
            .map_err(|error| PrimadbError::Message(error.to_string()))?;
        let (mut writer, mut reader) = socket.split();
        let (outbound, mut outbound_rx) = unbounded_channel::<Message>();

        let peer_id = format!("mesh:{}:{}", db.replica_id(), crate::clock::now_millis());
        let state = Arc::new(NativeWebRtcMeshState {
            db: db.clone(),
            api,
            router: Router::new(RouterConfig {
                peer_id: peer_id.clone(),
                default_channel: format!("mesh:{}", config.room),
                default_ttl: 6,
                max_seen_routes: db.limits().max_seen_routes,
            }),
            room: config.room,
            peer_id,
            relay_url: relay_url.clone(),
            rtc_configuration,
            relay_connected: AtomicBool::new(true),
            next_message_seq: AtomicU64::new(0),
            outbound,
            peers: Mutex::new(BTreeMap::new()),
            inflight: Mutex::new(BTreeMap::new()),
            recommendations: Mutex::new(BTreeMap::new()),
        });

        let writer_state = state.clone();
        let writer_task = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                if writer.send(message).await.is_err() {
                    writer_state.relay_connected.store(false, Ordering::SeqCst);
                    break;
                }
            }
        });

        let reader_state = state.clone();
        let reader_task = tokio::spawn(async move {
            while let Some(message) = reader.next().await {
                match message {
                    Ok(Message::Text(payload)) => {
                        let payload = payload.to_string();
                        let _ = handle_mesh_signaling_message(&reader_state, &payload).await;
                    }
                    Ok(Message::Binary(_)) | Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Frame(_)) => {}
                }
            }
            reader_state.relay_connected.store(false, Ordering::SeqCst);
        });

        let change_subscription = db.subscribe_changes();
        let change_receiver = change_subscription.receiver();
        let change_state = state.clone();
        let change_task = tokio::spawn(async move {
            while let Ok(event) = change_receiver.recv().await {
                if event.pending_ops > 0 {
                    let _ = flush_mesh_pending_state(&change_state).await;
                }
            }
        });

        let retry_interval = Duration::from_millis(config.retry_interval_ms.max(1));
        let retry_state = state.clone();
        let retry_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retry_interval);
            loop {
                interval.tick().await;
                if !retry_state.relay_connected.load(Ordering::SeqCst) {
                    continue;
                }
                let _ = announce_mesh_join_state(&retry_state).await;
                let _ = retry_mesh_inflight_state(&retry_state).await;
                let _ = flush_mesh_pending_state(&retry_state).await;
            }
        });

        send_mesh_presence_state(&state).await?;
        announce_mesh_join_state(&state).await?;

        Ok(Self {
            state,
            change_subscription: Some(change_subscription),
            writer_task: Some(writer_task),
            reader_task: Some(reader_task),
            change_task: Some(change_task),
            retry_task: Some(retry_task),
        })
    }

    pub fn peer_id(&self) -> String {
        self.state.peer_id.clone()
    }

    pub fn signaling_mode(&self) -> &'static str {
        "relay"
    }

    pub fn relay_url(&self) -> &str {
        &self.state.relay_url
    }

    pub fn relay_connected(&self) -> bool {
        self.state.relay_connected.load(Ordering::SeqCst)
    }

    pub async fn peer_count(&self) -> usize {
        self.state.peers.lock().await.len()
    }

    pub async fn open_peer_count(&self) -> usize {
        self.state
            .peers
            .lock()
            .await
            .values()
            .filter(|peer| peer.channel.as_ref().is_some_and(mesh_channel_is_open))
            .count()
    }

    pub async fn inflight_count(&self) -> usize {
        self.state.inflight.lock().await.len()
    }

    pub async fn recommended_peers(&self) -> Vec<PeerRecommendation> {
        self.state
            .recommendations
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn flush_pending(&self) -> Result<usize> {
        flush_mesh_pending_state(&self.state).await
    }

    pub async fn retry_inflight(&self) -> Result<usize> {
        retry_mesh_inflight_state(&self.state).await
    }

    pub async fn close(&mut self) {
        self.teardown().await;
    }

    async fn teardown(&mut self) {
        let _ = post_mesh_leave_signal_state(&self.state).await;
        self.state.relay_connected.store(false, Ordering::SeqCst);
        let _ = self.state.outbound.send(Message::Close(None));
        self.change_subscription.take();
        if let Some(task) = self.writer_task.take() {
            task.abort();
        }
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        if let Some(task) = self.change_task.take() {
            task.abort();
        }
        if let Some(task) = self.retry_task.take() {
            task.abort();
        }

        let peers = {
            let mut peers = self.state.peers.lock().await;
            std::mem::take(&mut *peers)
        };
        for (_, mut peer) in peers {
            if let Some(channel) = peer.channel.take() {
                let _ = channel.close().await;
            }
            if let Some(connection) = peer.connection.take() {
                let _ = connection.close().await;
            }
        }
    }
}

impl Drop for NativeWebRtcMesh {
    fn drop(&mut self) {
        self.state.relay_connected.store(false, Ordering::SeqCst);
        let _ = self.state.outbound.send(Message::Close(None));
        if let Some(task) = self.writer_task.take() {
            task.abort();
        }
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        if let Some(task) = self.change_task.take() {
            task.abort();
        }
        if let Some(task) = self.retry_task.take() {
            task.abort();
        }
    }
}

async fn handle_mesh_signaling_message(
    state: &Arc<NativeWebRtcMeshState>,
    payload: &str,
) -> Result<()> {
    let route: RouteEnvelope = serde_json::from_str(payload)?;
    let decision = state.router.accept(route.clone());
    if !decision.deliver {
        return Ok(());
    }

    let room = state.room.clone();
    let channel = format!("mesh:{room}");
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                store_peer_recommendations(
                    state,
                    vec![peer_recommendation_from_presence(&peer, &state.relay_url)],
                )
                .await;
                let in_room = peer.topics.iter().any(|topic| topic == &channel)
                    || peer
                        .metadata
                        .get("mesh_room")
                        .is_some_and(|candidate| candidate == &room);
                if in_room {
                    handle_mesh_signal(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer.peer_id,
                        },
                    )
                    .await?;
                }
            }
            RoutePayload::Signal {
                room: signal_room,
                payload,
            } => {
                if signal_room != room {
                    continue;
                }
                let signal: MeshSignal = serde_json::from_value(payload)?;
                handle_mesh_signal(state, signal).await?;
            }
            RoutePayload::PeerExchange { peers } => {
                store_peer_recommendations(state, peers.clone()).await;
                for recommendation in peers {
                    let peer = recommendation.peer;
                    let in_room = peer.topics.iter().any(|topic| topic == &channel)
                        || peer
                            .metadata
                            .get("mesh_room")
                            .is_some_and(|candidate| candidate == &room);
                    if in_room {
                        handle_mesh_signal(
                            state,
                            MeshSignal::Join {
                                room: room.clone(),
                                from: peer.peer_id,
                            },
                        )
                        .await?;
                    }
                }
            }
            RoutePayload::Batch { items } => {
                for item in items.into_iter().rev() {
                    pending.push(RoutePayload::from_batch_item(item));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn handle_mesh_signal(state: &Arc<NativeWebRtcMeshState>, signal: MeshSignal) -> Result<()> {
    let room = state.room.clone();
    let peer_id = state.peer_id.clone();
    match signal {
        MeshSignal::Join {
            room: join_room,
            from,
        } => {
            if join_room != room || from == peer_id {
                return Ok(());
            }
            if state.peers.lock().await.contains_key(&from) {
                return Ok(());
            }
            if peer_id < from {
                create_mesh_offer(state, from).await?;
            } else {
                announce_mesh_join_state(state).await?;
            }
        }
        MeshSignal::Offer {
            room: offer_room,
            from,
            to,
            sdp,
        } => {
            if offer_room != room || to != peer_id {
                return Ok(());
            }
            accept_mesh_offer(state, from, sdp).await?;
        }
        MeshSignal::Answer {
            room: answer_room,
            from,
            to,
            sdp,
        } => {
            if answer_room != room || to != peer_id {
                return Ok(());
            }
            accept_mesh_answer(state, from, sdp).await?;
        }
        MeshSignal::Ice {
            room: ice_room,
            from,
            to,
            candidate,
            sdp_mid,
            sdp_mline_index,
        } => {
            if ice_room != room || to != peer_id {
                return Ok(());
            }
            add_mesh_ice_candidate(state, from, candidate, sdp_mid, sdp_mline_index).await?;
        }
        MeshSignal::Leave {
            room: leave_room,
            from,
        } => {
            if leave_room != room || from == peer_id {
                return Ok(());
            }
            remove_mesh_peer_state(state, &from).await;
        }
    }
    Ok(())
}

async fn create_mesh_offer(state: &Arc<NativeWebRtcMeshState>, remote_peer: String) -> Result<()> {
    let connection = ensure_mesh_peer(state, &remote_peer).await?;
    let needs_channel = {
        let peers = state.peers.lock().await;
        peers
            .get(&remote_peer)
            .and_then(|peer| peer.channel.as_ref())
            .is_none()
    };
    if needs_channel {
        let channel = connection.create_data_channel("primadb", None).await.map_err(to_error)?;
        attach_mesh_channel_handlers(state, &remote_peer, channel).await;
    }

    let offer = connection.create_offer(None).await.map_err(to_error)?;
    connection
        .set_local_description(offer.clone())
        .await
        .map_err(to_error)?;
    post_mesh_signal_state(
        state,
        &MeshSignal::Offer {
            room: state.room.clone(),
            from: state.peer_id.clone(),
            to: remote_peer,
            sdp: offer.sdp,
        },
    )
    .await
}

async fn accept_mesh_offer(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: String,
    sdp: String,
) -> Result<()> {
    let connection = ensure_mesh_peer(state, &remote_peer).await?;
    let offer = RTCSessionDescription::offer(sdp).map_err(to_error)?;
    connection
        .set_remote_description(offer)
        .await
        .map_err(to_error)?;
    flush_pending_ice_candidates(state, &remote_peer).await?;

    let answer = connection.create_answer(None).await.map_err(to_error)?;
    connection
        .set_local_description(answer.clone())
        .await
        .map_err(to_error)?;
    post_mesh_signal_state(
        state,
        &MeshSignal::Answer {
            room: state.room.clone(),
            from: state.peer_id.clone(),
            to: remote_peer,
            sdp: answer.sdp,
        },
    )
    .await
}

async fn accept_mesh_answer(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: String,
    sdp: String,
) -> Result<()> {
    let connection = ensure_mesh_peer(state, &remote_peer).await?;
    let answer = RTCSessionDescription::answer(sdp).map_err(to_error)?;
    connection
        .set_remote_description(answer)
        .await
        .map_err(to_error)?;
    flush_pending_ice_candidates(state, &remote_peer).await
}

async fn add_mesh_ice_candidate(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: String,
    candidate: String,
    sdp_mid: Option<String>,
    sdp_mline_index: Option<u16>,
) -> Result<()> {
    let connection = ensure_mesh_peer(state, &remote_peer).await?;
    let candidate = RTCIceCandidateInit {
        candidate,
        sdp_mid,
        sdp_mline_index,
        username_fragment: None,
    };
    if connection.remote_description().await.is_none() {
        let mut peers = state.peers.lock().await;
        if let Some(peer) = peers.get_mut(&remote_peer) {
            peer.pending_remote_ice.push(candidate);
        }
        return Ok(());
    }
    connection.add_ice_candidate(candidate).await.map_err(to_error)
}

async fn flush_pending_ice_candidates(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
) -> Result<()> {
    let pending = {
        let mut peers = state.peers.lock().await;
        peers
            .get_mut(remote_peer)
            .map(|peer| std::mem::take(&mut peer.pending_remote_ice))
            .unwrap_or_default()
    };
    if pending.is_empty() {
        return Ok(());
    }
    let connection = ensure_mesh_peer(state, remote_peer).await?;
    for candidate in pending {
        connection
            .add_ice_candidate(candidate)
            .await
            .map_err(to_error)?;
    }
    Ok(())
}

async fn ensure_mesh_peer(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
) -> Result<Arc<RTCPeerConnection>> {
    if let Some(connection) = state
        .peers
        .lock()
        .await
        .get(remote_peer)
        .and_then(|peer| peer.connection.clone())
    {
        return Ok(connection);
    }

    let connection = Arc::new(
        state
            .api
            .new_peer_connection(state.rtc_configuration.clone())
            .await
            .map_err(to_error)?,
    );

    let remote_for_ice = remote_peer.to_owned();
    let state_for_ice = state.clone();
    connection.on_ice_candidate(Box::new(move |candidate: Option<RTCIceCandidate>| {
        let state_for_ice = state_for_ice.clone();
        let remote_for_ice = remote_for_ice.clone();
        Box::pin(async move {
            let Some(candidate) = candidate else {
                return;
            };
            let Ok(candidate) = candidate.to_json() else {
                return;
            };
            let _ = post_mesh_signal_state(
                &state_for_ice,
                &MeshSignal::Ice {
                    room: state_for_ice.room.clone(),
                    from: state_for_ice.peer_id.clone(),
                    to: remote_for_ice,
                    candidate: candidate.candidate,
                    sdp_mid: candidate.sdp_mid,
                    sdp_mline_index: candidate.sdp_mline_index,
                },
            )
            .await;
        })
    }));

    let remote_for_data = remote_peer.to_owned();
    let state_for_data = state.clone();
    connection.on_data_channel(Box::new(move |channel: Arc<RTCDataChannel>| {
        let state_for_data = state_for_data.clone();
        let remote_for_data = remote_for_data.clone();
        Box::pin(async move {
            attach_mesh_channel_handlers(&state_for_data, &remote_for_data, channel).await;
        })
    }));

    let mut peers = state.peers.lock().await;
    if let Some(existing) = peers
        .get(remote_peer)
        .and_then(|peer| peer.connection.clone())
    {
        return Ok(existing);
    }
    peers.insert(
        remote_peer.to_owned(),
        NativeMeshPeer {
            connection: Some(connection.clone()),
            channel: None,
            pending_remote_ice: Vec::new(),
        },
    );
    Ok(connection)
}

async fn attach_mesh_channel_handlers(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    channel: Arc<RTCDataChannel>,
) {
    let message_state = state.clone();
    let remote_for_message = remote_peer.to_owned();
    channel.on_message(Box::new(move |message: DataChannelMessage| {
        let message_state = message_state.clone();
        let remote_for_message = remote_for_message.clone();
        Box::pin(async move {
            let Ok(payload) = String::from_utf8(message.data.to_vec()) else {
                return;
            };
            let _ = handle_mesh_data_message(&message_state, &remote_for_message, &payload).await;
        })
    }));

    let open_state = state.clone();
    channel.on_open(Box::new(move || {
        let open_state = open_state.clone();
        Box::pin(async move {
            let _ = flush_mesh_pending_state(&open_state).await;
        })
    }));

    let close_state = state.clone();
    let remote_for_close = remote_peer.to_owned();
    channel.on_close(Box::new(move || {
        let close_state = close_state.clone();
        let remote_for_close = remote_for_close.clone();
        Box::pin(async move {
            remove_mesh_peer_state(&close_state, &remote_for_close).await;
        })
    }));

    let mut peers = state.peers.lock().await;
    if let Some(peer) = peers.get_mut(remote_peer) {
        peer.channel = Some(channel);
    }
}

async fn remove_mesh_peer_state(state: &Arc<NativeWebRtcMeshState>, remote_peer: &str) {
    let removed = {
        let mut peers = state.peers.lock().await;
        peers.remove(remote_peer)
    };
    if let Some(mut peer) = removed {
        if let Some(channel) = peer.channel.take() {
            let _ = channel.close().await;
        }
        if let Some(connection) = peer.connection.take() {
            let _ = connection.close().await;
        }
    }

    let mut to_requeue = Vec::new();
    {
        let mut inflight = state.inflight.lock().await;
        let mut empty = Vec::new();
        for (message_id, outbound) in inflight.iter_mut() {
            outbound.awaiting.remove(remote_peer);
            if outbound.awaiting.is_empty() {
                empty.push(message_id.clone());
            }
        }
        for message_id in empty {
            if let Some(outbound) = inflight.remove(&message_id) {
                to_requeue.push(outbound);
            }
        }
    }

    for outbound in to_requeue {
        if let Ok(frame) = decode_sync_payload(&state.db, &outbound.encoding, outbound.payload) {
            if let SyncFrame::Sync { ops, .. } = frame {
                let _ = state.db.requeue_pending_operations(ops);
            }
        }
    }
}

async fn handle_mesh_data_message(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    payload: &str,
) -> Result<()> {
    if let Ok(route) = serde_json::from_str::<RouteEnvelope>(payload) {
        return handle_mesh_route_message(state, remote_peer, route).await;
    }
    let frame: SyncFrame = serde_json::from_str(payload)?;
    handle_mesh_sync_frame(state, remote_peer, frame).await
}

async fn handle_mesh_route_message(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    route: RouteEnvelope,
) -> Result<()> {
    let decision = state.router.accept(route.clone());
    if !decision.deliver {
        return Ok(());
    }
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { .. } | RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let response = state.router.snapshot_response(
                    root,
                    state.db.snapshot(),
                    RouteTarget::Peer(remote_peer.to_owned()),
                );
                send_mesh_route_to_peer(state, remote_peer, &response).await?;
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state.db.load_snapshot(snapshot)?;
            }
            RoutePayload::Sync { encoding, payload } => {
                let frame = decode_sync_payload(&state.db, &encoding, payload)?;
                handle_mesh_sync_frame(state, remote_peer, frame).await?;
            }
            RoutePayload::PeerExchange { peers } => {
                store_peer_recommendations(state, peers).await;
            }
            RoutePayload::Batch { items } => {
                for item in items.into_iter().rev() {
                    pending.push(RoutePayload::from_batch_item(item));
                }
            }
            RoutePayload::PullRequest { .. } | RoutePayload::PullResponse { .. } => {}
        }
    }
    Ok(())
}

async fn handle_mesh_sync_frame(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    frame: SyncFrame,
) -> Result<()> {
    match frame {
        SyncFrame::Sync {
            from,
            message_id,
            ops,
        } => {
            let applied = state.db.apply_sync_envelope(SyncEnvelope { from, ops })?;
            let ack = SyncFrame::Ack {
                from: state.db.replica_id(),
                message_id,
                applied,
            };
            send_mesh_sync_frame(state, ack, RouteTarget::Peer(remote_peer.to_owned())).await
        }
        SyncFrame::Ack { message_id, .. } => {
            let mut inflight = state.inflight.lock().await;
            if let Some(outbound) = inflight.get_mut(&message_id) {
                outbound.awaiting.remove(remote_peer);
                if outbound.awaiting.is_empty() {
                    inflight.remove(&message_id);
                }
            }
            Ok(())
        }
    }
}

async fn flush_mesh_pending_state(state: &Arc<NativeWebRtcMeshState>) -> Result<usize> {
    let peer_ids = {
        let peers = state.peers.lock().await;
        peers
            .iter()
            .filter_map(|(peer_id, peer)| {
                peer.channel
                    .as_ref()
                    .is_some_and(mesh_channel_is_open)
                    .then_some(peer_id.clone())
            })
            .collect::<Vec<_>>()
    };
    if peer_ids.is_empty() {
        return Ok(0);
    }

    let mut envelope = state.db.drain_sync_envelope()?;
    if envelope.ops.is_empty() {
        return Ok(0);
    }

    let max_ops = state.db.limits().max_ops_per_message.max(1);
    if envelope.ops.len() > max_ops {
        let remainder = envelope.ops.split_off(max_ops);
        let _ = state.db.requeue_pending_operations(remainder);
    }

    let message_id = format!(
        "{}/mesh/{:x}",
        state.db.replica_id(),
        state.next_message_seq.fetch_add(1, Ordering::SeqCst) + 1
    );
    let frame = SyncFrame::Sync {
        from: envelope.from.clone(),
        message_id: message_id.clone(),
        ops: envelope.ops.clone(),
    };
    let (encoding, payload) = encode_sync_payload(&state.db, frame)?;
    let route = state
        .router
        .wrap_sync(encoding.clone(), payload.clone(), RouteTarget::Broadcast);

    let mut awaiting = BTreeMap::new();
    for peer_id in &peer_ids {
        if send_mesh_route_to_peer(state, peer_id, &route).await.is_ok() {
            awaiting.insert(peer_id.clone(), false);
        }
    }
    if awaiting.is_empty() {
        let _ = state.db.requeue_pending_operations(envelope.ops);
        return Ok(0);
    }

    state.inflight.lock().await.insert(
        message_id,
        MeshOutbound {
            encoding,
            payload,
            awaiting,
        },
    );
    Ok(peer_ids.len())
}

async fn retry_mesh_inflight_state(state: &Arc<NativeWebRtcMeshState>) -> Result<usize> {
    let outbound = state
        .inflight
        .lock()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for item in &outbound {
        let route = state.router.wrap_sync(
            item.encoding.clone(),
            item.payload.clone(),
            RouteTarget::Broadcast,
        );
        for peer_id in item.awaiting.keys() {
            let _ = send_mesh_route_to_peer(state, peer_id, &route).await;
        }
    }
    Ok(outbound.len())
}

async fn send_mesh_sync_frame(
    state: &Arc<NativeWebRtcMeshState>,
    frame: SyncFrame,
    target: RouteTarget,
) -> Result<()> {
    let (encoding, payload) = encode_sync_payload(&state.db, frame)?;
    let route = state.router.wrap_sync(encoding, payload, target.clone());
    match target {
        RouteTarget::Peer(peer_id) => send_mesh_route_to_peer(state, &peer_id, &route).await,
        RouteTarget::Broadcast | RouteTarget::Topic(_) => {
            let peer_ids = state.peers.lock().await.keys().cloned().collect::<Vec<_>>();
            for peer_id in peer_ids {
                let _ = send_mesh_route_to_peer(state, &peer_id, &route).await;
            }
            Ok(())
        }
    }
}

async fn send_mesh_route_to_peer(
    state: &Arc<NativeWebRtcMeshState>,
    peer_id: &str,
    route: &RouteEnvelope,
) -> Result<()> {
    let payload = serde_json::to_string(route)?;
    let max_bytes = state.db.limits().max_route_payload_bytes;
    if payload.len() > max_bytes {
        return Err(PrimadbError::Message(format!(
            "route payload exceeds {max_bytes} bytes"
        )));
    }
    let channel = state
        .peers
        .lock()
        .await
        .get(peer_id)
        .and_then(|peer| peer.channel.clone())
        .ok_or_else(|| PrimadbError::Message("mesh peer channel is unavailable".to_owned()))?;
    if !mesh_channel_is_open(&channel) {
        return Err(PrimadbError::Message(
            "mesh peer channel is not open".to_owned(),
        ));
    }
    channel.send_text(payload).await.map_err(to_error)?;
    Ok(())
}

fn mesh_channel_is_open(channel: &Arc<RTCDataChannel>) -> bool {
    channel.ready_state() == RTCDataChannelState::Open
}

async fn post_mesh_signal_state(
    state: &Arc<NativeWebRtcMeshState>,
    signal: &MeshSignal,
) -> Result<()> {
    if !state.relay_connected.load(Ordering::SeqCst) {
        return Err(PrimadbError::Message(
            "mesh relay websocket is not connected".to_owned(),
        ));
    }
    let route = state.router.wrap_signal(
        state.room.clone(),
        serde_json::to_value(signal)?,
        mesh_signal_target(signal),
    );
    send_route(state, &route)
}

async fn announce_mesh_join_state(state: &Arc<NativeWebRtcMeshState>) -> Result<()> {
    post_mesh_signal_state(
        state,
        &MeshSignal::Join {
            room: state.room.clone(),
            from: state.peer_id.clone(),
        },
    )
    .await
}

async fn post_mesh_leave_signal_state(state: &Arc<NativeWebRtcMeshState>) -> Result<()> {
    post_mesh_signal_state(
        state,
        &MeshSignal::Leave {
            room: state.room.clone(),
            from: state.peer_id.clone(),
        },
    )
    .await
}

fn mesh_signal_target(signal: &MeshSignal) -> RouteTarget {
    match signal {
        MeshSignal::Join { .. } | MeshSignal::Leave { .. } => RouteTarget::Broadcast,
        MeshSignal::Offer { to, .. }
        | MeshSignal::Answer { to, .. }
        | MeshSignal::Ice { to, .. } => RouteTarget::Peer(to.clone()),
    }
}

async fn send_mesh_presence_state(state: &Arc<NativeWebRtcMeshState>) -> Result<()> {
    let mut route = state.router.presence(
        state.db.replica_id(),
        "webrtc-relay",
        vec![
            "signal".to_owned(),
            "webrtc".to_owned(),
            "peer_exchange".to_owned(),
        ],
        vec![format!("mesh:{}", state.room)],
    );
    if let RoutePayload::Presence { peer } = &mut route.payload {
        peer.metadata
            .insert("relay_url".to_owned(), state.relay_url.clone());
        peer.metadata
            .insert("mesh_room".to_owned(), state.room.clone());
        peer.metadata
            .insert("signaling".to_owned(), "relay".to_owned());
    }
    send_route(state, &route)
}

fn send_route(state: &Arc<NativeWebRtcMeshState>, route: &RouteEnvelope) -> Result<()> {
    let payload = serde_json::to_string(route)?;
    let max_bytes = state.db.limits().max_route_payload_bytes;
    if payload.len() > max_bytes {
        return Err(PrimadbError::Message(format!(
            "route payload exceeds {max_bytes} bytes"
        )));
    }
    state
        .outbound
        .send(Message::Text(payload.into()))
        .map_err(|error| PrimadbError::Message(error.to_string()))
}

fn build_rtc_configuration(ice_servers: &[IceServerConfig]) -> RTCConfiguration {
    RTCConfiguration {
        ice_servers: ice_servers
            .iter()
            .map(|server| RTCIceServer {
                urls: server.urls.clone().into_vec(),
                username: server.username.clone().unwrap_or_default(),
                credential: server.credential.clone().unwrap_or_default(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn encode_sync_payload(_db: &Primadb, frame: SyncFrame) -> Result<(String, JsonValue)> {
    #[cfg(feature = "crypto")]
    {
        let frame = _db.secure_sync_frame(frame)?;
        return Ok(("secure_sync_frame".to_owned(), serde_json::to_value(frame)?));
    }

    #[cfg(not(feature = "crypto"))]
    {
        Ok(("sync_frame".to_owned(), serde_json::to_value(frame)?))
    }
}

fn decode_sync_payload(_db: &Primadb, encoding: &str, payload: JsonValue) -> Result<SyncFrame> {
    match encoding {
        "sync_frame" => Ok(serde_json::from_value(payload)?),
        #[cfg(feature = "crypto")]
        "secure_sync_frame" => _db.decode_secure_sync_frame(serde_json::from_value(payload)?),
        #[cfg(not(feature = "crypto"))]
        "secure_sync_frame" => Err(PrimadbError::Message(
            "received secure sync frame without crypto support".to_owned(),
        )),
        other => Err(PrimadbError::Message(format!(
            "unsupported sync encoding `{other}`"
        ))),
    }
}

async fn store_peer_recommendations(
    state: &Arc<NativeWebRtcMeshState>,
    recommendations: Vec<PeerRecommendation>,
) {
    let mut store = state.recommendations.lock().await;
    for recommendation in recommendations {
        store.insert(recommendation.peer.peer_id.clone(), recommendation);
    }
}

fn peer_recommendation_from_presence(peer: &crate::PeerPresence, relay_url: &str) -> PeerRecommendation {
    PeerRecommendation {
        peer: peer.clone(),
        relay_urls: vec![relay_url.to_owned()],
        score: 100,
        discovered_at_millis: crate::clock::now_millis(),
    }
}

fn to_error(error: impl ToString) -> PrimadbError {
    PrimadbError::Message(error.to_string())
}
