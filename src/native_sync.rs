use crate::{
    ChangeSubscription, HybridClock, LexEntry, MapEntry, Operation, PeerRecommendation, Primadb,
    PrimadbError, RemotePath, RemoteResult, Result, RouteBatchItem, RouteEnvelope, RoutePayload,
    RouteTarget, Router, RouterConfig, SyncEnvelope, SyncFrame,
};
use async_channel::{Sender, bounded};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone)]
struct OutboundSync {
    encoding: String,
    payload: JsonValue,
    target: RouteTarget,
}

#[derive(Debug)]
struct PendingPullRequest {
    sender: Sender<std::result::Result<RemoteResult, String>>,
    accumulator: PullAccumulator,
}

#[derive(Debug)]
enum PullAccumulator {
    Get {
        value: Option<JsonValue>,
    },
    Map {
        entries: Vec<MapEntry>,
    },
    Query {
        entries: Vec<MapEntry>,
    },
    Lex {
        entries: Vec<LexEntry>,
    },
    Snapshot {
        clock: Option<HybridClock>,
        nodes: BTreeMap<String, crate::NodeState>,
        pending_ops: Vec<Operation>,
    },
}

#[derive(Debug)]
struct NativeWebSocketSyncState {
    db: Primadb,
    router: Router,
    connected: AtomicBool,
    next_message_seq: AtomicU64,
    inflight: Mutex<BTreeMap<String, OutboundSync>>,
    pending_requests: Mutex<BTreeMap<String, PendingPullRequest>>,
    recommendations: Mutex<BTreeMap<String, PeerRecommendation>>,
    outbound: UnboundedSender<Message>,
}

pub struct NativeWebSocketSync {
    state: Arc<NativeWebSocketSyncState>,
    change_subscription: Option<ChangeSubscription>,
    writer_task: Option<JoinHandle<()>>,
    reader_task: Option<JoinHandle<()>>,
    change_task: Option<JoinHandle<()>>,
    retry_task: Option<JoinHandle<()>>,
}

impl NativeWebSocketSync {
    pub async fn connect(
        db: Primadb,
        url: impl AsRef<str>,
        retry_interval: Duration,
    ) -> Result<Self> {
        let url = url.as_ref().to_owned();
        let (socket, _) = connect_async(&url)
            .await
            .map_err(|error| PrimadbError::Message(error.to_string()))?;
        let (mut writer, mut reader) = socket.split();
        let (outbound, mut outbound_rx) = unbounded_channel::<Message>();

        let router = Router::new(RouterConfig {
            peer_id: format!("native:{}", db.replica_id()),
            default_channel: "primadb-sync".to_owned(),
            default_ttl: 6,
            max_seen_routes: db.limits().max_seen_routes,
        });

        let state = Arc::new(NativeWebSocketSyncState {
            db: db.clone(),
            router,
            connected: AtomicBool::new(true),
            next_message_seq: AtomicU64::new(0),
            inflight: Mutex::new(BTreeMap::new()),
            pending_requests: Mutex::new(BTreeMap::new()),
            recommendations: Mutex::new(BTreeMap::new()),
            outbound,
        });

        let writer_state = state.clone();
        let writer_task = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                if writer.send(message).await.is_err() {
                    writer_state.connected.store(false, Ordering::SeqCst);
                    requeue_inflight_state(&writer_state);
                    fail_pending_requests(
                        &writer_state,
                        "websocket writer closed while requests were in flight",
                    );
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
                        let _ = handle_incoming_text(&reader_state, &payload).await;
                    }
                    Ok(Message::Binary(_)) | Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Frame(_)) => {}
                }
            }
            reader_state.connected.store(false, Ordering::SeqCst);
            requeue_inflight_state(&reader_state);
            fail_pending_requests(
                &reader_state,
                "websocket reader closed while requests were in flight",
            );
        });

        let change_subscription = db.subscribe_changes();
        let change_receiver = change_subscription.receiver();
        let change_state = state.clone();
        let change_task = tokio::spawn(async move {
            while let Ok(event) = change_receiver.recv().await {
                if event.pending_ops > 0 {
                    let _ = flush_pending_state(&change_state).await;
                }
            }
        });

        let retry_state = state.clone();
        let retry_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retry_interval);
            loop {
                interval.tick().await;
                if !retry_state.connected.load(Ordering::SeqCst) {
                    continue;
                }
                let _ = retry_inflight_state(&retry_state).await;
                let _ = flush_pending_state(&retry_state).await;
            }
        });

        let mut presence = state.router.presence(
            db.replica_id(),
            "websocket",
            vec![
                "sync".to_owned(),
                "ack".to_owned(),
                "routing".to_owned(),
                "snapshot".to_owned(),
                "batch".to_owned(),
                "pull_get".to_owned(),
                "pull_query".to_owned(),
                "pull_lex".to_owned(),
                "peer_exchange".to_owned(),
            ],
            vec!["primadb-sync".to_owned()],
        );
        if let RoutePayload::Presence { peer } = &mut presence.payload {
            peer.metadata.insert("relay_url".to_owned(), url);
        }
        send_route(&state, &presence)?;

        Ok(Self {
            state,
            change_subscription: Some(change_subscription),
            writer_task: Some(writer_task),
            reader_task: Some(reader_task),
            change_task: Some(change_task),
            retry_task: Some(retry_task),
        })
    }

    pub fn is_connected(&self) -> bool {
        self.state.connected.load(Ordering::SeqCst)
    }

    pub fn pending_count(&self) -> usize {
        self.state.db.pending_operations().len()
    }

    pub fn inflight_count(&self) -> usize {
        self.state.inflight.lock().unwrap().len()
    }

    pub fn known_peer_count(&self) -> usize {
        self.state.router.known_peers().len()
    }

    pub fn recommended_peers(&self) -> Vec<PeerRecommendation> {
        self.state
            .recommendations
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub async fn remote_get(&self, peer_id: impl Into<String>, path: RemotePath) -> Result<Option<JsonValue>> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Get { path },
        )
        .await?
        {
            RemoteResult::Get { value } => Ok(value),
            other => Err(PrimadbError::Message(format!(
                "expected get result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_query(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
        spec: crate::QuerySpec,
    ) -> Result<Vec<MapEntry>> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Query { path, spec },
        )
        .await?
        {
            RemoteResult::Query { entries } => Ok(entries),
            other => Err(PrimadbError::Message(format!(
                "expected query result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_lex(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
        spec: crate::LexSpec,
    ) -> Result<Vec<LexEntry>> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Lex { path, spec },
        )
        .await?
        {
            RemoteResult::Lex { entries } => Ok(entries),
            other => Err(PrimadbError::Message(format!(
                "expected lex result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_snapshot(
        &self,
        peer_id: impl Into<String>,
        root: Option<String>,
    ) -> Result<crate::DatabaseSnapshot> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Snapshot { root },
        )
        .await?
        {
            RemoteResult::Snapshot { snapshot } => Ok(snapshot),
            other => Err(PrimadbError::Message(format!(
                "expected snapshot result, received {other:?}"
            ))),
        }
    }

    pub async fn flush_pending(&self) -> Result<usize> {
        flush_pending_state(&self.state).await
    }

    pub async fn retry_inflight(&self) -> Result<usize> {
        retry_inflight_state(&self.state).await
    }

    pub fn close(&mut self) {
        self.teardown();
    }

    fn teardown(&mut self) {
        self.state.connected.store(false, Ordering::SeqCst);
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
        requeue_inflight_state(&self.state);
        fail_pending_requests(&self.state, "connection closed");
    }
}

impl Drop for NativeWebSocketSync {
    fn drop(&mut self) {
        self.teardown();
    }
}

async fn request_remote_result(
    state: &Arc<NativeWebSocketSyncState>,
    target_peer_id: String,
    request_kind: crate::PullRequestKind,
) -> Result<RemoteResult> {
    if !state.connected.load(Ordering::SeqCst) {
        return Err(PrimadbError::Message(
            "native websocket is not connected".to_owned(),
        ));
    }

    let request_id = format!(
        "{}/pull/{:x}",
        state.db.replica_id(),
        state.next_message_seq.fetch_add(1, Ordering::SeqCst) + 1
    );
    let (sender, receiver) = bounded(1);
    state.pending_requests.lock().unwrap().insert(
        request_id.clone(),
        PendingPullRequest {
            sender,
            accumulator: PullAccumulator::new(&request_kind),
        },
    );

    let request = crate::PullRequest {
        request_id: request_id.clone(),
        request: request_kind,
    };
    let route = state
        .router
        .wrap_pull_request(request, RouteTarget::Peer(target_peer_id));
    if let Err(error) = send_route(state, &route) {
        state.pending_requests.lock().unwrap().remove(&request_id);
        return Err(error);
    }

    receiver
        .recv()
        .await
        .map_err(|error| PrimadbError::Message(error.to_string()))?
        .map_err(PrimadbError::Message)
}

async fn handle_incoming_text(
    state: &Arc<NativeWebSocketSyncState>,
    payload: &str,
) -> Result<()> {
    if let Ok(route) = serde_json::from_str::<RouteEnvelope>(payload) {
        return handle_route_envelope(state, route).await;
    }

    let frame: SyncFrame =
        serde_json::from_str(payload).map_err(|error| PrimadbError::Message(error.to_string()))?;
    handle_sync_frame(state, frame, None).await
}

async fn handle_route_envelope(
    state: &Arc<NativeWebSocketSyncState>,
    route: RouteEnvelope,
) -> Result<()> {
    let decision = state.router.accept(route.clone());
    if !decision.deliver {
        return Ok(());
    }
    handle_route_payload(state, route.from, route.route_id, route.payload).await
}

async fn handle_route_payload(
    state: &Arc<NativeWebSocketSyncState>,
    from: String,
    route_id: String,
    payload: RoutePayload,
) -> Result<()> {
    let mut pending = vec![payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                store_peer_recommendations(state, vec![peer_recommendation_from_presence(&peer)]);
            }
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let response = state
                    .router
                    .snapshot_response(root, state.db.snapshot(), RouteTarget::Peer(from.clone()));
                send_route(state, &response)?;
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state.db.load_snapshot(snapshot)?;
            }
            RoutePayload::Sync { encoding, payload } => {
                let frame = decode_sync_payload(&state.db, &encoding, payload)?;
                handle_sync_frame(state, frame, Some(from.clone())).await?;
            }
            RoutePayload::PullRequest { request } => {
                let result = state.db.execute_pull_request(&request)?;
                let items = state
                    .db
                    .chunk_remote_result(&request.request_id, result)
                    .into_iter()
                    .map(|response| RouteBatchItem::PullResponse { response })
                    .collect::<Vec<_>>();
                for route in pack_batch_routes(
                    &state.router,
                    RouteTarget::Peer(from.clone()),
                    Some(route_id.clone()),
                    items,
                    state.db.limits().max_batch_items_per_route.max(1),
                ) {
                    send_route(state, &route)?;
                }
            }
            RoutePayload::PullResponse { response } => {
                accept_pull_response(state, response)?;
            }
            RoutePayload::PeerExchange { peers } => {
                store_peer_recommendations(state, peers);
            }
            RoutePayload::Batch { items } => {
                for item in items.into_iter().rev() {
                    pending.push(RoutePayload::from_batch_item(item));
                }
            }
        }
    }
    Ok(())
}

async fn handle_sync_frame(
    state: &Arc<NativeWebSocketSyncState>,
    frame: SyncFrame,
    reply_peer: Option<String>,
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
            send_sync_frame(
                state,
                ack,
                reply_peer
                    .map(RouteTarget::Peer)
                    .unwrap_or(RouteTarget::Broadcast),
            )?;
        }
        SyncFrame::Ack {
            from: _,
            message_id,
            applied: _,
        } => {
            state.inflight.lock().unwrap().remove(&message_id);
            let _ = flush_pending_state(state).await;
        }
    }
    Ok(())
}

async fn flush_pending_state(state: &Arc<NativeWebSocketSyncState>) -> Result<usize> {
    if !state.connected.load(Ordering::SeqCst) {
        return Ok(0);
    }

    let mut envelope = state.db.drain_sync_envelope()?;
    if envelope.ops.is_empty() {
        return Ok(0);
    }

    let max_ops = state.db.limits().max_ops_per_message.max(1);
    if envelope.ops.len() > max_ops {
        let remainder = envelope.ops.split_off(max_ops);
        let _ = state.db.requeue_pending_operations(remainder)?;
    }

    let message_id = format!(
        "{}/native/{:x}",
        state.db.replica_id(),
        state.next_message_seq.fetch_add(1, Ordering::SeqCst) + 1
    );

    let frame = SyncFrame::Sync {
        from: envelope.from.clone(),
        message_id: message_id.clone(),
        ops: envelope.ops.clone(),
    };
    let (encoding, payload) = encode_sync_payload(&state.db, frame)?;
    let outbound = OutboundSync {
        encoding: encoding.clone(),
        payload: payload.clone(),
        target: RouteTarget::Broadcast,
    };
    let sent_ops = envelope.ops.len();
    let route = state.router.wrap_sync(encoding, payload, RouteTarget::Broadcast);

    if let Err(error) = send_route(state, &route) {
        let _ = state.db.requeue_pending_operations(envelope.ops);
        return Err(error);
    }

    state.inflight.lock().unwrap().insert(message_id, outbound);
    Ok(sent_ops)
}

async fn retry_inflight_state(state: &Arc<NativeWebSocketSyncState>) -> Result<usize> {
    if !state.connected.load(Ordering::SeqCst) {
        return Ok(0);
    }

    let outbound = state
        .inflight
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();

    for item in &outbound {
        let route = state
            .router
            .wrap_sync(item.encoding.clone(), item.payload.clone(), item.target.clone());
        send_route(state, &route)?;
    }

    Ok(outbound.len())
}

fn send_sync_frame(
    state: &Arc<NativeWebSocketSyncState>,
    frame: SyncFrame,
    target: RouteTarget,
) -> Result<()> {
    let (encoding, payload) = encode_sync_payload(&state.db, frame)?;
    let route = state.router.wrap_sync(encoding, payload, target);
    send_route(state, &route)
}

fn send_route(state: &Arc<NativeWebSocketSyncState>, route: &RouteEnvelope) -> Result<()> {
    if !state.connected.load(Ordering::SeqCst) {
        return Err(PrimadbError::Message(
            "native websocket is not connected".to_owned(),
        ));
    }
    let payload = serde_json::to_string(route)?;
    if payload.len() > state.db.limits().max_route_payload_bytes {
        return Err(PrimadbError::Message(format!(
            "route payload exceeds {} bytes",
            state.db.limits().max_route_payload_bytes
        )));
    }
    state
        .outbound
        .send(Message::Text(payload.into()))
        .map_err(|error| PrimadbError::Message(error.to_string()))
}

fn requeue_inflight_state(state: &Arc<NativeWebSocketSyncState>) {
    let (db, inflight) = {
        let mut inflight = state.inflight.lock().unwrap();
        (state.db.clone(), std::mem::take(&mut *inflight))
    };
    for (_, outbound) in inflight {
        if let Ok(frame) = decode_sync_payload(&db, &outbound.encoding, outbound.payload) {
            if let SyncFrame::Sync { ops, .. } = frame {
                let _ = db.requeue_pending_operations(ops);
            }
        }
    }
}

fn fail_pending_requests(state: &Arc<NativeWebSocketSyncState>, message: &str) {
    let pending = {
        let mut pending = state.pending_requests.lock().unwrap();
        std::mem::take(&mut *pending)
    };
    for request in pending.into_values() {
        let _ = request.sender.try_send(Err(message.to_owned()));
    }
}

fn accept_pull_response(state: &Arc<NativeWebSocketSyncState>, response: crate::PullResponse) -> Result<()> {
    let mut pending = state.pending_requests.lock().unwrap();
    let Some(request) = pending.get_mut(&response.request_id) else {
        return Ok(());
    };

    match &response.result {
        crate::PullResponseBody::Get { value } => {
            request.accumulator = PullAccumulator::Get {
                value: value.clone(),
            };
        }
        crate::PullResponseBody::Map { entries } => {
            if let PullAccumulator::Map { entries: current } = &mut request.accumulator {
                current.extend(entries.clone());
            }
        }
        crate::PullResponseBody::Query { entries } => {
            if let PullAccumulator::Query { entries: current } = &mut request.accumulator {
                current.extend(entries.clone());
            }
        }
        crate::PullResponseBody::Lex { entries } => {
            if let PullAccumulator::Lex { entries: current } = &mut request.accumulator {
                current.extend(entries.clone());
            }
        }
        crate::PullResponseBody::Snapshot {
            clock,
            nodes,
            pending_ops,
        } => {
            if let PullAccumulator::Snapshot {
                clock: current_clock,
                nodes: current_nodes,
                pending_ops: current_ops,
            } = &mut request.accumulator
            {
                if current_clock.is_none() {
                    *current_clock = clock.clone();
                }
                current_nodes.extend(nodes.clone());
                current_ops.extend(pending_ops.clone());
            }
        }
        crate::PullResponseBody::Error { message } => {
            let request = pending.remove(&response.request_id).unwrap();
            let _ = request.sender.try_send(Err(message.clone()));
            return Ok(());
        }
    }

    if response.is_final() {
        let request = pending.remove(&response.request_id).unwrap();
        let result = request.accumulator.into_result()?;
        let _ = request.sender.try_send(Ok(result));
    }
    Ok(())
}

fn store_peer_recommendations(
    state: &Arc<NativeWebSocketSyncState>,
    peers: Vec<PeerRecommendation>,
) {
    let max = state.db.limits().max_peer_recommendations.max(1);
    let mut recommendations = state.recommendations.lock().unwrap();
    for peer in peers {
        recommendations.insert(peer.peer.peer_id.clone(), peer);
    }
    while recommendations.len() > max {
        let Some(oldest) = recommendations.keys().next().cloned() else {
            break;
        };
        recommendations.remove(&oldest);
    }
}

fn peer_recommendation_from_presence(peer: &crate::PeerPresence) -> PeerRecommendation {
    let relay_urls = peer
        .metadata
        .get("relay_url")
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    PeerRecommendation {
        peer: peer.clone(),
        relay_urls,
        score: 100 + peer.capabilities.len().min(8) as u16 * 5 + peer.topics.len().min(8) as u16 * 5,
        discovered_at_millis: crate::clock::now_millis(),
    }
}

fn pack_batch_routes(
    router: &Router,
    target: RouteTarget,
    reply_to: Option<String>,
    items: Vec<RouteBatchItem>,
    batch_size: usize,
) -> Vec<RouteEnvelope> {
    if items.is_empty() {
        return Vec::new();
    }
    items.chunks(batch_size.max(1))
        .map(|chunk| {
            if chunk.len() == 1 {
                router.wrap_batch_item(chunk[0].clone(), target.clone(), reply_to.clone())
            } else {
                router.wrap_batch(chunk.to_vec(), target.clone(), reply_to.clone())
            }
        })
        .collect()
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

impl PullAccumulator {
    fn new(request: &crate::PullRequestKind) -> Self {
        match request {
            crate::PullRequestKind::Get { .. } => Self::Get { value: None },
            crate::PullRequestKind::Map { .. } => Self::Map { entries: Vec::new() },
            crate::PullRequestKind::Query { .. } => Self::Query { entries: Vec::new() },
            crate::PullRequestKind::Lex { .. } => Self::Lex { entries: Vec::new() },
            crate::PullRequestKind::Snapshot { .. } => Self::Snapshot {
                clock: None,
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
            },
        }
    }

    fn into_result(self) -> Result<RemoteResult> {
        match self {
            Self::Get { value } => Ok(RemoteResult::Get { value }),
            Self::Map { entries } => Ok(RemoteResult::Map { entries }),
            Self::Query { entries } => Ok(RemoteResult::Query { entries }),
            Self::Lex { entries } => Ok(RemoteResult::Lex { entries }),
            Self::Snapshot {
                clock,
                nodes,
                pending_ops,
            } => Ok(RemoteResult::Snapshot {
                snapshot: crate::DatabaseSnapshot {
                    clock: clock.ok_or_else(|| {
                        PrimadbError::Message(
                            "snapshot response completed without a clock".to_owned(),
                        )
                    })?,
                    nodes,
                    pending_ops,
                },
            }),
        }
    }
}
