#[cfg(feature = "crypto")]
use crate::SecureSyncFrame;
use crate::{
    ChangeSubscription, HookTransport, IceServerConfig, MeshConfig, MeshSignal, MeshSignalingMode,
    NodeFetchScheduler, PeerRecommendation, Primadb, PrimadbError, RecordEntry, RecordScanResult,
    RemoteInterestPolicy, RemoteInterestTarget, RemoteWatchMessage, RemoteWatchSubscription,
    Result, RouteBatchItem, RouteEnvelope, RoutePayload, RouteTarget, Router, RouterConfig,
    SyncEnvelope, SyncFrame, VerifiedIdentity, WatchEvent, WatchRequest, WatchRequestKind,
    error_watch_event,
};
use async_channel::{Sender, unbounded};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
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

#[derive(Debug)]
struct OutgoingWatch {
    sender: Sender<std::result::Result<RemoteWatchMessage, String>>,
    target_peer_id: String,
    request_kind: crate::PullRequestKind,
    pending_sequence: Option<PendingWatchSequence>,
    last_delivered_sequence: Option<u64>,
}

#[derive(Debug)]
struct PendingWatchSequence {
    sequence: u64,
    initial: bool,
    accumulator: PullAccumulator,
}

#[derive(Debug, Clone)]
struct IncomingWatch {
    target_peer_id: String,
    request_kind: crate::PullRequestKind,
    interest_path: Option<String>,
    next_sequence: u64,
    last_hash: Option<String>,
}

#[derive(Debug)]
enum PullAccumulator {
    Get {
        value: Option<JsonValue>,
    },
    Map {
        entries: Vec<crate::MapEntry>,
    },
    Query {
        entries: Vec<crate::MapEntry>,
    },
    Lex {
        entries: Vec<crate::LexEntry>,
    },
    Records {
        entries: Vec<RecordEntry>,
        next_cursor: Option<String>,
    },
    VectorSearch {
        result: Option<crate::VectorSearchResult>,
    },
    Node {
        node: Option<crate::NodeState>,
    },
    Snapshot {
        clock: Option<crate::HybridClock>,
        nodes: BTreeMap<String, crate::NodeState>,
        pending_ops: Vec<crate::Operation>,
        scope_policies: BTreeMap<String, crate::ScopePolicy>,
    },
    Transaction {
        report: Option<crate::TransactionReport>,
    },
}

#[derive(Default)]
struct NativeMeshPeer {
    connection: Option<Arc<RTCPeerConnection>>,
    channel: Option<Arc<RTCDataChannel>>,
    pending_remote_ice: Vec<RTCIceCandidateInit>,
    created_at_millis: u64,
}

const STALE_MESH_PEER_MILLIS: u64 = 5_000;

type MeshRelaySocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type MeshRelayWriter = SplitSink<MeshRelaySocket, Message>;

#[derive(Default)]
struct MeshRelayState {
    writer: Option<MeshRelayWriter>,
    reader_task: Option<JoinHandle<()>>,
}

struct NativeWebRtcMeshState {
    db: Primadb,
    api: Arc<API>,
    runtime: tokio::runtime::Handle,
    router: Router,
    room: String,
    peer_id: String,
    session_id: String,
    session_auth: crate::SessionAuthConfig,
    relay_url: String,
    rtc_configuration: RTCConfiguration,
    closed: AtomicBool,
    relay_connected: AtomicBool,
    relay_connecting: AtomicBool,
    next_message_seq: AtomicU64,
    relay: Mutex<MeshRelayState>,
    peers: Mutex<BTreeMap<String, NativeMeshPeer>>,
    inflight: Mutex<BTreeMap<String, MeshOutbound>>,
    outgoing_watches: Mutex<BTreeMap<String, OutgoingWatch>>,
    incoming_watches: Mutex<BTreeMap<String, IncomingWatch>>,
    recommendations: Mutex<BTreeMap<String, PeerRecommendation>>,
    pending_auth_challenges: Mutex<BTreeMap<String, crate::AuthChallenge>>,
    pending_auth_peers: Mutex<BTreeMap<String, crate::PeerPresence>>,
    verified_identities: Mutex<BTreeMap<String, VerifiedIdentity>>,
}

pub struct NativeWebRtcMesh {
    state: Arc<NativeWebRtcMeshState>,
    change_subscription: Option<ChangeSubscription>,
    change_task: Option<JoinHandle<()>>,
    retry_task: Option<JoinHandle<()>>,
    node_fetch_registration: Option<u64>,
}

struct NativeMeshNodeFetchScheduler {
    state: Weak<NativeWebRtcMeshState>,
}

impl NativeWebRtcMesh {
    pub async fn connect_with_config(db: Primadb, config: MeshConfig) -> Result<Self> {
        if !matches!(config.signaling, MeshSignalingMode::Relay) {
            return Err(PrimadbError::Message(
                "native mesh currently requires relay signaling".to_owned(),
            ));
        }
        let relay_url = config
            .relay_url
            .clone()
            .ok_or_else(|| PrimadbError::Message("native mesh requires a relay_url".to_owned()))?;

        let rtc_configuration = build_rtc_configuration(&config.effective_ice_servers());
        let api = Arc::new(APIBuilder::new().build());

        let peer_id = format!("mesh:{}:{}", db.replica_id(), crate::clock::now_millis());
        let state = Arc::new(NativeWebRtcMeshState {
            db: db.clone(),
            api,
            runtime: tokio::runtime::Handle::current(),
            router: Router::new(RouterConfig {
                peer_id: peer_id.clone(),
                default_channel: format!("mesh:{}", config.room),
                default_ttl: 6,
                max_seen_routes: db.limits().max_seen_routes,
            }),
            room: config.room,
            peer_id,
            session_id: crate::session_auth::random_session_id(&format!(
                "mesh:native:{}",
                db.replica_id()
            )),
            session_auth: config.session_auth,
            relay_url: relay_url.clone(),
            rtc_configuration,
            closed: AtomicBool::new(false),
            relay_connected: AtomicBool::new(false),
            relay_connecting: AtomicBool::new(false),
            next_message_seq: AtomicU64::new(0),
            relay: Mutex::new(MeshRelayState::default()),
            peers: Mutex::new(BTreeMap::new()),
            inflight: Mutex::new(BTreeMap::new()),
            outgoing_watches: Mutex::new(BTreeMap::new()),
            incoming_watches: Mutex::new(BTreeMap::new()),
            recommendations: Mutex::new(BTreeMap::new()),
            pending_auth_challenges: Mutex::new(BTreeMap::new()),
            pending_auth_peers: Mutex::new(BTreeMap::new()),
            verified_identities: Mutex::new(BTreeMap::new()),
        });

        let change_subscription = db.subscribe_changes();
        let change_receiver = change_subscription.receiver();
        let change_state = state.clone();
        let change_task = tokio::spawn(async move {
            while let Ok(mut event) = change_receiver.recv().await {
                while let Ok(next) = change_receiver.try_recv() {
                    event.merge(next);
                }
                if event.pending_ops > 0 {
                    let _ = flush_mesh_pending_state(&change_state).await;
                }
                if event.data_changed {
                    let _ = emit_incoming_mesh_watch_updates(&change_state, &event).await;
                }
            }
        });

        let retry_interval = Duration::from_millis(config.retry_interval_ms.max(1));
        let retry_state = state.clone();
        let retry_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(retry_interval).await;
                if retry_state.closed.load(Ordering::SeqCst) {
                    break;
                }
                if !retry_state.relay_connected.load(Ordering::SeqCst) {
                    let _ = connect_mesh_relay_state(&retry_state).await;
                    continue;
                }
                let _ = announce_mesh_join_state(&retry_state).await;
                let _ = retry_mesh_inflight_state(&retry_state).await;
                let _ = flush_mesh_pending_state(&retry_state).await;
            }
        });

        let _ = connect_mesh_relay_state(&state).await;

        let node_fetch_registration =
            db.register_node_fetch_scheduler(Arc::new(NativeMeshNodeFetchScheduler {
                state: Arc::downgrade(&state),
            }));

        Ok(Self {
            state,
            change_subscription: Some(change_subscription),
            change_task: Some(change_task),
            retry_task: Some(retry_task),
            node_fetch_registration: Some(node_fetch_registration),
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

    pub async fn watch_get(
        &self,
        peer_id: impl Into<String>,
        path: crate::RemotePath,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Get { path },
        )
        .await
    }

    pub async fn watch_map(
        &self,
        peer_id: impl Into<String>,
        path: crate::RemotePath,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Map { path },
        )
        .await
    }

    pub async fn watch_query(
        &self,
        peer_id: impl Into<String>,
        path: crate::RemotePath,
        spec: crate::QuerySpec,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Query { path, spec },
        )
        .await
    }

    pub async fn watch_lex(
        &self,
        peer_id: impl Into<String>,
        path: crate::RemotePath,
        spec: crate::LexSpec,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Lex { path, spec },
        )
        .await
    }

    pub async fn watch_records(
        &self,
        peer_id: impl Into<String>,
        scan: crate::RecordScan,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Records { scan },
        )
        .await
    }

    pub async fn watch_vector_search(
        &self,
        peer_id: impl Into<String>,
        collection: impl Into<String>,
        query: Vec<f32>,
        spec: crate::VectorSearchSpec,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::VectorSearch {
                collection: collection.into(),
                query,
                spec,
            },
        )
        .await
    }

    pub async fn watch_node(
        &self,
        peer_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Node { id: id.into() },
        )
        .await
    }

    pub async fn watch_snapshot(
        &self,
        peer_id: impl Into<String>,
        root: Option<String>,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Snapshot { root },
        )
        .await
    }

    pub async fn watch_get_with_policy(
        &self,
        path: crate::RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(&self.state, policy, crate::PullRequestKind::Get { path })
            .await
    }

    pub async fn watch_map_with_policy(
        &self,
        path: crate::RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(&self.state, policy, crate::PullRequestKind::Map { path })
            .await
    }

    pub async fn watch_query_with_policy(
        &self,
        path: crate::RemotePath,
        spec: crate::QuerySpec,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Query { path, spec },
        )
        .await
    }

    pub async fn watch_lex_with_policy(
        &self,
        path: crate::RemotePath,
        spec: crate::LexSpec,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Lex { path, spec },
        )
        .await
    }

    pub async fn watch_records_with_policy(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Records { scan },
        )
        .await
    }

    pub async fn watch_vector_search_with_policy(
        &self,
        collection: impl Into<String>,
        query: Vec<f32>,
        spec: crate::VectorSearchSpec,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::VectorSearch {
                collection: collection.into(),
                query,
                spec,
            },
        )
        .await
    }

    pub async fn watch_node_with_policy(
        &self,
        id: impl Into<String>,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Node { id: id.into() },
        )
        .await
    }

    pub async fn watch_snapshot_with_policy(
        &self,
        root: Option<String>,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_mesh_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Snapshot { root },
        )
        .await
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
        if let Some(id) = self.node_fetch_registration.take() {
            self.state.db.unregister_node_fetch_scheduler(id);
        }
        self.state.closed.store(true, Ordering::SeqCst);
        let _ = post_mesh_leave_signal_state(&self.state).await;
        disconnect_mesh_relay_state(&self.state, true).await;
        self.change_subscription.take();
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
        fail_outgoing_mesh_watches(&self.state, "mesh closed").await;
        clear_incoming_mesh_watches(&self.state).await;
    }
}

impl Drop for NativeWebRtcMesh {
    fn drop(&mut self) {
        if let Some(id) = self.node_fetch_registration.take() {
            self.state.db.unregister_node_fetch_scheduler(id);
        }
        self.state.closed.store(true, Ordering::SeqCst);
        self.state.relay_connected.store(false, Ordering::SeqCst);
        self.state.runtime.spawn({
            let state = self.state.clone();
            async move {
                disconnect_mesh_relay_state(&state, true).await;
            }
        });
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
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                maybe_send_mesh_auth_challenge_via_relay(state, &peer).await?;
                if state.session_auth.require_authenticated_peers
                    && verified_mesh_identity_for_peer(state, &peer.peer_id)
                        .await
                        .is_none()
                {
                    continue;
                }
                let recommendation = peer_recommendation_from_presence(&peer, &state.relay_url);
                let verified_identity =
                    verified_mesh_identity_for_peer(state, &recommendation.peer.peer_id).await;
                let (_, peer_to_join) =
                    accept_mesh_recommendation(state, recommendation, verified_identity.as_ref())
                        .await?;
                if let Some(peer_id) = peer_to_join {
                    handle_mesh_signal(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer_id,
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
                for recommendation in peers {
                    maybe_send_mesh_auth_challenge_via_relay(state, &recommendation.peer).await?;
                    if state.session_auth.require_authenticated_peers
                        && verified_mesh_identity_for_peer(state, &recommendation.peer.peer_id)
                            .await
                            .is_none()
                    {
                        continue;
                    }
                    let verified_identity =
                        verified_mesh_identity_for_peer(state, &recommendation.peer.peer_id).await;
                    let (_, peer_to_join) = accept_mesh_recommendation(
                        state,
                        recommendation,
                        verified_identity.as_ref(),
                    )
                    .await?;
                    if let Some(peer_id) = peer_to_join {
                        handle_mesh_signal(
                            state,
                            MeshSignal::Join {
                                room: room.clone(),
                                from: peer_id,
                            },
                        )
                        .await?;
                    }
                }
            }
            RoutePayload::AuthChallenge { challenge } => {
                handle_mesh_auth_challenge_via_relay(state, challenge).await?;
            }
            RoutePayload::AuthResponse { response } => {
                if let Some(peer_id) = handle_mesh_auth_response(state, response).await? {
                    handle_mesh_signal(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer_id,
                        },
                    )
                    .await?;
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

async fn verified_mesh_identity_for_peer(
    state: &Arc<NativeWebRtcMeshState>,
    peer_id: &str,
) -> Option<VerifiedIdentity> {
    state.verified_identities.lock().await.get(peer_id).cloned()
}

async fn remove_mesh_peer_identity_state(state: &Arc<NativeWebRtcMeshState>, peer_id: &str) {
    state.router.forget_peer(peer_id);
    state.recommendations.lock().await.remove(peer_id);
    state.verified_identities.lock().await.remove(peer_id);
}

async fn accept_mesh_recommendation(
    state: &Arc<NativeWebRtcMeshState>,
    recommendation: PeerRecommendation,
    verified_identity: Option<&VerifiedIdentity>,
) -> Result<(bool, Option<String>)> {
    let relay_url = recommendation.relay_urls.first().cloned();
    let connect_allowed = state
        .db
        .allow_peer_connection(&crate::ConnectHookContext {
            peer: recommendation.peer.clone(),
            transport: HookTransport::Mesh,
            relay_url,
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if connect_allowed.is_err() {
        remove_mesh_peer_identity_state(state, &recommendation.peer.peer_id).await;
        return Ok((false, None));
    }

    let peer = recommendation.peer.clone();
    store_peer_recommendations(state, vec![recommendation]).await;

    let in_room = peer
        .topics
        .iter()
        .any(|topic| topic == &format!("mesh:{}", state.room))
        || peer
            .metadata
            .get("mesh_room")
            .is_some_and(|candidate| candidate == &state.room);
    if !in_room {
        return Ok((true, None));
    }

    let room_allowed = state
        .db
        .allow_room_join(&crate::RoomHookContext {
            peer_id: peer.peer_id.clone(),
            room: state.room.clone(),
            transport: HookTransport::Mesh,
            peer: Some(peer.clone()),
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if room_allowed.is_err() {
        return Ok((true, None));
    }
    Ok((true, Some(peer.peer_id)))
}

async fn maybe_send_mesh_auth_challenge_via_relay(
    state: &Arc<NativeWebRtcMeshState>,
    peer: &crate::PeerPresence,
) -> Result<()> {
    if peer.peer_id == state.router.peer_id()
        || verified_mesh_identity_for_peer(state, &peer.peer_id)
            .await
            .is_some()
    {
        return Ok(());
    }
    let Some(identity) = peer.identity.as_ref() else {
        if !state.session_auth.allow_unauthenticated_presence {
            remove_mesh_peer_identity_state(state, &peer.peer_id).await;
        }
        return Ok(());
    };

    #[cfg(feature = "crypto")]
    {
        let challenge = crate::session_auth::create_auth_challenge(
            state.router.peer_id(),
            &state.db.replica_id(),
            &state.session_id,
            &peer.peer_id,
            &peer.replica_id,
            identity,
            "mesh",
            &state.session_auth,
        );
        let route = state
            .router
            .auth_challenge(challenge.clone(), RouteTarget::Peer(peer.peer_id.clone()));
        send_route(state, &route).await?;
        state
            .pending_auth_challenges
            .lock()
            .await
            .insert(challenge.challenge_id.clone(), challenge.clone());
        state
            .pending_auth_peers
            .lock()
            .await
            .insert(challenge.challenge_id.clone(), peer.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = identity;
    }

    Ok(())
}

async fn maybe_send_mesh_auth_challenge_to_peer(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    peer: &crate::PeerPresence,
) -> Result<()> {
    if peer.peer_id == state.router.peer_id()
        || verified_mesh_identity_for_peer(state, &peer.peer_id)
            .await
            .is_some()
    {
        return Ok(());
    }
    let Some(identity) = peer.identity.as_ref() else {
        if !state.session_auth.allow_unauthenticated_presence {
            remove_mesh_peer_identity_state(state, &peer.peer_id).await;
        }
        return Ok(());
    };

    #[cfg(feature = "crypto")]
    {
        let challenge = crate::session_auth::create_auth_challenge(
            state.router.peer_id(),
            &state.db.replica_id(),
            &state.session_id,
            &peer.peer_id,
            &peer.replica_id,
            identity,
            "mesh",
            &state.session_auth,
        );
        let route = state
            .router
            .auth_challenge(challenge.clone(), RouteTarget::Peer(peer.peer_id.clone()));
        send_mesh_route_to_peer(state, remote_peer, &route).await?;
        state
            .pending_auth_challenges
            .lock()
            .await
            .insert(challenge.challenge_id.clone(), challenge.clone());
        state
            .pending_auth_peers
            .lock()
            .await
            .insert(challenge.challenge_id.clone(), peer.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = remote_peer;
        let _ = identity;
    }

    Ok(())
}

async fn handle_mesh_auth_challenge_via_relay(
    state: &Arc<NativeWebRtcMeshState>,
    challenge: crate::AuthChallenge,
) -> Result<()> {
    if challenge.target_peer_id != state.router.peer_id() {
        return Ok(());
    }
    let Some(response) = state.db.sign_session_auth_response(
        &challenge,
        state.router.peer_id(),
        &state.session_id,
        &state.session_auth,
    )?
    else {
        return Ok(());
    };
    let route = state
        .router
        .auth_response(response, RouteTarget::Peer(challenge.issuer_peer_id));
    send_route(state, &route).await
}

async fn handle_mesh_auth_challenge_to_peer(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    challenge: crate::AuthChallenge,
) -> Result<()> {
    if challenge.target_peer_id != state.router.peer_id() {
        return Ok(());
    }
    let Some(response) = state.db.sign_session_auth_response(
        &challenge,
        state.router.peer_id(),
        &state.session_id,
        &state.session_auth,
    )?
    else {
        return Ok(());
    };
    let route = state
        .router
        .auth_response(response, RouteTarget::Peer(challenge.issuer_peer_id));
    send_mesh_route_to_peer(state, remote_peer, &route).await
}

async fn handle_mesh_auth_response(
    state: &Arc<NativeWebRtcMeshState>,
    response: crate::AuthResponse,
) -> Result<Option<String>> {
    let Some(challenge) = state
        .pending_auth_challenges
        .lock()
        .await
        .remove(&response.challenge_id)
    else {
        return Ok(None);
    };
    let peer = state
        .pending_auth_peers
        .lock()
        .await
        .remove(&response.challenge_id)
        .unwrap_or_else(|| crate::PeerPresence {
            peer_id: response.responder_peer_id.clone(),
            replica_id: response.responder_replica_id.clone(),
            transport: challenge.transport.clone(),
            identity: Some(response.responder_identity.clone()),
            capabilities: Vec::new(),
            topics: Vec::new(),
            metadata: BTreeMap::new(),
        });
    let verified =
        crate::session_auth::verify_auth_response(&challenge, &response, &state.session_auth)?;
    state
        .verified_identities
        .lock()
        .await
        .insert(verified.peer_id.clone(), verified.clone());
    let recommendation = peer_recommendation_from_presence(&peer, &state.relay_url);
    let (accepted, peer_to_join) =
        accept_mesh_recommendation(state, recommendation, Some(&verified)).await?;
    if !accepted {
        state
            .verified_identities
            .lock()
            .await
            .remove(&verified.peer_id);
    }
    Ok(peer_to_join)
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
            let verified_identity = verified_mesh_identity_for_peer(state, &from).await;
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                return Ok(());
            }
            if state
                .db
                .allow_room_join(&crate::RoomHookContext {
                    peer_id: from.clone(),
                    room: join_room.clone(),
                    transport: HookTransport::Mesh,
                    peer: None,
                    verified_identity,
                })
                .into_result()
                .is_err()
            {
                return Ok(());
            }
            let (already_open, is_stale) = {
                let peers = state.peers.lock().await;
                let peer = peers.get(&from);
                let already_open = peer
                    .and_then(|peer| peer.channel.as_ref())
                    .is_some_and(mesh_channel_is_open);
                let is_stale = peer.is_some_and(|peer| {
                    !peer.channel.as_ref().is_some_and(mesh_channel_is_open)
                        && crate::clock::now_millis().saturating_sub(peer.created_at_millis)
                            >= STALE_MESH_PEER_MILLIS
                });
                (already_open, is_stale)
            };
            if already_open {
                return Ok(());
            }
            if is_stale {
                remove_mesh_peer_state(state, &from).await;
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
            let verified_identity = verified_mesh_identity_for_peer(state, &from).await;
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                return Ok(());
            }
            if state
                .db
                .allow_room_join(&crate::RoomHookContext {
                    peer_id: from.clone(),
                    room: offer_room.clone(),
                    transport: HookTransport::Mesh,
                    peer: None,
                    verified_identity,
                })
                .into_result()
                .is_err()
            {
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
            let verified_identity = verified_mesh_identity_for_peer(state, &from).await;
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                return Ok(());
            }
            if state
                .db
                .allow_room_join(&crate::RoomHookContext {
                    peer_id: from.clone(),
                    room: answer_room.clone(),
                    transport: HookTransport::Mesh,
                    peer: None,
                    verified_identity,
                })
                .into_result()
                .is_err()
            {
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
            let verified_identity = verified_mesh_identity_for_peer(state, &from).await;
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                return Ok(());
            }
            if state
                .db
                .allow_room_join(&crate::RoomHookContext {
                    peer_id: from.clone(),
                    room: ice_room.clone(),
                    transport: HookTransport::Mesh,
                    peer: None,
                    verified_identity,
                })
                .into_result()
                .is_err()
            {
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
            let verified_identity = verified_mesh_identity_for_peer(state, &from).await;
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                return Ok(());
            }
            if state
                .db
                .allow_room_join(&crate::RoomHookContext {
                    peer_id: from.clone(),
                    room: leave_room.clone(),
                    transport: HookTransport::Mesh,
                    peer: None,
                    verified_identity,
                })
                .into_result()
                .is_err()
            {
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
        let channel = connection
            .create_data_channel("primadb", None)
            .await
            .map_err(to_error)?;
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
    connection
        .add_ice_candidate(candidate)
        .await
        .map_err(to_error)
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
            created_at_millis: crate::clock::now_millis(),
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
    let remote_for_open = remote_peer.to_owned();
    channel.on_open(Box::new(move || {
        let open_state = open_state.clone();
        let remote_for_open = remote_for_open.clone();
        Box::pin(async move {
            let _ = flush_mesh_pending_state(&open_state).await;
            let _ = replay_outgoing_mesh_watches_for_peer(&open_state, &remote_for_open).await;
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
    state
        .incoming_watches
        .lock()
        .await
        .retain(|_, watch| watch.target_peer_id != remote_peer);

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
            RoutePayload::Presence { peer } => {
                maybe_send_mesh_auth_challenge_to_peer(state, remote_peer, &peer).await?;
                if state.session_auth.require_authenticated_peers
                    && verified_mesh_identity_for_peer(state, &peer.peer_id)
                        .await
                        .is_none()
                {
                    continue;
                }
                let recommendation = peer_recommendation_from_presence(&peer, &state.relay_url);
                let verified_identity =
                    verified_mesh_identity_for_peer(state, &recommendation.peer.peer_id).await;
                let _ =
                    accept_mesh_recommendation(state, recommendation, verified_identity.as_ref())
                        .await?;
            }
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let verified_identity = verified_mesh_identity_for_peer(state, remote_peer).await;
                if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                    continue;
                }
                match state.db.serve_pull_request_for_peer(
                    remote_peer,
                    HookTransport::Mesh,
                    &format!("snapshot:{remote_peer}"),
                    &crate::PullRequestKind::Snapshot { root: root.clone() },
                    verified_identity.as_ref(),
                )? {
                    crate::HookDecision::Allow {
                        value: crate::RemoteResult::Snapshot { snapshot },
                    } => {
                        let response = state.router.snapshot_response(
                            root,
                            snapshot,
                            RouteTarget::Peer(remote_peer.to_owned()),
                        );
                        send_mesh_route_to_peer(state, remote_peer, &response).await?;
                    }
                    crate::HookDecision::Allow { .. } => {}
                    crate::HookDecision::Deny { .. } => {}
                }
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state.db.load_snapshot(snapshot)?;
            }
            RoutePayload::Sync { encoding, payload } => {
                if state.session_auth.require_authenticated_peers
                    && verified_mesh_identity_for_peer(state, remote_peer)
                        .await
                        .is_none()
                {
                    continue;
                }
                let frame = decode_sync_payload(&state.db, &encoding, payload)?;
                handle_mesh_sync_frame(state, remote_peer, frame).await?;
            }
            RoutePayload::PeerExchange { peers } => {
                for recommendation in peers {
                    maybe_send_mesh_auth_challenge_to_peer(
                        state,
                        remote_peer,
                        &recommendation.peer,
                    )
                    .await?;
                    if state.session_auth.require_authenticated_peers
                        && verified_mesh_identity_for_peer(state, &recommendation.peer.peer_id)
                            .await
                            .is_none()
                    {
                        continue;
                    }
                    let verified_identity =
                        verified_mesh_identity_for_peer(state, &recommendation.peer.peer_id).await;
                    let _ = accept_mesh_recommendation(
                        state,
                        recommendation,
                        verified_identity.as_ref(),
                    )
                    .await?;
                }
            }
            RoutePayload::AuthChallenge { challenge } => {
                handle_mesh_auth_challenge_to_peer(state, remote_peer, challenge).await?;
            }
            RoutePayload::AuthResponse { response } => {
                let _ = handle_mesh_auth_response(state, response).await?;
            }
            RoutePayload::Batch { items } => {
                for item in items.into_iter().rev() {
                    pending.push(RoutePayload::from_batch_item(item));
                }
            }
            RoutePayload::WatchRequest { request } => {
                handle_mesh_watch_request(state, remote_peer, request).await?;
            }
            RoutePayload::WatchEvent { event } => {
                accept_mesh_watch_event(state, event).await?;
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
            if state.session_auth.require_authenticated_peers
                && verified_mesh_identity_for_peer(state, remote_peer)
                    .await
                    .is_none()
            {
                return Ok(());
            }
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

async fn start_mesh_watch_with_policy(
    state: &Arc<NativeWebRtcMeshState>,
    policy: RemoteInterestPolicy,
    request_kind: crate::PullRequestKind,
) -> Result<RemoteWatchSubscription> {
    let capability = format!("watch_{}", request_kind.kind_name());
    let peer_id =
        select_mesh_peer_for_policy(state, &policy, Some(&capability), Some(&request_kind)).await?;
    start_mesh_watch(state, peer_id, request_kind).await
}

async fn start_mesh_watch(
    state: &Arc<NativeWebRtcMeshState>,
    target_peer_id: String,
    request_kind: crate::PullRequestKind,
) -> Result<RemoteWatchSubscription> {
    let limit = state.db.limits().max_active_remote_watches.max(1);
    let watch_id = format!(
        "{}/watch/{:x}",
        state.db.replica_id(),
        state.next_message_seq.fetch_add(1, Ordering::SeqCst) + 1
    );
    let (sender, receiver) = unbounded();
    {
        let mut outgoing = state.outgoing_watches.lock().await;
        if outgoing.len() >= limit {
            return Err(PrimadbError::TooManyRemoteWatches { limit });
        }
        outgoing.insert(
            watch_id.clone(),
            OutgoingWatch {
                sender,
                target_peer_id: target_peer_id.clone(),
                request_kind: request_kind.clone(),
                pending_sequence: None,
                last_delivered_sequence: None,
            },
        );
    }

    let _ = send_mesh_watch_request(
        state,
        &target_peer_id,
        WatchRequest {
            watch_id: watch_id.clone(),
            request: WatchRequestKind::Subscribe {
                request: request_kind,
            },
        },
    )
    .await;

    let cancel_state = state.clone();
    let runtime = state.runtime.clone();
    Ok(RemoteWatchSubscription::new(receiver, move || {
        runtime.spawn({
            let cancel_state = cancel_state.clone();
            let watch_id = watch_id.clone();
            async move {
                cancel_mesh_watch(&cancel_state, &watch_id).await;
            }
        });
    }))
}

async fn select_mesh_peer_for_policy(
    state: &Arc<NativeWebRtcMeshState>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&crate::PullRequestKind>,
) -> Result<String> {
    if let Some(peer_ids) = explicit_mesh_policy_peers(policy)? {
        if peer_ids.is_empty() {
            return Err(PrimadbError::Message(
                "remote interest policy did not include any peer ids".to_owned(),
            ));
        }
        if !policy.require_capability {
            return Ok(peer_ids[0].clone());
        }
        let recommendations = state.recommendations.lock().await;
        return peer_ids
            .into_iter()
            .find(|peer_id| {
                recommendations.get(peer_id).is_some_and(|recommendation| {
                    peer_supports_request(&recommendation.peer, capability, request)
                })
            })
            .ok_or_else(|| {
                PrimadbError::Message(format!(
                    "no requested peer advertises required capability `{}`",
                    capability.unwrap_or("unknown")
                ))
            });
    }

    let mut candidates = mesh_peer_candidates(state).await;
    prefer_vector_request_candidates(&mut candidates, request);
    if let Some(peer) = candidates
        .iter()
        .find(|peer| peer_supports_request(peer, capability, request))
    {
        return Ok(peer.peer_id.clone());
    }
    if !policy.require_capability {
        if let Some(peer) = candidates.first() {
            return Ok(peer.peer_id.clone());
        }
    }
    Err(PrimadbError::Message(match capability {
        Some(capability) => format!("no open mesh peer advertises capability `{capability}`"),
        None => "no open mesh peer is available for remote interest".to_owned(),
    }))
}

fn explicit_mesh_policy_peers(policy: &RemoteInterestPolicy) -> Result<Option<Vec<String>>> {
    match policy.target {
        RemoteInterestTarget::Any => {
            if !policy.peers.is_empty() {
                Ok(Some(policy.peers.clone()))
            } else {
                Ok(policy.peer_id.clone().map(|peer_id| vec![peer_id]))
            }
        }
        RemoteInterestTarget::Peer => policy
            .peer_id
            .clone()
            .map(|peer_id| Some(vec![peer_id]))
            .ok_or_else(|| {
                PrimadbError::Message("remote interest policy target `peer` requires peerId".into())
            }),
        RemoteInterestTarget::Peers => Ok(Some(policy.peers.clone())),
    }
}

async fn mesh_peer_candidates(state: &Arc<NativeWebRtcMeshState>) -> Vec<crate::PeerPresence> {
    let open_peer_ids = state
        .peers
        .lock()
        .await
        .iter()
        .filter_map(|(peer_id, peer)| {
            peer.channel
                .as_ref()
                .is_some_and(mesh_channel_is_open)
                .then_some(peer_id.clone())
        })
        .collect::<Vec<_>>();
    let mut recommendations = state
        .recommendations
        .lock()
        .await
        .values()
        .cloned()
        .collect::<Vec<_>>();
    recommendations.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.peer.peer_id.cmp(&right.peer.peer_id))
    });

    let mut candidates = Vec::new();
    for recommendation in recommendations {
        if open_peer_ids.contains(&recommendation.peer.peer_id)
            && !candidates
                .iter()
                .any(|peer: &crate::PeerPresence| peer.peer_id == recommendation.peer.peer_id)
        {
            candidates.push(recommendation.peer);
        }
    }
    for peer_id in open_peer_ids {
        if !candidates
            .iter()
            .any(|candidate| candidate.peer_id == peer_id)
        {
            candidates.push(crate::PeerPresence {
                peer_id,
                replica_id: String::new(),
                transport: "webrtc".to_owned(),
                identity: None,
                capabilities: Vec::new(),
                topics: Vec::new(),
                metadata: BTreeMap::new(),
            });
        }
    }
    if state.session_auth.require_authenticated_peers {
        let verified = state.verified_identities.lock().await;
        candidates.retain(|peer| verified.contains_key(&peer.peer_id));
    }
    candidates
}

fn peer_supports_capability(peer: &crate::PeerPresence, capability: Option<&str>) -> bool {
    capability.is_none_or(|capability| peer.capabilities.iter().any(|item| item == capability))
}

fn peer_supports_request(
    peer: &crate::PeerPresence,
    capability: Option<&str>,
    request: Option<&crate::PullRequestKind>,
) -> bool {
    if !peer_supports_capability(peer, capability) {
        return false;
    }
    vector_request_hint_score(peer, request) != Some(0)
}

fn prefer_vector_request_candidates(
    candidates: &mut [crate::PeerPresence],
    request: Option<&crate::PullRequestKind>,
) {
    candidates.sort_by(|left, right| {
        vector_request_hint_score(right, request)
            .unwrap_or(1)
            .cmp(&vector_request_hint_score(left, request).unwrap_or(1))
    });
}

fn vector_request_hint_score(
    peer: &crate::PeerPresence,
    request: Option<&crate::PullRequestKind>,
) -> Option<u8> {
    let Some(crate::PullRequestKind::VectorSearch {
        collection,
        query,
        spec,
    }) = request
    else {
        return None;
    };
    let prefix = format!("vector_collection:{}:", crate::encode_component(collection));
    let hints = peer
        .capabilities
        .iter()
        .filter(|item| item.starts_with(&prefix))
        .collect::<Vec<_>>();
    if hints.is_empty() {
        return None;
    }
    let allow_stale = spec.stale_policy == crate::VectorStalePolicy::AllowStale;
    let query_dim = query.len().to_string();
    Some(
        hints
            .iter()
            .any(|hint| {
                let parts = hint.split(':').collect::<Vec<_>>();
                parts.len() >= 6 && parts[2] == query_dim && (allow_stale || parts[4] == "ready")
            })
            .then_some(2)
            .unwrap_or(0),
    )
}

async fn cancel_mesh_watch(state: &Arc<NativeWebRtcMeshState>, watch_id: &str) {
    let Some(watch) = state.outgoing_watches.lock().await.remove(watch_id) else {
        return;
    };
    let _ = send_mesh_watch_request(
        state,
        &watch.target_peer_id,
        WatchRequest {
            watch_id: watch_id.to_owned(),
            request: WatchRequestKind::Cancel,
        },
    )
    .await;
}

async fn send_mesh_watch_request(
    state: &Arc<NativeWebRtcMeshState>,
    target_peer_id: &str,
    request: WatchRequest,
) -> Result<()> {
    let route = state.router.wrap_watch_request(
        request,
        RouteTarget::Peer(target_peer_id.to_owned()),
        None,
    );
    send_mesh_route_to_peer(state, target_peer_id, &route).await
}

async fn replay_outgoing_mesh_watches_for_peer(
    state: &Arc<NativeWebRtcMeshState>,
    peer_id: &str,
) -> Result<usize> {
    let requests = {
        let outgoing = state.outgoing_watches.lock().await;
        outgoing
            .iter()
            .filter(|(_, watch)| watch.target_peer_id == peer_id)
            .map(|(watch_id, watch)| WatchRequest {
                watch_id: watch_id.clone(),
                request: WatchRequestKind::Subscribe {
                    request: watch.request_kind.clone(),
                },
            })
            .collect::<Vec<_>>()
    };
    for request in &requests {
        send_mesh_watch_request(state, peer_id, request.clone()).await?;
    }
    Ok(requests.len())
}

async fn handle_mesh_watch_request(
    state: &Arc<NativeWebRtcMeshState>,
    remote_peer: &str,
    request: WatchRequest,
) -> Result<()> {
    match request.request {
        WatchRequestKind::Subscribe {
            request: incoming_request_kind,
        } => {
            let verified_identity = verified_mesh_identity_for_peer(state, remote_peer).await;
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                let route = state.router.wrap_watch_event(
                    error_watch_event(&request.watch_id, 0, true, "peer is not authenticated"),
                    RouteTarget::Peer(remote_peer.to_owned()),
                    None,
                );
                send_mesh_route_to_peer(state, remote_peer, &route).await?;
                return Ok(());
            }
            let request_kind = match state
                .db
                .authorize_watch_request_for_peer(
                    remote_peer,
                    HookTransport::Mesh,
                    &request.watch_id,
                    &incoming_request_kind,
                    verified_identity.as_ref(),
                )
                .into_result()
            {
                Ok(request_kind) => request_kind,
                Err(message) => {
                    let route = state.router.wrap_watch_event(
                        error_watch_event(&request.watch_id, 0, true, message),
                        RouteTarget::Peer(remote_peer.to_owned()),
                        None,
                    );
                    send_mesh_route_to_peer(state, remote_peer, &route).await?;
                    return Ok(());
                }
            };
            let limit = state.db.limits().max_active_remote_watches.max(1);
            {
                let mut incoming = state.incoming_watches.lock().await;
                if incoming.len() >= limit && !incoming.contains_key(&request.watch_id) {
                    return Err(PrimadbError::TooManyRemoteWatches { limit });
                }
                let interest_path = request_kind.interest_path();
                incoming.insert(
                    request.watch_id.clone(),
                    IncomingWatch {
                        target_peer_id: remote_peer.to_owned(),
                        request_kind,
                        interest_path,
                        next_sequence: 0,
                        last_hash: None,
                    },
                );
            }
            let _ = emit_single_incoming_mesh_watch_update(state, &request.watch_id, true).await?;
        }
        WatchRequestKind::Cancel => {
            state
                .incoming_watches
                .lock()
                .await
                .remove(&request.watch_id);
        }
    }
    Ok(())
}

async fn accept_mesh_watch_event(
    state: &Arc<NativeWebRtcMeshState>,
    event: WatchEvent,
) -> Result<()> {
    let mut deliver: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        RemoteWatchMessage,
    )> = None;
    let mut failure: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        String,
    )> = None;

    {
        let mut outgoing = state.outgoing_watches.lock().await;
        let Some(watch) = outgoing.get_mut(&event.watch_id) else {
            return Ok(());
        };

        if watch
            .last_delivered_sequence
            .is_some_and(|last| event.sequence <= last)
        {
            return Ok(());
        }

        let pending_sequence = watch
            .pending_sequence
            .as_ref()
            .map(|pending| pending.sequence);
        if pending_sequence != Some(event.sequence) {
            if pending_sequence.is_some_and(|pending| event.sequence < pending) {
                return Ok(());
            }
            watch.pending_sequence = Some(PendingWatchSequence {
                sequence: event.sequence,
                initial: event.initial,
                accumulator: PullAccumulator::new(&watch.request_kind),
            });
        }

        let Some(pending) = watch.pending_sequence.as_mut() else {
            return Ok(());
        };
        if let Some(message) = apply_response_body(&mut pending.accumulator, &event.result) {
            let sender = watch.sender.clone();
            outgoing.remove(&event.watch_id);
            failure = Some((sender, message));
        } else if event.done || event.chunk.index.saturating_add(1) >= event.chunk.total {
            let sender = watch.sender.clone();
            let pending = watch.pending_sequence.take().unwrap();
            let result = pending.accumulator.into_result()?;
            watch.last_delivered_sequence = Some(event.sequence);
            deliver = Some((
                sender,
                RemoteWatchMessage {
                    initial: pending.initial,
                    result,
                },
            ));
        }
    }

    if let Some((sender, message)) = deliver {
        let _ = sender.try_send(Ok(message));
    }
    if let Some((sender, message)) = failure {
        let _ = sender.try_send(Err(message));
    }
    Ok(())
}

async fn emit_incoming_mesh_watch_updates(
    state: &Arc<NativeWebRtcMeshState>,
    event: &crate::ChangeEvent,
) -> Result<usize> {
    let watch_ids = {
        let incoming = state.incoming_watches.lock().await;
        incoming
            .iter()
            .filter_map(|(watch_id, watch)| {
                incoming_watch_overlaps_event(watch, event).then_some(watch_id.clone())
            })
            .collect::<Vec<_>>()
    };
    let mut emitted = 0;
    for watch_id in watch_ids {
        if emit_single_incoming_mesh_watch_update(state, &watch_id, false).await? {
            emitted += 1;
        }
    }
    Ok(emitted)
}

async fn emit_single_incoming_mesh_watch_update(
    state: &Arc<NativeWebRtcMeshState>,
    watch_id: &str,
    initial: bool,
) -> Result<bool> {
    let Some(watch) = state.incoming_watches.lock().await.get(watch_id).cloned() else {
        return Ok(false);
    };
    let verified_identity = verified_mesh_identity_for_peer(state, &watch.target_peer_id).await;
    if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
        let route = state.router.wrap_watch_event(
            error_watch_event(
                watch_id,
                watch.next_sequence,
                initial,
                "peer is not authenticated",
            ),
            RouteTarget::Peer(watch.target_peer_id.clone()),
            None,
        );
        send_mesh_route_to_peer(state, &watch.target_peer_id, &route).await?;
        state.incoming_watches.lock().await.remove(watch_id);
        return Ok(true);
    }
    let decision = state.db.serve_watch_result_for_peer(
        &watch.target_peer_id,
        HookTransport::Mesh,
        watch_id,
        &watch.request_kind,
        initial,
        verified_identity.as_ref(),
    )?;
    let (result, content_hash, denied_message) = match decision {
        crate::HookDecision::Allow { value } => {
            let content_hash = crate::stable_content_hash(&value);
            (value, content_hash, None)
        }
        crate::HookDecision::Deny { message } => (
            crate::RemoteResult::Get { value: None },
            None,
            Some(message),
        ),
    };
    if let Some(message) = denied_message {
        let route = state.router.wrap_watch_event(
            error_watch_event(watch_id, watch.next_sequence, initial, message),
            RouteTarget::Peer(watch.target_peer_id.clone()),
            None,
        );
        send_mesh_route_to_peer(state, &watch.target_peer_id, &route).await?;
        state.incoming_watches.lock().await.remove(watch_id);
        return Ok(true);
    }
    if !initial && content_hash == watch.last_hash {
        return Ok(false);
    }

    let items = state
        .db
        .chunk_watch_result(watch_id, watch.next_sequence, initial, result)
        .into_iter()
        .map(|event| RouteBatchItem::WatchEvent { event })
        .collect::<Vec<_>>();
    for route in pack_batch_routes(
        &state.router,
        RouteTarget::Peer(watch.target_peer_id.clone()),
        None,
        items,
        state.db.limits().max_batch_items_per_route.max(1),
    ) {
        send_mesh_route_to_peer(state, &watch.target_peer_id, &route).await?;
    }

    if let Some(entry) = state.incoming_watches.lock().await.get_mut(watch_id) {
        entry.last_hash = content_hash;
        entry.next_sequence = entry.next_sequence.saturating_add(1);
    }
    Ok(true)
}

async fn fail_outgoing_mesh_watches(state: &Arc<NativeWebRtcMeshState>, message: &str) {
    let outgoing = {
        let mut outgoing = state.outgoing_watches.lock().await;
        std::mem::take(&mut *outgoing)
    };
    for watch in outgoing.into_values() {
        let _ = watch.sender.try_send(Err(message.to_owned()));
    }
}

async fn clear_incoming_mesh_watches(state: &Arc<NativeWebRtcMeshState>) {
    state.incoming_watches.lock().await.clear();
}

fn incoming_watch_overlaps_event(watch: &IncomingWatch, event: &crate::ChangeEvent) -> bool {
    if event.full_refresh {
        return true;
    }
    if let crate::PullRequestKind::Records { scan } = &watch.request_kind {
        return event.records_changed
            && (event.touched_record_keys.is_empty()
                || event
                    .touched_record_keys
                    .iter()
                    .any(|key| scan.matches_key(key)));
    }
    if let crate::PullRequestKind::VectorSearch { collection, .. } = &watch.request_kind {
        return event.records_changed
            && (event.touched_record_keys.is_empty()
                || event.touched_record_keys.iter().any(|key| {
                    crate::vector_collection_from_record_key(key).as_deref()
                        == Some(collection.as_str())
                }));
    }
    event.full_refresh
        || watch.interest_path.as_ref().map_or(true, |path| {
            event
                .touched_paths
                .iter()
                .any(|changed| paths_overlap(path, changed))
        })
}

fn paths_overlap(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
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
        if send_mesh_route_to_peer(state, peer_id, &route)
            .await
            .is_ok()
        {
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

async fn connect_mesh_relay_state(state: &Arc<NativeWebRtcMeshState>) -> Result<bool> {
    struct ConnectAttemptGuard<'a> {
        flag: &'a AtomicBool,
    }

    impl Drop for ConnectAttemptGuard<'_> {
        fn drop(&mut self) {
            self.flag.store(false, Ordering::SeqCst);
        }
    }

    if state.closed.load(Ordering::SeqCst) {
        return Ok(false);
    }
    if state.relay_connected.load(Ordering::SeqCst) {
        return Ok(false);
    }
    if state.relay_connecting.swap(true, Ordering::SeqCst) {
        return Ok(false);
    }
    let _attempt_guard = ConnectAttemptGuard {
        flag: &state.relay_connecting,
    };
    if state.closed.load(Ordering::SeqCst) || state.relay_connected.load(Ordering::SeqCst) {
        return Ok(false);
    }

    let (socket, _) = connect_async(&state.relay_url)
        .await
        .map_err(|error| PrimadbError::Message(error.to_string()))?;
    let (writer, mut reader) = socket.split();

    {
        let mut relay = state.relay.lock().await;
        if state.closed.load(Ordering::SeqCst) {
            return Ok(false);
        }
        if relay.writer.is_some() {
            return Ok(false);
        }
        relay.writer = Some(writer);
    }

    state.relay_connected.store(true, Ordering::SeqCst);

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
        disconnect_mesh_relay_state(&reader_state, false).await;
    });

    {
        let mut relay = state.relay.lock().await;
        if let Some(task) = relay.reader_task.take() {
            task.abort();
        }
        relay.reader_task = Some(reader_task);
    }

    send_mesh_presence_state(state).await?;
    announce_mesh_join_state(state).await?;
    Ok(true)
}

async fn disconnect_mesh_relay_state(state: &Arc<NativeWebRtcMeshState>, abort_reader: bool) {
    state.relay_connected.store(false, Ordering::SeqCst);
    let reader_task = {
        let mut relay = state.relay.lock().await;
        relay.writer.take();
        if abort_reader {
            relay.reader_task.take()
        } else {
            relay.reader_task = None;
            None
        }
    };
    if let Some(task) = reader_task {
        task.abort();
    }
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
    send_route(state, &route).await
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
    let mut capabilities = vec![
        "signal".to_owned(),
        "webrtc".to_owned(),
        "peer_exchange".to_owned(),
        "watch_get".to_owned(),
        "watch_map".to_owned(),
        "watch_query".to_owned(),
        "watch_lex".to_owned(),
        "watch_records".to_owned(),
        "watch_vector_search".to_owned(),
        "watch_node".to_owned(),
        "watch_snapshot".to_owned(),
    ];
    capabilities.extend(state.db.vector_presence_capabilities());
    let mut route = state.router.presence(
        state.db.replica_id(),
        "webrtc-relay",
        capabilities,
        vec![format!("mesh:{}", state.room)],
    );
    if let RoutePayload::Presence { peer } = &mut route.payload {
        peer.metadata
            .insert("relay_url".to_owned(), state.relay_url.clone());
        peer.metadata
            .insert("mesh_room".to_owned(), state.room.clone());
        peer.metadata
            .insert("signaling".to_owned(), "relay".to_owned());
        peer.identity = state.db.session_presence_identity(&state.session_id);
    }
    send_route(state, &route).await
}

async fn send_route(state: &Arc<NativeWebRtcMeshState>, route: &RouteEnvelope) -> Result<()> {
    let payload = serde_json::to_string(route)?;
    let max_bytes = state.db.limits().max_route_payload_bytes;
    if payload.len() > max_bytes {
        return Err(PrimadbError::Message(format!(
            "route payload exceeds {max_bytes} bytes"
        )));
    }
    let send_result = {
        let mut relay = state.relay.lock().await;
        let Some(writer) = relay.writer.as_mut() else {
            return Err(PrimadbError::Message(
                "mesh relay websocket is not connected".to_owned(),
            ));
        };
        writer.send(Message::Text(payload.into())).await
    };
    match send_result {
        Ok(()) => Ok(()),
        Err(error) => {
            disconnect_mesh_relay_state(state, true).await;
            Err(PrimadbError::Message(error.to_string()))
        }
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
    items
        .chunks(batch_size.max(1))
        .map(|chunk: &[RouteBatchItem]| {
            if chunk.len() == 1 {
                router.wrap_batch_item(chunk[0].clone(), target.clone(), reply_to.clone())
            } else {
                router.wrap_batch(chunk.to_vec(), target.clone(), reply_to.clone())
            }
        })
        .collect()
}

fn apply_response_body(
    accumulator: &mut PullAccumulator,
    result: &crate::PullResponseBody,
) -> Option<String> {
    match result {
        crate::PullResponseBody::Get { value } => {
            *accumulator = PullAccumulator::Get {
                value: value.clone(),
            };
            None
        }
        crate::PullResponseBody::Map { entries } => {
            if let PullAccumulator::Map { entries: current } = accumulator {
                current.extend(entries.clone());
            }
            None
        }
        crate::PullResponseBody::Query { entries } => {
            if let PullAccumulator::Query { entries: current } = accumulator {
                current.extend(entries.clone());
            }
            None
        }
        crate::PullResponseBody::Lex { entries } => {
            if let PullAccumulator::Lex { entries: current } = accumulator {
                current.extend(entries.clone());
            }
            None
        }
        crate::PullResponseBody::Records {
            entries,
            next_cursor,
        } => {
            if let PullAccumulator::Records {
                entries: current,
                next_cursor: current_cursor,
            } = accumulator
            {
                current.extend(entries.clone());
                if next_cursor.is_some() {
                    *current_cursor = next_cursor.clone();
                }
            }
            None
        }
        crate::PullResponseBody::VectorSearch { result } => {
            *accumulator = PullAccumulator::VectorSearch {
                result: Some(result.clone()),
            };
            None
        }
        crate::PullResponseBody::Node { node } => {
            *accumulator = PullAccumulator::Node { node: node.clone() };
            None
        }
        crate::PullResponseBody::Snapshot {
            clock,
            nodes,
            pending_ops,
            scope_policies,
        } => {
            if let PullAccumulator::Snapshot {
                clock: current_clock,
                nodes: current_nodes,
                pending_ops: current_ops,
                scope_policies: current_scope_policies,
            } = accumulator
            {
                if current_clock.is_none() {
                    *current_clock = clock.clone();
                }
                current_nodes.extend(nodes.clone());
                current_ops.extend(pending_ops.clone());
                current_scope_policies.extend(scope_policies.clone());
            }
            None
        }
        crate::PullResponseBody::Transaction { report } => {
            *accumulator = PullAccumulator::Transaction {
                report: Some(report.clone()),
            };
            None
        }
        crate::PullResponseBody::Error { message } => Some(message.clone()),
    }
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
        return match frame {
            SecureSyncFrame::Plain(frame) => {
                Ok(("sync_frame".to_owned(), serde_json::to_value(frame)?))
            }
            secure => Ok((
                "secure_sync_frame".to_owned(),
                serde_json::to_value(secure)?,
            )),
        };
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

fn peer_recommendation_from_presence(
    peer: &crate::PeerPresence,
    relay_url: &str,
) -> PeerRecommendation {
    PeerRecommendation {
        peer: peer.clone(),
        relay_urls: vec![relay_url.to_owned()],
        score: 100,
        discovered_at_millis: crate::clock::now_millis(),
    }
}

impl NodeFetchScheduler for NativeMeshNodeFetchScheduler {
    fn fetch_nodes(&self, nodes: Vec<String>) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let runtime = state.runtime.clone();
        runtime.spawn(async move {
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
            for node_id in nodes {
                let mut fetched = false;
                for peer_id in &peer_ids {
                    let subscription = match start_mesh_watch(
                        &state,
                        peer_id.clone(),
                        crate::PullRequestKind::Node {
                            id: node_id.clone(),
                        },
                    )
                    .await
                    {
                        Ok(subscription) => subscription,
                        Err(_) => continue,
                    };
                    let message = subscription.recv().await;
                    subscription.close();
                    if let Some(Ok(crate::RemoteWatchMessage {
                        result: crate::RemoteResult::Node { node: Some(node) },
                        ..
                    })) = message
                    {
                        let _ = state.db.apply_node_state(node);
                        fetched = true;
                        break;
                    }
                }
                if !fetched {
                    state.db.clear_scheduled_node_fetch(&node_id);
                }
            }
        });
    }
}

impl PullAccumulator {
    fn new(request: &crate::PullRequestKind) -> Self {
        match request {
            crate::PullRequestKind::Get { .. } => Self::Get { value: None },
            crate::PullRequestKind::Map { .. } => Self::Map {
                entries: Vec::new(),
            },
            crate::PullRequestKind::Query { .. } => Self::Query {
                entries: Vec::new(),
            },
            crate::PullRequestKind::Lex { .. } => Self::Lex {
                entries: Vec::new(),
            },
            crate::PullRequestKind::Records { .. } => Self::Records {
                entries: Vec::new(),
                next_cursor: None,
            },
            crate::PullRequestKind::VectorSearch { .. } => Self::VectorSearch { result: None },
            crate::PullRequestKind::Node { .. } => Self::Node { node: None },
            crate::PullRequestKind::Snapshot { .. } => Self::Snapshot {
                clock: None,
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
                scope_policies: BTreeMap::new(),
            },
            crate::PullRequestKind::Transaction { .. } => Self::Transaction { report: None },
        }
    }

    fn into_result(self) -> Result<crate::RemoteResult> {
        match self {
            Self::Get { value } => Ok(crate::RemoteResult::Get { value }),
            Self::Map { entries } => Ok(crate::RemoteResult::Map { entries }),
            Self::Query { entries } => Ok(crate::RemoteResult::Query { entries }),
            Self::Lex { entries } => Ok(crate::RemoteResult::Lex { entries }),
            Self::Records {
                entries,
                next_cursor,
            } => Ok(crate::RemoteResult::Records {
                result: RecordScanResult {
                    entries,
                    next_cursor,
                },
            }),
            Self::VectorSearch { result } => Ok(crate::RemoteResult::VectorSearch {
                result: result.ok_or_else(|| {
                    PrimadbError::Message(
                        "vector search response completed without a result".to_owned(),
                    )
                })?,
            }),
            Self::Node { node } => Ok(crate::RemoteResult::Node { node }),
            Self::Snapshot {
                clock,
                nodes,
                pending_ops,
                scope_policies,
            } => Ok(crate::RemoteResult::Snapshot {
                snapshot: crate::DatabaseSnapshot {
                    clock: clock.ok_or_else(|| {
                        PrimadbError::Message("snapshot watch completed without a clock".to_owned())
                    })?,
                    nodes,
                    pending_ops,
                    scope_policies,
                    provisional_transactions: Default::default(),
                    next_provisional_transaction_id: 0,
                },
            }),
            Self::Transaction { report } => Ok(crate::RemoteResult::Transaction {
                report: report.ok_or_else(|| {
                    PrimadbError::Message(
                        "transaction response completed without a report".to_owned(),
                    )
                })?,
            }),
        }
    }
}

fn to_error(error: impl ToString) -> PrimadbError {
    PrimadbError::Message(error.to_string())
}
