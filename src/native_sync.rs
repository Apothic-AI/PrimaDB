#[cfg(feature = "native-moq")]
use crate::MoqRelayClientConfig;
#[cfg(feature = "crypto")]
use crate::SecureSyncFrame;
use crate::app_route::ApplicationRouteBus;
#[cfg(feature = "native-moq")]
use crate::native_moq::NativeMoqRouteClient;
use crate::{
    ApplicationRouteContext, ApplicationRouteEvent, ApplicationRouteFilter,
    ApplicationRouteMessage, ApplicationRouteSubscription, ChangeSubscription, HookTransport,
    HybridClock, LexEntry, MapEntry, NodeFetchScheduler, Operation, PeerPresence,
    PeerRecommendation, Primadb, PrimadbError, RecordEntry, RecordScanResult, RelayClientConfig,
    RemoteFanInWatch, RemoteFanInWatchEvent, RemoteInterestPolicy, RemoteInterestTarget,
    RemotePath, RemotePeerFailure, RemotePeerRecords, RemoteRecordsFanIn, RemoteResult,
    RemoteWatchMessage, RemoteWatchSubscription, Result, RouteBatchItem, RouteEnvelope,
    RouteOverlayUnderlayHandle, RouteOverlayUnderlayInfo, RoutePayload, RouteTarget,
    RouteTransportKind, Router, RouterConfig, SyncEnvelope, SyncFrame, VerifiedIdentity,
    WatchEvent, WatchRequest, WatchRequestKind, error_pull_response, error_watch_event,
    merge_remote_records_fan_in,
};
use async_channel::{Sender, bounded, unbounded};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
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

#[derive(Debug, Clone)]
enum NativeRouteOutbound {
    Route(RouteEnvelope),
    Close,
}

#[derive(Debug)]
struct PendingPullRequest {
    sender: Sender<std::result::Result<RemoteResult, String>>,
    accumulator: PullAccumulator,
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
        entries: Vec<MapEntry>,
    },
    Query {
        entries: Vec<MapEntry>,
    },
    Lex {
        entries: Vec<LexEntry>,
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
        clock: Option<HybridClock>,
        nodes: BTreeMap<String, crate::NodeState>,
        pending_ops: Vec<Operation>,
        scope_policies: BTreeMap<String, crate::ScopePolicy>,
    },
    Transaction {
        report: Option<crate::TransactionReport>,
    },
}

#[derive(Debug)]
struct NativeWebSocketSyncState {
    db: Primadb,
    router: Router,
    transport: RouteTransportKind,
    applications: ApplicationRouteBus,
    session_id: String,
    session_auth: crate::SessionAuthConfig,
    closed: AtomicBool,
    connected: AtomicBool,
    next_message_seq: AtomicU64,
    inflight: Mutex<BTreeMap<String, OutboundSync>>,
    pending_requests: Mutex<BTreeMap<String, PendingPullRequest>>,
    outgoing_watches: Mutex<BTreeMap<String, OutgoingWatch>>,
    incoming_watches: Mutex<BTreeMap<String, IncomingWatch>>,
    recommendations: Mutex<BTreeMap<String, PeerRecommendation>>,
    pending_auth_challenges: Mutex<BTreeMap<String, crate::AuthChallenge>>,
    pending_auth_peers: Mutex<BTreeMap<String, crate::PeerPresence>>,
    verified_identities: Mutex<BTreeMap<String, VerifiedIdentity>>,
    outbound: UnboundedSender<NativeRouteOutbound>,
}

pub struct NativeWebSocketSync {
    state: Arc<NativeWebSocketSyncState>,
    change_subscription: Option<ChangeSubscription>,
    connection_task: Option<JoinHandle<()>>,
    change_task: Option<JoinHandle<()>>,
    retry_task: Option<JoinHandle<()>>,
    node_fetch_registration: Option<u64>,
}

#[cfg(feature = "native-moq")]
pub struct NativeMoqSync {
    state: Arc<NativeWebSocketSyncState>,
    route_client: Arc<NativeMoqRouteClient>,
    inbound_task: Option<JoinHandle<()>>,
    outbound_task: Option<JoinHandle<()>>,
    change_subscription: Option<ChangeSubscription>,
    change_task: Option<JoinHandle<()>>,
    retry_task: Option<JoinHandle<()>>,
    node_fetch_registration: Option<u64>,
}

struct NativeWebSocketNodeFetchScheduler {
    state: Weak<NativeWebSocketSyncState>,
}

impl NativeWebSocketSync {
    pub async fn connect_with_config(db: Primadb, config: RelayClientConfig) -> Result<Self> {
        let retry_interval = Duration::from_millis(config.retry_interval_ms.max(1));
        Self::connect_with_session_auth(db, config.url, retry_interval, config.session_auth).await
    }

    pub async fn connect(
        db: Primadb,
        url: impl AsRef<str>,
        retry_interval: Duration,
    ) -> Result<Self> {
        Self::connect_with_session_auth(
            db,
            url,
            retry_interval,
            crate::SessionAuthConfig::default(),
        )
        .await
    }

    pub async fn connect_with_session_auth(
        db: Primadb,
        url: impl AsRef<str>,
        retry_interval: Duration,
        session_auth: crate::SessionAuthConfig,
    ) -> Result<Self> {
        let url = url.as_ref().to_owned();
        let (outbound, mut outbound_rx) = unbounded_channel::<NativeRouteOutbound>();

        let router = Router::new(RouterConfig {
            peer_id: format!("native:{}", db.replica_id()),
            default_channel: "primadb-sync".to_owned(),
            default_ttl: 6,
            max_seen_routes: db.limits().max_seen_routes,
        });

        let state = Arc::new(NativeWebSocketSyncState {
            db: db.clone(),
            router,
            transport: RouteTransportKind::WebSocket,
            applications: ApplicationRouteBus::default(),
            session_id: crate::session_auth::random_session_id(&format!(
                "native:{}",
                db.replica_id()
            )),
            session_auth,
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            next_message_seq: AtomicU64::new(0),
            inflight: Mutex::new(BTreeMap::new()),
            pending_requests: Mutex::new(BTreeMap::new()),
            outgoing_watches: Mutex::new(BTreeMap::new()),
            incoming_watches: Mutex::new(BTreeMap::new()),
            recommendations: Mutex::new(BTreeMap::new()),
            pending_auth_challenges: Mutex::new(BTreeMap::new()),
            pending_auth_peers: Mutex::new(BTreeMap::new()),
            verified_identities: Mutex::new(BTreeMap::new()),
            outbound,
        });

        let connection_state = state.clone();
        let connection_url = url.clone();
        let connection_task = tokio::spawn(async move {
            let presence =
                build_relay_presence_route(&connection_state, &connection_url, "websocket");
            let presence_payload = serde_json::to_string(&presence).ok();

            loop {
                if connection_state.closed.load(Ordering::SeqCst) {
                    break;
                }

                let socket = match connect_async(&connection_url).await {
                    Ok((socket, _)) => socket,
                    Err(_) => {
                        tokio::time::sleep(retry_interval).await;
                        continue;
                    }
                };
                let (mut writer, mut reader) = socket.split();
                connection_state.connected.store(true, Ordering::SeqCst);

                if let Some(payload) = &presence_payload {
                    if writer
                        .send(Message::Text(payload.clone().into()))
                        .await
                        .is_err()
                    {
                        connection_state.connected.store(false, Ordering::SeqCst);
                        requeue_inflight_state(&connection_state);
                        fail_pending_requests(
                            &connection_state,
                            "websocket closed while requests were in flight",
                        );
                        clear_incoming_watches(&connection_state);
                        tokio::time::sleep(retry_interval).await;
                        continue;
                    }
                }

                let _ = retry_inflight_state(&connection_state).await;
                let _ = flush_pending_state(&connection_state).await;

                loop {
                    if connection_state.closed.load(Ordering::SeqCst) {
                        let _ = writer.send(Message::Close(None)).await;
                        break;
                    }
                    tokio::select! {
                        maybe_message = outbound_rx.recv() => {
                            match maybe_message {
                                Some(NativeRouteOutbound::Route(route)) => {
                                    let Ok(payload) = serde_json::to_string(&route) else {
                                        continue;
                                    };
                                    if writer.send(Message::Text(payload.into())).await.is_err() {
                                        break;
                                    }
                                }
                                Some(NativeRouteOutbound::Close) => {
                                    let _ = writer.send(Message::Close(None)).await;
                                    break;
                                }
                                None => break,
                            }
                        }
                        maybe_incoming = reader.next() => {
                            match maybe_incoming {
                                Some(Ok(Message::Text(payload))) => {
                                    let payload = payload.to_string();
                                    let _ = handle_incoming_text(&connection_state, &payload).await;
                                }
                                Some(Ok(Message::Binary(_))) | Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                Some(Ok(Message::Frame(_))) => {}
                            }
                        }
                    }
                }

                connection_state.connected.store(false, Ordering::SeqCst);
                requeue_inflight_state(&connection_state);
                fail_pending_requests(
                    &connection_state,
                    "websocket closed while requests were in flight",
                );
                clear_incoming_watches(&connection_state);

                if connection_state.closed.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(retry_interval).await;
            }
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
                    let _ = flush_pending_state(&change_state).await;
                }
                if event.data_changed {
                    let _ = emit_incoming_watch_updates(&change_state, &event).await;
                }
            }
        });

        let retry_state = state.clone();
        let retry_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retry_interval);
            loop {
                interval.tick().await;
                if retry_state.closed.load(Ordering::SeqCst) {
                    break;
                }
                if !retry_state.connected.load(Ordering::SeqCst) {
                    continue;
                }
                let _ = retry_inflight_state(&retry_state).await;
                let _ = flush_pending_state(&retry_state).await;
            }
        });

        let node_fetch_registration =
            db.register_node_fetch_scheduler(Arc::new(NativeWebSocketNodeFetchScheduler {
                state: Arc::downgrade(&state),
            }));

        Ok(Self {
            state,
            change_subscription: Some(change_subscription),
            connection_task: Some(connection_task),
            change_task: Some(change_task),
            retry_task: Some(retry_task),
            node_fetch_registration: Some(node_fetch_registration),
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

    pub fn publish_application(
        &self,
        message: ApplicationRouteMessage,
        target: RouteTarget,
    ) -> Result<RouteEnvelope> {
        publish_application_state(&self.state, message, target, None)
    }

    pub fn send_route_envelope(&self, route: RouteEnvelope) -> Result<RouteEnvelope> {
        send_route(&self.state, &route)?;
        Ok(route)
    }

    pub fn send_application(
        &self,
        namespace: impl Into<String>,
        protocol: impl Into<String>,
        topic: Option<String>,
        body: JsonValue,
        metadata: BTreeMap<String, JsonValue>,
        target: RouteTarget,
    ) -> Result<RouteEnvelope> {
        publish_application_state(
            &self.state,
            ApplicationRouteMessage::new(namespace, protocol, topic, body, metadata),
            target,
            None,
        )
    }

    pub fn subscribe_applications(
        &self,
        filter: ApplicationRouteFilter,
    ) -> ApplicationRouteSubscription {
        self.state.applications.subscribe(filter)
    }

    pub fn route_overlay_underlay(&self, id: impl Into<String>) -> RouteOverlayUnderlayHandle {
        let state = self.state.clone();
        let send_state = state.clone();
        let connected_state = state.clone();
        let subscription = Arc::new(self.subscribe_applications(ApplicationRouteFilter::default()));
        RouteOverlayUnderlayHandle::new(
            RouteOverlayUnderlayInfo {
                id: id.into(),
                transport: RouteTransportKind::WebSocket,
                direct: false,
                relay_routed: true,
                connected: self.is_connected(),
                priority: 0,
                metadata: BTreeMap::new(),
            },
            move |route| send_route(&send_state, &route),
            Vec::new,
            move || connected_state.connected.load(Ordering::SeqCst),
        )
        .with_application_events(move || subscription.drain())
    }

    pub fn watch_get(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Get { path },
        )
    }

    pub fn watch_map(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Map { path },
        )
    }

    pub fn watch_query(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
        spec: crate::QuerySpec,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Query { path, spec },
        )
    }

    pub fn watch_lex(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
        spec: crate::LexSpec,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Lex { path, spec },
        )
    }

    pub fn watch_records(
        &self,
        peer_id: impl Into<String>,
        scan: crate::RecordScan,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Records { scan },
        )
    }

    pub fn watch_vector_search(
        &self,
        peer_id: impl Into<String>,
        collection: impl Into<String>,
        query: Vec<f32>,
        spec: crate::VectorSearchSpec,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::VectorSearch {
                collection: collection.into(),
                query,
                spec,
            },
        )
    }

    pub fn watch_node(
        &self,
        peer_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Node { id: id.into() },
        )
    }

    pub fn watch_snapshot(
        &self,
        peer_id: impl Into<String>,
        root: Option<String>,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Snapshot { root },
        )
    }

    pub fn watch_get_with_policy(
        &self,
        path: RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(&self.state, policy, crate::PullRequestKind::Get { path })
    }

    pub fn watch_map_with_policy(
        &self,
        path: RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(&self.state, policy, crate::PullRequestKind::Map { path })
    }

    pub fn watch_query_with_policy(
        &self,
        path: RemotePath,
        spec: crate::QuerySpec,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Query { path, spec },
        )
    }

    pub fn watch_lex_with_policy(
        &self,
        path: RemotePath,
        spec: crate::LexSpec,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Lex { path, spec },
        )
    }

    pub fn watch_records_with_policy(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Records { scan },
        )
    }

    pub fn watch_vector_search_with_policy(
        &self,
        collection: impl Into<String>,
        query: Vec<f32>,
        spec: crate::VectorSearchSpec,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::VectorSearch {
                collection: collection.into(),
                query,
                spec,
            },
        )
    }

    pub fn watch_node_with_policy(
        &self,
        id: impl Into<String>,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Node { id: id.into() },
        )
    }

    pub fn watch_snapshot_with_policy(
        &self,
        root: Option<String>,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Snapshot { root },
        )
    }

    pub async fn remote_get(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
    ) -> Result<Option<JsonValue>> {
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

    pub async fn remote_records(
        &self,
        peer_id: impl Into<String>,
        scan: crate::RecordScan,
    ) -> Result<RecordScanResult> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Records { scan },
        )
        .await?
        {
            RemoteResult::Records { result } => Ok(result),
            other => Err(PrimadbError::Message(format!(
                "expected records result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_vector_search(
        &self,
        peer_id: impl Into<String>,
        collection: impl Into<String>,
        query: Vec<f32>,
        spec: crate::VectorSearchSpec,
    ) -> Result<crate::VectorSearchResult> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::VectorSearch {
                collection: collection.into(),
                query,
                spec,
            },
        )
        .await?
        {
            RemoteResult::VectorSearch { result } => Ok(result),
            other => Err(PrimadbError::Message(format!(
                "expected vector_search result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_node(
        &self,
        peer_id: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<Option<crate::NodeState>> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Node { id: id.into() },
        )
        .await?
        {
            RemoteResult::Node { node } => Ok(node),
            other => Err(PrimadbError::Message(format!(
                "expected node result, received {other:?}"
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

    pub async fn remote_get_with_policy(
        &self,
        path: RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<Option<JsonValue>> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
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

    pub async fn remote_query_with_policy(
        &self,
        path: RemotePath,
        spec: crate::QuerySpec,
        policy: RemoteInterestPolicy,
    ) -> Result<Vec<MapEntry>> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
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

    pub async fn remote_lex_with_policy(
        &self,
        path: RemotePath,
        spec: crate::LexSpec,
        policy: RemoteInterestPolicy,
    ) -> Result<Vec<LexEntry>> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
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

    pub async fn remote_records_with_policy(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RecordScanResult> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Records { scan },
        )
        .await?
        {
            RemoteResult::Records { result } => Ok(result),
            other => Err(PrimadbError::Message(format!(
                "expected records result, received {other:?}"
            ))),
        }
    }

    pub async fn records_fan_in(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteRecordsFanIn> {
        records_fan_in_state(&self.state, scan, policy).await
    }

    pub fn watch_records_fan_in(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteFanInWatch> {
        watch_records_fan_in_state(&self.state, scan, policy)
    }

    pub async fn remote_vector_search_with_policy(
        &self,
        collection: impl Into<String>,
        query: Vec<f32>,
        spec: crate::VectorSearchSpec,
        policy: RemoteInterestPolicy,
    ) -> Result<crate::VectorSearchResult> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::VectorSearch {
                collection: collection.into(),
                query,
                spec,
            },
        )
        .await?
        {
            RemoteResult::VectorSearch { result } => Ok(result),
            other => Err(PrimadbError::Message(format!(
                "expected vector_search result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_node_with_policy(
        &self,
        id: impl Into<String>,
        policy: RemoteInterestPolicy,
    ) -> Result<Option<crate::NodeState>> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Node { id: id.into() },
        )
        .await?
        {
            RemoteResult::Node { node } => Ok(node),
            other => Err(PrimadbError::Message(format!(
                "expected node result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_snapshot_with_policy(
        &self,
        root: Option<String>,
        policy: RemoteInterestPolicy,
    ) -> Result<crate::DatabaseSnapshot> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
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

    pub async fn remote_transaction(
        &self,
        peer_id: impl Into<String>,
        scope: impl Into<String>,
        steps: Vec<crate::TransactionStep>,
        options: crate::TransactionOptions,
    ) -> Result<crate::TransactionReport> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Transaction {
                scope: scope.into(),
                steps,
                options,
            },
        )
        .await?
        {
            RemoteResult::Transaction { report } => Ok(report),
            other => Err(PrimadbError::Message(format!(
                "expected transaction result, received {other:?}"
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
        if let Some(id) = self.node_fetch_registration.take() {
            self.state.db.unregister_node_fetch_scheduler(id);
        }
        self.state.closed.store(true, Ordering::SeqCst);
        self.state.connected.store(false, Ordering::SeqCst);
        let _ = self.state.outbound.send(NativeRouteOutbound::Close);
        self.change_subscription.take();
        if let Some(task) = self.connection_task.take() {
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
        fail_outgoing_watches(&self.state, "connection closed");
        clear_incoming_watches(&self.state);
    }
}

impl Drop for NativeWebSocketSync {
    fn drop(&mut self) {
        self.teardown();
    }
}

#[cfg(feature = "native-moq")]
impl NativeMoqSync {
    pub async fn connect_with_config(db: Primadb, config: MoqRelayClientConfig) -> Result<Self> {
        let retry_interval = Duration::from_millis(config.retry_interval_ms.max(1));
        let route_client = Arc::new(NativeMoqRouteClient::connect(config.clone()).await?);
        let (outbound, mut outbound_rx) = unbounded_channel::<NativeRouteOutbound>();

        let router = Router::new(RouterConfig {
            peer_id: format!("native:{}", db.replica_id()),
            default_channel: config.channel.clone(),
            default_ttl: 6,
            max_seen_routes: db.limits().max_seen_routes,
        });

        let state = Arc::new(NativeWebSocketSyncState {
            db: db.clone(),
            router,
            transport: RouteTransportKind::Moq,
            applications: ApplicationRouteBus::default(),
            session_id: crate::session_auth::random_session_id(&format!(
                "moq:native:{}",
                db.replica_id()
            )),
            session_auth: config.session_auth.clone(),
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            next_message_seq: AtomicU64::new(0),
            inflight: Mutex::new(BTreeMap::new()),
            pending_requests: Mutex::new(BTreeMap::new()),
            outgoing_watches: Mutex::new(BTreeMap::new()),
            incoming_watches: Mutex::new(BTreeMap::new()),
            recommendations: Mutex::new(BTreeMap::new()),
            pending_auth_challenges: Mutex::new(BTreeMap::new()),
            pending_auth_peers: Mutex::new(BTreeMap::new()),
            verified_identities: Mutex::new(BTreeMap::new()),
            outbound,
        });

        let mut presence = build_relay_presence_route(&state, &config.url, "moq");
        if let RoutePayload::Presence { peer } = &mut presence.payload {
            peer.metadata
                .insert("moq_path".to_owned(), config.path.clone());
            peer.metadata
                .insert("moq_track".to_owned(), config.track.clone());
        }
        let _ = route_client.send_route(presence);

        let outbound_client = route_client.clone();
        let outbound_state = state.clone();
        let outbound_task = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                match message {
                    NativeRouteOutbound::Route(route) => {
                        let _ = outbound_client.send_route(route);
                    }
                    NativeRouteOutbound::Close => break,
                }
                if outbound_state.closed.load(Ordering::SeqCst) {
                    break;
                }
            }
        });

        let inbound_client = route_client.clone();
        let inbound_state = state.clone();
        let inbound_task = tokio::spawn(async move {
            loop {
                if inbound_state.closed.load(Ordering::SeqCst) {
                    break;
                }
                match inbound_client.recv_route().await {
                    Ok(route) => {
                        let _ = handle_route_envelope(&inbound_state, route).await;
                    }
                    Err(_) => {
                        if inbound_state.closed.load(Ordering::SeqCst) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
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
                    let _ = flush_pending_state(&change_state).await;
                }
                if event.data_changed {
                    let _ = emit_incoming_watch_updates(&change_state, &event).await;
                }
            }
        });

        let retry_state = state.clone();
        let retry_client = route_client.clone();
        let retry_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retry_interval);
            loop {
                interval.tick().await;
                if retry_state.closed.load(Ordering::SeqCst) {
                    break;
                }
                retry_state
                    .connected
                    .store(retry_client.is_connected(), Ordering::SeqCst);
                if !retry_state.connected.load(Ordering::SeqCst) {
                    continue;
                }
                let _ = retry_inflight_state(&retry_state).await;
                let _ = flush_pending_state(&retry_state).await;
            }
        });

        let node_fetch_registration =
            db.register_node_fetch_scheduler(Arc::new(NativeWebSocketNodeFetchScheduler {
                state: Arc::downgrade(&state),
            }));

        Ok(Self {
            state,
            route_client,
            inbound_task: Some(inbound_task),
            outbound_task: Some(outbound_task),
            change_subscription: Some(change_subscription),
            change_task: Some(change_task),
            retry_task: Some(retry_task),
            node_fetch_registration: Some(node_fetch_registration),
        })
    }

    pub async fn connect(db: Primadb, config: MoqRelayClientConfig) -> Result<Self> {
        Self::connect_with_config(db, config).await
    }

    pub fn is_connected(&self) -> bool {
        self.route_client.is_connected()
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

    pub fn publish_application(
        &self,
        message: ApplicationRouteMessage,
        target: RouteTarget,
    ) -> Result<RouteEnvelope> {
        publish_application_state(&self.state, message, target, None)
    }

    pub fn send_route_envelope(&self, route: RouteEnvelope) -> Result<RouteEnvelope> {
        send_route(&self.state, &route)?;
        Ok(route)
    }

    pub fn send_application(
        &self,
        namespace: impl Into<String>,
        protocol: impl Into<String>,
        topic: Option<String>,
        body: JsonValue,
        metadata: BTreeMap<String, JsonValue>,
        target: RouteTarget,
    ) -> Result<RouteEnvelope> {
        publish_application_state(
            &self.state,
            ApplicationRouteMessage::new(namespace, protocol, topic, body, metadata),
            target,
            None,
        )
    }

    pub fn subscribe_applications(
        &self,
        filter: ApplicationRouteFilter,
    ) -> ApplicationRouteSubscription {
        self.state.applications.subscribe(filter)
    }

    pub fn route_overlay_underlay(&self, id: impl Into<String>) -> RouteOverlayUnderlayHandle {
        let state = self.state.clone();
        let send_state = state.clone();
        let route_client = self.route_client.clone();
        let subscription = Arc::new(self.subscribe_applications(ApplicationRouteFilter::default()));
        RouteOverlayUnderlayHandle::new(
            RouteOverlayUnderlayInfo {
                id: id.into(),
                transport: RouteTransportKind::Moq,
                direct: false,
                relay_routed: true,
                connected: self.is_connected(),
                priority: 0,
                metadata: BTreeMap::new(),
            },
            move |route| send_route(&send_state, &route),
            Vec::new,
            move || route_client.is_connected(),
        )
        .with_application_events(move || subscription.drain())
    }

    pub fn watch_get(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Get { path },
        )
    }

    pub fn watch_get_with_policy(
        &self,
        path: RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(&self.state, policy, crate::PullRequestKind::Get { path })
    }

    pub async fn remote_get(
        &self,
        peer_id: impl Into<String>,
        path: RemotePath,
    ) -> Result<Option<JsonValue>> {
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

    pub async fn remote_get_with_policy(
        &self,
        path: RemotePath,
        policy: RemoteInterestPolicy,
    ) -> Result<Option<JsonValue>> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
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

    pub async fn remote_records(
        &self,
        peer_id: impl Into<String>,
        scan: crate::RecordScan,
    ) -> Result<RecordScanResult> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Records { scan },
        )
        .await?
        {
            RemoteResult::Records { result } => Ok(result),
            other => Err(PrimadbError::Message(format!(
                "expected records result, received {other:?}"
            ))),
        }
    }

    pub async fn remote_records_with_policy(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RecordScanResult> {
        match request_remote_result_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Records { scan },
        )
        .await?
        {
            RemoteResult::Records { result } => Ok(result),
            other => Err(PrimadbError::Message(format!(
                "expected records result, received {other:?}"
            ))),
        }
    }

    pub fn watch_records(
        &self,
        peer_id: impl Into<String>,
        scan: crate::RecordScan,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Records { scan },
        )
    }

    pub fn watch_records_with_policy(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteWatchSubscription> {
        start_remote_watch_with_policy(
            &self.state,
            policy,
            crate::PullRequestKind::Records { scan },
        )
    }

    pub async fn records_fan_in(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteRecordsFanIn> {
        records_fan_in_state(&self.state, scan, policy).await
    }

    pub fn watch_records_fan_in(
        &self,
        scan: crate::RecordScan,
        policy: RemoteInterestPolicy,
    ) -> Result<RemoteFanInWatch> {
        watch_records_fan_in_state(&self.state, scan, policy)
    }

    pub async fn remote_transaction(
        &self,
        peer_id: impl Into<String>,
        scope: impl Into<String>,
        steps: Vec<crate::TransactionStep>,
        options: crate::TransactionOptions,
    ) -> Result<crate::TransactionReport> {
        match request_remote_result(
            &self.state,
            peer_id.into(),
            crate::PullRequestKind::Transaction {
                scope: scope.into(),
                steps,
                options,
            },
        )
        .await?
        {
            RemoteResult::Transaction { report } => Ok(report),
            other => Err(PrimadbError::Message(format!(
                "expected transaction result, received {other:?}"
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
        if let Some(id) = self.node_fetch_registration.take() {
            self.state.db.unregister_node_fetch_scheduler(id);
        }
        self.state.closed.store(true, Ordering::SeqCst);
        self.state.connected.store(false, Ordering::SeqCst);
        let _ = self.state.outbound.send(NativeRouteOutbound::Close);
        self.route_client.shutdown();
        self.change_subscription.take();
        if let Some(task) = self.inbound_task.take() {
            task.abort();
        }
        if let Some(task) = self.outbound_task.take() {
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
        fail_outgoing_watches(&self.state, "connection closed");
        clear_incoming_watches(&self.state);
    }
}

#[cfg(feature = "native-moq")]
impl Drop for NativeMoqSync {
    fn drop(&mut self) {
        self.teardown();
    }
}

async fn request_remote_result_with_policy(
    state: &Arc<NativeWebSocketSyncState>,
    policy: RemoteInterestPolicy,
    request_kind: crate::PullRequestKind,
) -> Result<RemoteResult> {
    let capability = pull_capability_for_request(&request_kind);
    let peer_id = select_relay_peer_for_policy(state, &policy, capability, Some(&request_kind))?;
    request_remote_result(state, peer_id, request_kind).await
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

async fn records_fan_in_state(
    state: &Arc<NativeWebSocketSyncState>,
    scan: crate::RecordScan,
    policy: RemoteInterestPolicy,
) -> Result<RemoteRecordsFanIn> {
    let request_kind = crate::PullRequestKind::Records { scan: scan.clone() };
    let peers = resolve_relay_peers_for_policy(
        state,
        &policy,
        pull_capability_for_request(&request_kind),
        Some(&request_kind),
    )?;
    if peers.is_empty() {
        return Err(PrimadbError::Message(
            "remote interest policy did not select any peers".to_owned(),
        ));
    }

    let request_id = format!(
        "{}/records-fan-in/{:x}",
        state.db.replica_id(),
        state.next_message_seq.fetch_add(1, Ordering::SeqCst) + 1
    );
    let mut records = Vec::new();
    let mut failures = Vec::new();
    for peer in peers {
        let transport = route_transport_for_peer(&peer, &state.transport);
        match request_remote_result(
            state,
            peer.peer_id.clone(),
            crate::PullRequestKind::Records { scan: scan.clone() },
        )
        .await
        {
            Ok(RemoteResult::Records { result }) => records.push(RemotePeerRecords {
                peer_id: peer.peer_id,
                transport,
                result,
            }),
            Ok(other) => failures.push(RemotePeerFailure {
                peer_id: peer.peer_id,
                transport,
                message: format!("expected records result, received {other:?}"),
            }),
            Err(error) => failures.push(RemotePeerFailure {
                peer_id: peer.peer_id,
                transport,
                message: error.to_string(),
            }),
        }
    }

    Ok(merge_remote_records_fan_in(request_id, records, failures))
}

fn watch_records_fan_in_state(
    state: &Arc<NativeWebSocketSyncState>,
    scan: crate::RecordScan,
    policy: RemoteInterestPolicy,
) -> Result<RemoteFanInWatch> {
    let request_kind = crate::PullRequestKind::Records { scan: scan.clone() };
    let peers = resolve_relay_peers_for_policy(
        state,
        &policy,
        Some(&watch_capability_for_request(&request_kind)),
        Some(&request_kind),
    )?;
    if peers.is_empty() {
        return Err(PrimadbError::Message(
            "remote interest policy did not select any peers".to_owned(),
        ));
    }

    let (sender, receiver) = unbounded();
    let watches = Arc::new(Mutex::new(Vec::<RemoteWatchSubscription>::new()));
    let tasks = Arc::new(Mutex::new(Vec::<JoinHandle<()>>::new()));

    for peer in peers {
        let transport = route_transport_for_peer(&peer, &state.transport);
        match start_remote_watch(
            state,
            peer.peer_id.clone(),
            crate::PullRequestKind::Records { scan: scan.clone() },
        ) {
            Ok(watch) => {
                let child_receiver = watch.receiver();
                watches.lock().unwrap().push(watch);
                let child_sender = sender.clone();
                let peer_id = peer.peer_id.clone();
                let task = tokio::spawn(async move {
                    let mut sequence = 0_u64;
                    while let Ok(message) = child_receiver.recv().await {
                        match message {
                            Ok(message) => {
                                sequence = sequence.saturating_add(1);
                                let _ = child_sender
                                    .send(RemoteFanInWatchEvent::Update {
                                        peer_id: peer_id.clone(),
                                        transport: transport.clone(),
                                        initial: message.initial,
                                        sequence,
                                        result: message.result,
                                    })
                                    .await;
                            }
                            Err(message) => {
                                let _ = child_sender
                                    .send(RemoteFanInWatchEvent::Failure {
                                        peer_id: peer_id.clone(),
                                        transport: transport.clone(),
                                        message,
                                        terminal: true,
                                    })
                                    .await;
                                break;
                            }
                        }
                    }
                });
                tasks.lock().unwrap().push(task);
            }
            Err(error) => {
                let _ = sender.try_send(RemoteFanInWatchEvent::Failure {
                    peer_id: peer.peer_id,
                    transport,
                    message: error.to_string(),
                    terminal: true,
                });
            }
        }
    }

    let cancel_watches = watches.clone();
    let cancel_tasks = tasks.clone();
    Ok(RemoteFanInWatch::new(receiver, move || {
        for watch in cancel_watches.lock().unwrap().drain(..) {
            watch.close();
        }
        for task in cancel_tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }))
}

fn start_remote_watch_with_policy(
    state: &Arc<NativeWebSocketSyncState>,
    policy: RemoteInterestPolicy,
    request_kind: crate::PullRequestKind,
) -> Result<RemoteWatchSubscription> {
    let capability = Some(watch_capability_for_request(&request_kind));
    let peer_id =
        select_relay_peer_for_policy(state, &policy, capability.as_deref(), Some(&request_kind))?;
    start_remote_watch(state, peer_id, request_kind)
}

fn start_remote_watch(
    state: &Arc<NativeWebSocketSyncState>,
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
        let mut outgoing = state.outgoing_watches.lock().unwrap();
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

    let _ = send_watch_request(
        state,
        &target_peer_id,
        WatchRequest {
            watch_id: watch_id.clone(),
            request: WatchRequestKind::Subscribe {
                request: request_kind,
            },
        },
    );

    let cancel_state = state.clone();
    Ok(RemoteWatchSubscription::new(receiver, move || {
        cancel_remote_watch(&cancel_state, &watch_id);
    }))
}

fn select_relay_peer_for_policy(
    state: &Arc<NativeWebSocketSyncState>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&crate::PullRequestKind>,
) -> Result<String> {
    if let Some(peer) = resolve_relay_peers_for_policy(state, policy, capability, request)?
        .into_iter()
        .next()
    {
        return Ok(peer.peer_id);
    }
    Err(PrimadbError::Message(match capability {
        Some(capability) => format!("no connected peer advertises capability `{capability}`"),
        None => "no connected peer is available for remote interest".to_owned(),
    }))
}

fn resolve_relay_peers_for_policy(
    state: &Arc<NativeWebSocketSyncState>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&crate::PullRequestKind>,
) -> Result<Vec<PeerPresence>> {
    if let Some(peer_ids) = explicit_policy_peers(policy)? {
        if peer_ids.is_empty() {
            return Err(PrimadbError::Message(
                "remote interest policy did not include any peer ids".to_owned(),
            ));
        }
        let known = relay_peer_candidates(state);
        let mut peers = Vec::new();
        for peer_id in peer_ids {
            if let Some(peer) = known.iter().find(|peer| peer.peer_id == peer_id).cloned() {
                if !policy.require_capability || peer_supports_request(&peer, capability, request) {
                    peers.push(peer);
                }
            } else if !policy.require_capability {
                peers.push(PeerPresence {
                    peer_id,
                    replica_id: String::new(),
                    transport: state.transport.as_str().to_owned(),
                    identity: None,
                    capabilities: Vec::new(),
                    topics: Vec::new(),
                    metadata: BTreeMap::new(),
                });
            }
        }
        if policy.require_capability && peers.is_empty() {
            return Err(PrimadbError::Message(format!(
                "no requested peer advertises required capability `{}`",
                capability.unwrap_or("unknown")
            )));
        }
        return Ok(peers);
    }

    let mut candidates = relay_peer_candidates(state);
    prefer_vector_request_candidates(&mut candidates, request);
    if policy.require_capability {
        candidates.retain(|peer| peer_supports_request(peer, capability, request));
    } else if capability.is_some() {
        candidates.sort_by(|left, right| {
            peer_supports_request(right, capability, request)
                .cmp(&peer_supports_request(left, capability, request))
        });
    }
    Ok(candidates)
}

fn explicit_policy_peers(policy: &RemoteInterestPolicy) -> Result<Option<Vec<String>>> {
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

fn relay_peer_candidates(state: &Arc<NativeWebSocketSyncState>) -> Vec<crate::PeerPresence> {
    let mut peers = state
        .recommendations
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    peers.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.peer.peer_id.cmp(&right.peer.peer_id))
    });
    let mut candidates = Vec::new();
    for recommendation in peers {
        if !candidates
            .iter()
            .any(|peer: &crate::PeerPresence| peer.peer_id == recommendation.peer.peer_id)
        {
            candidates.push(recommendation.peer);
        }
    }
    for peer in state.router.known_peers() {
        if !candidates
            .iter()
            .any(|candidate| candidate.peer_id == peer.peer_id)
        {
            candidates.push(peer);
        }
    }
    if state.session_auth.require_authenticated_peers {
        candidates.retain(|peer| verified_identity_for_peer(state, &peer.peer_id).is_some());
    }
    candidates
}

fn route_transport_for_peer(
    peer: &PeerPresence,
    fallback: &RouteTransportKind,
) -> RouteTransportKind {
    match peer.transport.as_str() {
        "websocket" => RouteTransportKind::WebSocket,
        "moq" => RouteTransportKind::Moq,
        "webrtc" => RouteTransportKind::WebRtc,
        "broadcast_channel" => RouteTransportKind::BroadcastChannel,
        "in_memory" => RouteTransportKind::InMemory,
        _ => fallback.clone(),
    }
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

fn pull_capability_for_request(request: &crate::PullRequestKind) -> Option<&'static str> {
    match request {
        crate::PullRequestKind::Get { .. } => Some("pull_get"),
        crate::PullRequestKind::Query { .. } => Some("pull_query"),
        crate::PullRequestKind::Lex { .. } => Some("pull_lex"),
        crate::PullRequestKind::Records { .. } => Some("pull_records"),
        crate::PullRequestKind::VectorSearch { .. } => Some("pull_vector_search"),
        crate::PullRequestKind::Snapshot { .. } => Some("snapshot"),
        crate::PullRequestKind::Node { .. } => Some("pull_node"),
        crate::PullRequestKind::Map { .. } => Some("pull_map"),
        crate::PullRequestKind::Transaction { .. } => None,
    }
}

fn watch_capability_for_request(request: &crate::PullRequestKind) -> String {
    format!("watch_{}", request.kind_name())
}

fn cancel_remote_watch(state: &Arc<NativeWebSocketSyncState>, watch_id: &str) {
    let Some(watch) = state.outgoing_watches.lock().unwrap().remove(watch_id) else {
        return;
    };
    let _ = send_watch_request(
        state,
        &watch.target_peer_id,
        WatchRequest {
            watch_id: watch_id.to_owned(),
            request: WatchRequestKind::Cancel,
        },
    );
}

fn send_watch_request(
    state: &Arc<NativeWebSocketSyncState>,
    target_peer_id: &str,
    request: WatchRequest,
) -> Result<()> {
    let route = state.router.wrap_watch_request(
        request,
        RouteTarget::Peer(target_peer_id.to_owned()),
        None,
    );
    send_route(state, &route)
}

fn replay_outgoing_watches_for_peer(
    state: &Arc<NativeWebSocketSyncState>,
    peer_id: &str,
) -> Result<usize> {
    let requests = {
        let outgoing = state.outgoing_watches.lock().unwrap();
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
        send_watch_request(state, peer_id, request.clone())?;
    }
    Ok(requests.len())
}

async fn handle_incoming_text(state: &Arc<NativeWebSocketSyncState>, payload: &str) -> Result<()> {
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
    handle_route_payload(state, route).await
}

async fn handle_route_payload(
    state: &Arc<NativeWebSocketSyncState>,
    route: RouteEnvelope,
) -> Result<()> {
    let from = route.from.clone();
    let route_id = route.route_id.clone();
    let channel = route.channel.clone();
    let target = route.target.clone();
    let issued_at_millis = route.issued_at_millis;
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Application { message } => {
                let verified_identity = verified_identity_for_peer(state, &from);
                if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                    continue;
                }
                let mut context = ApplicationRouteContext::with_verified_identity(
                    from.clone(),
                    state.transport.clone(),
                    verified_identity.as_ref(),
                    state.session_auth.require_authenticated_peers,
                );
                context.relay_routed = true;
                state.applications.publish(ApplicationRouteEvent {
                    route_id: route_id.clone(),
                    from: from.clone(),
                    channel: channel.clone(),
                    target: target.clone(),
                    issued_at_millis,
                    received_at_millis: crate::clock::now_millis(),
                    transport: state.transport.clone(),
                    verified_identity,
                    context,
                    message,
                });
            }
            RoutePayload::Presence { peer } => {
                maybe_send_relay_auth_challenge(state, &peer)?;
                if state.session_auth.require_authenticated_peers
                    && verified_identity_for_peer(state, &peer.peer_id).is_none()
                {
                    continue;
                }
                let verified_identity = verified_identity_for_peer(state, &peer.peer_id);
                let _ = accept_relay_peer(state, peer, verified_identity.as_ref())?;
            }
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let verified_identity = verified_identity_for_peer(state, &from);
                if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                    continue;
                }
                match state.db.serve_pull_request_for_peer(
                    &from,
                    HookTransport::Relay,
                    &format!("snapshot:{route_id}"),
                    &crate::PullRequestKind::Snapshot { root: root.clone() },
                    verified_identity.as_ref(),
                )? {
                    crate::HookDecision::Allow {
                        value: crate::RemoteResult::Snapshot { snapshot },
                    } => {
                        let response = state.router.snapshot_response(
                            root,
                            snapshot,
                            RouteTarget::Peer(from.clone()),
                        );
                        send_route(state, &response)?;
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
                    && verified_identity_for_peer(state, &from).is_none()
                {
                    continue;
                }
                let frame = decode_sync_payload(&state.db, &encoding, payload)?;
                handle_sync_frame(state, frame, Some(from.clone())).await?;
            }
            RoutePayload::PullRequest { request } => {
                let verified_identity = verified_identity_for_peer(state, &from);
                let items = if state.session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    vec![RouteBatchItem::PullResponse {
                        response: error_pull_response(
                            &request.request_id,
                            "peer is not authenticated",
                        ),
                    }]
                } else {
                    match state.db.serve_pull_request_for_peer(
                        &from,
                        HookTransport::Relay,
                        &request.request_id,
                        &request.request,
                        verified_identity.as_ref(),
                    )? {
                        crate::HookDecision::Allow { value } => state
                            .db
                            .chunk_remote_result(&request.request_id, value)
                            .into_iter()
                            .map(|response| RouteBatchItem::PullResponse { response })
                            .collect::<Vec<_>>(),
                        crate::HookDecision::Deny { message } => {
                            vec![RouteBatchItem::PullResponse {
                                response: error_pull_response(&request.request_id, message),
                            }]
                        }
                    }
                };
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
            RoutePayload::WatchRequest { request } => {
                handle_watch_request(state, &from, request)?;
            }
            RoutePayload::WatchEvent { event } => {
                accept_watch_event(state, event)?;
            }
            RoutePayload::PeerExchange { peers } => {
                for recommendation in peers {
                    maybe_send_relay_auth_challenge(state, &recommendation.peer)?;
                    if state.session_auth.require_authenticated_peers
                        && verified_identity_for_peer(state, &recommendation.peer.peer_id).is_none()
                    {
                        continue;
                    }
                    let verified_identity =
                        verified_identity_for_peer(state, &recommendation.peer.peer_id);
                    let _ = accept_relay_recommendation(
                        state,
                        recommendation,
                        verified_identity.as_ref(),
                    )?;
                }
            }
            RoutePayload::AuthChallenge { challenge } => {
                handle_relay_auth_challenge(state, challenge)?;
            }
            RoutePayload::AuthResponse { response } => {
                handle_relay_auth_response(state, response)?;
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

fn publish_application_state(
    state: &Arc<NativeWebSocketSyncState>,
    message: ApplicationRouteMessage,
    target: RouteTarget,
    reply_to: Option<String>,
) -> Result<RouteEnvelope> {
    let route = state.router.wrap_application(message, target, reply_to);
    send_route(state, &route)?;
    Ok(route)
}

fn verified_identity_for_peer(
    state: &Arc<NativeWebSocketSyncState>,
    peer_id: &str,
) -> Option<VerifiedIdentity> {
    state
        .verified_identities
        .lock()
        .unwrap()
        .get(peer_id)
        .cloned()
}

fn remove_relay_peer_state(state: &Arc<NativeWebSocketSyncState>, peer_id: &str) {
    state.router.forget_peer(peer_id);
    state.recommendations.lock().unwrap().remove(peer_id);
    state.verified_identities.lock().unwrap().remove(peer_id);
}

fn accept_relay_peer(
    state: &Arc<NativeWebSocketSyncState>,
    peer: crate::PeerPresence,
    verified_identity: Option<&VerifiedIdentity>,
) -> Result<bool> {
    let relay_url = peer.metadata.get("relay_url").cloned();
    let connect_allowed = state
        .db
        .allow_peer_connection(&crate::ConnectHookContext {
            peer: peer.clone(),
            transport: HookTransport::Relay,
            relay_url,
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if connect_allowed.is_err() {
        remove_relay_peer_state(state, &peer.peer_id);
        return Ok(false);
    }
    let recommendation = peer_recommendation_from_presence(&peer);
    let peer_id = recommendation.peer.peer_id.clone();
    store_peer_recommendations(state, vec![recommendation]);
    let _ = replay_outgoing_watches_for_peer(state, &peer_id);
    Ok(true)
}

fn accept_relay_recommendation(
    state: &Arc<NativeWebSocketSyncState>,
    recommendation: PeerRecommendation,
    verified_identity: Option<&VerifiedIdentity>,
) -> Result<bool> {
    let relay_url = recommendation.relay_urls.first().cloned();
    let connect_allowed = state
        .db
        .allow_peer_connection(&crate::ConnectHookContext {
            peer: recommendation.peer.clone(),
            transport: HookTransport::Relay,
            relay_url,
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if connect_allowed.is_err() {
        remove_relay_peer_state(state, &recommendation.peer.peer_id);
        return Ok(false);
    }
    let peer_id = recommendation.peer.peer_id.clone();
    store_peer_recommendations(state, vec![recommendation]);
    let _ = replay_outgoing_watches_for_peer(state, &peer_id);
    Ok(true)
}

fn maybe_send_relay_auth_challenge(
    state: &Arc<NativeWebSocketSyncState>,
    peer: &crate::PeerPresence,
) -> Result<()> {
    if peer.peer_id == state.router.peer_id()
        || verified_identity_for_peer(state, &peer.peer_id).is_some()
    {
        return Ok(());
    }
    let Some(identity) = peer.identity.as_ref() else {
        if !state.session_auth.allow_unauthenticated_presence {
            remove_relay_peer_state(state, &peer.peer_id);
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
            "relay",
            &state.session_auth,
        );
        let route = state
            .router
            .auth_challenge(challenge.clone(), RouteTarget::Peer(peer.peer_id.clone()));
        send_route(state, &route)?;
        state
            .pending_auth_challenges
            .lock()
            .unwrap()
            .insert(challenge.challenge_id.clone(), challenge.clone());
        state
            .pending_auth_peers
            .lock()
            .unwrap()
            .insert(challenge.challenge_id.clone(), peer.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = identity;
    }

    Ok(())
}

fn handle_relay_auth_challenge(
    state: &Arc<NativeWebSocketSyncState>,
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
    send_route(state, &route)
}

fn handle_relay_auth_response(
    state: &Arc<NativeWebSocketSyncState>,
    response: crate::AuthResponse,
) -> Result<()> {
    let Some(challenge) = state
        .pending_auth_challenges
        .lock()
        .unwrap()
        .remove(&response.challenge_id)
    else {
        return Ok(());
    };
    let peer = state
        .pending_auth_peers
        .lock()
        .unwrap()
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
        .unwrap()
        .insert(verified.peer_id.clone(), verified.clone());
    if !accept_relay_peer(state, peer, Some(&verified))? {
        state
            .verified_identities
            .lock()
            .unwrap()
            .remove(&verified.peer_id);
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
            if state.session_auth.require_authenticated_peers {
                let authenticated = reply_peer
                    .as_ref()
                    .and_then(|peer_id| verified_identity_for_peer(state, peer_id))
                    .is_some();
                if !authenticated {
                    return Ok(());
                }
            }
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
    let route = state
        .router
        .wrap_sync(encoding, payload, RouteTarget::Broadcast);

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
        let route = state.router.wrap_sync(
            item.encoding.clone(),
            item.payload.clone(),
            item.target.clone(),
        );
        send_route(state, &route)?;
    }

    Ok(outbound.len())
}

fn build_relay_presence_route(
    state: &Arc<NativeWebSocketSyncState>,
    url: &str,
    transport: &str,
) -> RouteEnvelope {
    let mut capabilities = vec![
        "sync".to_owned(),
        "ack".to_owned(),
        "routing".to_owned(),
        "snapshot".to_owned(),
        "batch".to_owned(),
        "pull_get".to_owned(),
        "pull_map".to_owned(),
        "pull_query".to_owned(),
        "pull_lex".to_owned(),
        "pull_records".to_owned(),
        "pull_vector_search".to_owned(),
        "pull_node".to_owned(),
        "watch_get".to_owned(),
        "watch_map".to_owned(),
        "watch_query".to_owned(),
        "watch_lex".to_owned(),
        "watch_records".to_owned(),
        "watch_vector_search".to_owned(),
        "watch_node".to_owned(),
        "watch_snapshot".to_owned(),
        "peer_exchange".to_owned(),
    ];
    capabilities.extend(state.db.vector_presence_capabilities());
    let mut presence = state.router.presence(
        state.db.replica_id(),
        transport,
        capabilities,
        vec!["primadb-sync".to_owned()],
    );
    if let RoutePayload::Presence { peer } = &mut presence.payload {
        peer.metadata.insert("relay_url".to_owned(), url.to_owned());
        peer.identity = state.db.session_presence_identity(&state.session_id);
    }
    presence
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
    let payload = serde_json::to_string(route)?;
    if payload.len() > state.db.limits().max_route_payload_bytes {
        return Err(PrimadbError::Message(format!(
            "route payload exceeds {} bytes",
            state.db.limits().max_route_payload_bytes
        )));
    }
    if !state.connected.load(Ordering::SeqCst) {
        return Err(PrimadbError::Message(
            "native websocket is not connected".to_owned(),
        ));
    }
    state
        .outbound
        .send(NativeRouteOutbound::Route(route.clone()))
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

fn accept_pull_response(
    state: &Arc<NativeWebSocketSyncState>,
    response: crate::PullResponse,
) -> Result<()> {
    let mut pending = state.pending_requests.lock().unwrap();
    let Some(request) = pending.get_mut(&response.request_id) else {
        return Ok(());
    };

    if let Some(message) = apply_response_body(&mut request.accumulator, &response.result) {
        let request = pending.remove(&response.request_id).unwrap();
        let _ = request.sender.try_send(Err(message));
        return Ok(());
    }

    if response.is_final() {
        let request = pending.remove(&response.request_id).unwrap();
        let result = request.accumulator.into_result()?;
        let _ = request.sender.try_send(Ok(result));
    }
    Ok(())
}

fn handle_watch_request(
    state: &Arc<NativeWebSocketSyncState>,
    from: &str,
    request: WatchRequest,
) -> Result<()> {
    match request.request {
        WatchRequestKind::Subscribe {
            request: incoming_request_kind,
        } => {
            let verified_identity = verified_identity_for_peer(state, from);
            if state.session_auth.require_authenticated_peers && verified_identity.is_none() {
                let route = state.router.wrap_watch_event(
                    error_watch_event(&request.watch_id, 0, true, "peer is not authenticated"),
                    RouteTarget::Peer(from.to_owned()),
                    None,
                );
                send_route(state, &route)?;
                return Ok(());
            }
            let request_kind = match state
                .db
                .authorize_watch_request_for_peer(
                    from,
                    HookTransport::Relay,
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
                        RouteTarget::Peer(from.to_owned()),
                        None,
                    );
                    send_route(state, &route)?;
                    return Ok(());
                }
            };
            let limit = state.db.limits().max_active_remote_watches.max(1);
            {
                let mut watches = state.incoming_watches.lock().unwrap();
                if watches.len() >= limit && !watches.contains_key(&request.watch_id) {
                    return Err(PrimadbError::TooManyRemoteWatches { limit });
                }
                let interest_path = request_kind.interest_path();
                watches.insert(
                    request.watch_id.clone(),
                    IncomingWatch {
                        target_peer_id: from.to_owned(),
                        request_kind,
                        interest_path,
                        next_sequence: 0,
                        last_hash: None,
                    },
                );
            }
            let _ = emit_single_incoming_watch_update(state, &request.watch_id, true)?;
        }
        WatchRequestKind::Cancel => {
            state
                .incoming_watches
                .lock()
                .unwrap()
                .remove(&request.watch_id);
        }
    }
    Ok(())
}

fn accept_watch_event(state: &Arc<NativeWebSocketSyncState>, event: WatchEvent) -> Result<()> {
    let mut deliver: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        RemoteWatchMessage,
    )> = None;
    let mut failure: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        String,
    )> = None;

    {
        let mut watches = state.outgoing_watches.lock().unwrap();
        let Some(watch) = watches.get_mut(&event.watch_id) else {
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
            watches.remove(&event.watch_id);
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

async fn emit_incoming_watch_updates(
    state: &Arc<NativeWebSocketSyncState>,
    event: &crate::ChangeEvent,
) -> Result<usize> {
    let watch_ids = {
        let watches = state.incoming_watches.lock().unwrap();
        watches
            .iter()
            .filter_map(|(watch_id, watch)| {
                incoming_watch_overlaps_event(watch, event).then_some(watch_id.clone())
            })
            .collect::<Vec<_>>()
    };
    let mut emitted = 0;
    for watch_id in watch_ids {
        if emit_single_incoming_watch_update(state, &watch_id, false)? {
            emitted += 1;
        }
    }
    Ok(emitted)
}

fn emit_single_incoming_watch_update(
    state: &Arc<NativeWebSocketSyncState>,
    watch_id: &str,
    initial: bool,
) -> Result<bool> {
    let Some(watch) = state
        .incoming_watches
        .lock()
        .unwrap()
        .get(watch_id)
        .cloned()
    else {
        return Ok(false);
    };

    let verified_identity = verified_identity_for_peer(state, &watch.target_peer_id);
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
        send_route(state, &route)?;
        state.incoming_watches.lock().unwrap().remove(watch_id);
        return Ok(true);
    }

    let decision = state.db.serve_watch_result_for_peer(
        &watch.target_peer_id,
        HookTransport::Relay,
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
        send_route(state, &route)?;
        state.incoming_watches.lock().unwrap().remove(watch_id);
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
        send_route(state, &route)?;
    }

    if let Some(entry) = state.incoming_watches.lock().unwrap().get_mut(watch_id) {
        entry.last_hash = content_hash;
        entry.next_sequence = entry.next_sequence.saturating_add(1);
    }
    Ok(true)
}

fn fail_outgoing_watches(state: &Arc<NativeWebSocketSyncState>, message: &str) {
    let watches = {
        let mut watches = state.outgoing_watches.lock().unwrap();
        std::mem::take(&mut *watches)
    };
    for watch in watches.into_values() {
        let _ = watch.sender.try_send(Err(message.to_owned()));
    }
}

fn clear_incoming_watches(state: &Arc<NativeWebSocketSyncState>) {
    state.incoming_watches.lock().unwrap().clear();
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
        score: 100
            + peer.capabilities.len().min(8) as u16 * 5
            + peer.topics.len().min(8) as u16 * 5,
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
    items
        .chunks(batch_size.max(1))
        .map(|chunk| {
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

impl NodeFetchScheduler for NativeWebSocketNodeFetchScheduler {
    fn fetch_nodes(&self, nodes: Vec<String>) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        tokio::spawn(async move {
            let peers = state
                .router
                .known_peers()
                .into_iter()
                .map(|peer| peer.peer_id)
                .collect::<Vec<_>>();
            for node_id in nodes {
                let mut fetched = false;
                for peer_id in &peers {
                    match request_remote_result(
                        &state,
                        peer_id.clone(),
                        crate::PullRequestKind::Node {
                            id: node_id.clone(),
                        },
                    )
                    .await
                    {
                        Ok(RemoteResult::Node { node: Some(node) }) => {
                            let _ = state.db.apply_node_state(node);
                            fetched = true;
                            break;
                        }
                        Ok(RemoteResult::Node { node: None }) | Err(_) => {}
                        Ok(_) => {}
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

    fn into_result(self) -> Result<RemoteResult> {
        match self {
            Self::Get { value } => Ok(RemoteResult::Get { value }),
            Self::Map { entries } => Ok(RemoteResult::Map { entries }),
            Self::Query { entries } => Ok(RemoteResult::Query { entries }),
            Self::Lex { entries } => Ok(RemoteResult::Lex { entries }),
            Self::Records {
                entries,
                next_cursor,
            } => Ok(RemoteResult::Records {
                result: RecordScanResult {
                    entries,
                    next_cursor,
                },
            }),
            Self::VectorSearch { result } => Ok(RemoteResult::VectorSearch {
                result: result.ok_or_else(|| {
                    PrimadbError::Message(
                        "vector search response completed without a result".to_owned(),
                    )
                })?,
            }),
            Self::Node { node } => Ok(RemoteResult::Node { node }),
            Self::Snapshot {
                clock,
                nodes,
                pending_ops,
                scope_policies,
            } => Ok(RemoteResult::Snapshot {
                snapshot: crate::DatabaseSnapshot {
                    clock: clock.ok_or_else(|| {
                        PrimadbError::Message(
                            "snapshot response completed without a clock".to_owned(),
                        )
                    })?,
                    nodes,
                    pending_ops,
                    scope_policies,
                    provisional_transactions: Default::default(),
                    next_provisional_transaction_id: 0,
                },
            }),
            Self::Transaction { report } => Ok(RemoteResult::Transaction {
                report: report.ok_or_else(|| {
                    PrimadbError::Message(
                        "transaction response completed without a report".to_owned(),
                    )
                })?,
            }),
        }
    }
}
