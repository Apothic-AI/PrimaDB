#[cfg(feature = "crypto")]
use crate::SecureSyncFrame;
use crate::app_route::ApplicationRouteBus;
use crate::wasm_opfs::{
    apply_segment_transaction_opfs, estimate_segment_namespace_opfs, load_segment_snapshot_opfs,
    load_vector_cache_opfs, replace_segment_transaction_opfs, write_vector_cache_opfs,
};
use crate::{
    ApplicationRouteEvent, ApplicationRouteFilter, ApplicationRouteMessage,
    ApplicationRouteSubscription as CoreApplicationRouteSubscription, BlobRef, BlobStorageBinding,
    BlobStorageConfig, Chain, ChangeSubscription, ConnectHookContext, DurableStorageBinding,
    DurableStorageConfig, HookTransport, HybridClock, IceServerConfig, LexEntry, LexSpec, MapEntry,
    MeshConfig, MeshSignal, MeshSignalingMode, NodeFetchScheduler, Operation, PeerRecommendation,
    Primadb, PullRequest, PullRequestKind, PullResponse, PullResponseBody, QuerySpec, RecordBatch,
    RecordEntry, RecordScan, RecordScanResult,
    RecordWatchSubscription as CoreRecordWatchSubscription, RelayClientConfig, RelayEndpointConfig,
    RemoteFanInWatchEvent, RemoteInterestPolicy, RemoteInterestTarget, RemotePath,
    RemotePeerFailure, RemotePeerRecords, RemoteResult, RemoteWatchMessage, RoomHookContext,
    RouteBatchItem, RouteEnvelope, RoutePayload, RouteTarget, RouteTransportKind, Router,
    RouterConfig, Scope, ScopePolicy, ServeRequestContext, ServeResultContext, Subscription,
    SyncEnvelope, SyncFrame, TransactionOptions, TransactionStep, TraversalSpec,
    TraversalSubscription as CoreTraversalSubscription, VectorCollectionConfig, VectorSearchSpec,
    VectorWatchSubscription as CoreVectorWatchSubscription, VerifiedIdentity, WatchEvent,
    WatchRequest, WatchRequestKind, encode_component, error_pull_response, error_watch_event,
    merge_remote_records_fan_in,
};
#[cfg(feature = "scripting")]
use crate::{NodeScript, ScriptExecutionOptions};
use async_channel::{Sender, bounded, unbounded};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};

#[cfg(feature = "wasm-threads")]
#[allow(unused_imports)]
pub use wasm_bindgen_rayon::init_thread_pool;

#[wasm_bindgen(js_name = parallelEnabled)]
pub fn parallel_enabled_js() -> bool {
    crate::parallel_enabled()
}

#[wasm_bindgen(js_name = parallelThreadCount)]
pub fn parallel_thread_count_js() -> usize {
    crate::parallel_thread_count()
}

#[wasm_bindgen(js_name = Primadb)]
pub struct WasmPrimadb {
    inner: Primadb,
    durable_storage_hooks: Rc<RefCell<Vec<WasmDurableStorageHook>>>,
    blob_storage: Rc<RefCell<Option<WasmBlobStorageConfig>>>,
}

#[wasm_bindgen(js_name = Chain)]
pub struct WasmChain {
    inner: Chain,
    blob_storage: Rc<RefCell<Option<WasmBlobStorageConfig>>>,
}

#[wasm_bindgen(js_name = Scope)]
pub struct WasmScope {
    inner: Scope,
}

#[wasm_bindgen(js_name = Subscription)]
pub struct WasmSubscription {
    inner: Option<Subscription>,
}

#[wasm_bindgen(js_name = TraversalSubscription)]
pub struct WasmTraversalSubscription {
    inner: Option<CoreTraversalSubscription>,
}

#[wasm_bindgen(js_name = RecordWatchSubscription)]
pub struct WasmRecordWatchSubscription {
    inner: Option<CoreRecordWatchSubscription>,
}

#[wasm_bindgen(js_name = VectorWatchSubscription)]
pub struct WasmVectorWatchSubscription {
    inner: Option<CoreVectorWatchSubscription>,
}

#[wasm_bindgen(js_name = RemoteWatch)]
pub struct WasmRemoteWatch {
    inner: Option<WasmRemoteWatchInner>,
}

struct WasmRemoteWatchInner {
    receiver: async_channel::Receiver<std::result::Result<RemoteWatchMessage, String>>,
    cancel: Box<dyn Fn()>,
}

#[wasm_bindgen(js_name = ApplicationRouteSubscription)]
pub struct WasmApplicationRouteSubscription {
    inner: Option<CoreApplicationRouteSubscription>,
}

#[wasm_bindgen(js_name = RemoteFanInWatch)]
pub struct WasmRemoteFanInWatch {
    inner: Option<WasmRemoteFanInWatchInner>,
}

struct WasmRemoteFanInWatchInner {
    receiver: async_channel::Receiver<RemoteFanInWatchEvent>,
    close: Box<dyn Fn()>,
}

#[wasm_bindgen(js_name = IndexedDbPersistence)]
pub struct WasmIndexedDbPersistence {
    db: Primadb,
    database_name: String,
    store_name: String,
    key: String,
    subscription: Option<ChangeSubscription>,
}

#[wasm_bindgen(js_name = IndexedDbSegmentPersistence)]
pub struct WasmIndexedDbSegmentPersistence {
    db: Primadb,
    database_name: String,
    store_name: String,
    namespace: String,
    stats: Rc<RefCell<WasmSegmentPersistenceStats>>,
    external_hook_registered: bool,
    subscription: Option<ChangeSubscription>,
}

#[wasm_bindgen(js_name = OpfsSegmentPersistence)]
pub struct WasmOpfsSegmentPersistence {
    db: Primadb,
    directory: String,
    namespace: String,
    stats: Rc<RefCell<WasmSegmentPersistenceStats>>,
    external_hook_registered: bool,
    subscription: Option<ChangeSubscription>,
}

#[wasm_bindgen(js_name = IndexedDbBlobStorage)]
pub struct WasmIndexedDbBlobStorage {
    config: WasmBlobStorageConfig,
}

#[allow(dead_code)]
enum WasmDurableStorageHook {
    Snapshot {
        _hook: WasmIndexedDbPersistence,
    },
    Segment {
        _hook: WasmIndexedDbSegmentPersistence,
    },
    OpfsSegment {
        _hook: WasmOpfsSegmentPersistence,
    },
}

#[derive(Clone)]
struct WasmBlobStorageConfig {
    database_name: String,
    store_name: String,
    namespace: String,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WasmSegmentPersistenceStats {
    queued_events: u64,
    coalesced_events: u64,
    successful_writes: u64,
    failed_writes: u64,
    full_replacements: u64,
    incremental_transactions: u64,
    entries_written: u64,
    entries_deleted: u64,
    estimated_bytes_written: u64,
    last_entries_written: u64,
    last_entries_deleted: u64,
    last_estimated_bytes_written: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WasmSegmentStorageEstimate {
    pub key_count: u64,
    pub estimated_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_usage: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_quota: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct WasmSegmentWriteSummary {
    pub entries_written: u64,
    pub entries_deleted: u64,
    pub estimated_bytes_written: u64,
}

#[derive(Debug, Clone, Copy)]
enum SegmentWriteKind {
    FullReplacement,
    Incremental,
}

fn record_segment_write_success(
    stats: &Rc<RefCell<WasmSegmentPersistenceStats>>,
    summary: WasmSegmentWriteSummary,
    kind: SegmentWriteKind,
) {
    let mut stats = stats.borrow_mut();
    stats.successful_writes = stats.successful_writes.saturating_add(1);
    match kind {
        SegmentWriteKind::FullReplacement => {
            stats.full_replacements = stats.full_replacements.saturating_add(1);
        }
        SegmentWriteKind::Incremental => {
            stats.incremental_transactions = stats.incremental_transactions.saturating_add(1);
        }
    }
    stats.entries_written = stats
        .entries_written
        .saturating_add(summary.entries_written);
    stats.entries_deleted = stats
        .entries_deleted
        .saturating_add(summary.entries_deleted);
    stats.estimated_bytes_written = stats
        .estimated_bytes_written
        .saturating_add(summary.estimated_bytes_written);
    stats.last_entries_written = summary.entries_written;
    stats.last_entries_deleted = summary.entries_deleted;
    stats.last_estimated_bytes_written = summary.estimated_bytes_written;
    stats.last_error = None;
}

fn record_segment_write_error(stats: &Rc<RefCell<WasmSegmentPersistenceStats>>, error: String) {
    let mut stats = stats.borrow_mut();
    stats.failed_writes = stats.failed_writes.saturating_add(1);
    stats.last_error = Some(error);
}

#[derive(Debug)]
struct WebSocketSyncState {
    db: Primadb,
    router: Router,
    session_id: String,
    session_auth: crate::SessionAuthConfig,
    socket: web_sys::WebSocket,
    inflight: BTreeMap<String, OutboundSync>,
    pending_requests: BTreeMap<String, PendingPullRequest>,
    outgoing_watches: BTreeMap<String, OutgoingWatch>,
    incoming_watches: BTreeMap<String, IncomingWatch>,
    recommendations: BTreeMap<String, PeerRecommendation>,
    pending_auth_challenges: BTreeMap<String, crate::AuthChallenge>,
    pending_auth_peers: BTreeMap<String, crate::PeerPresence>,
    verified_identities: BTreeMap<String, VerifiedIdentity>,
    applications: ApplicationRouteBus,
    next_message_seq: u64,
}

struct WasmWebSocketNodeFetchScheduler {
    state: std::rc::Weak<RefCell<WebSocketSyncState>>,
}

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
struct OutgoingWatch {
    sender: Sender<std::result::Result<RemoteWatchMessage, String>>,
    target_peer_id: String,
    request_kind: PullRequestKind,
    pending_sequence: Option<PendingWatchSequence>,
    last_delivered_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
struct PendingWatchSequence {
    sequence: u64,
    initial: bool,
    accumulator: PullAccumulator,
}

#[derive(Debug, Clone)]
struct IncomingWatch {
    target_peer_id: String,
    request_kind: PullRequestKind,
    interest_path: Option<String>,
    next_sequence: u64,
    last_hash: Option<String>,
}

#[derive(Debug, Clone)]
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

#[wasm_bindgen(js_name = WebSocketSync)]
pub struct WasmWebSocketSync {
    state: Rc<RefCell<WebSocketSyncState>>,
    change_subscription: Option<ChangeSubscription>,
    onmessage: Option<Closure<dyn FnMut(web_sys::MessageEvent)>>,
    onopen: Option<Closure<dyn FnMut(web_sys::Event)>>,
    onclose: Option<Closure<dyn FnMut(web_sys::CloseEvent)>>,
    onerror: Option<Closure<dyn FnMut(web_sys::Event)>>,
    interval_callback: Option<Closure<dyn FnMut()>>,
    interval_id: Option<i32>,
    node_fetch_registration: Option<u64>,
}

#[derive(Debug, Clone)]
struct MeshOutbound {
    encoding: String,
    payload: JsonValue,
    awaiting: BTreeMap<String, bool>,
}

struct MeshPeer {
    connection: web_sys::RtcPeerConnection,
    channel: Option<web_sys::RtcDataChannel>,
    created_at_millis: u64,
    #[allow(dead_code)]
    onicecandidate: Option<Closure<dyn FnMut(web_sys::RtcPeerConnectionIceEvent)>>,
    #[allow(dead_code)]
    ondatachannel: Option<Closure<dyn FnMut(web_sys::RtcDataChannelEvent)>>,
    onmessage: Option<Closure<dyn FnMut(web_sys::MessageEvent)>>,
    onopen: Option<Closure<dyn FnMut(web_sys::Event)>>,
    onclose: Option<Closure<dyn FnMut(web_sys::Event)>>,
}

const STALE_MESH_PEER_MILLIS: u64 = 5_000;

#[derive(Clone)]
enum MeshSignalingTransport {
    BroadcastChannel(web_sys::BroadcastChannel),
    Relay {
        socket: web_sys::WebSocket,
        relay_url: String,
    },
    External {
        send_route: js_sys::Function,
        relay_url: Option<String>,
        mode: String,
    },
}

struct WebRtcMeshState {
    db: Primadb,
    router: Router,
    room: String,
    peer_id: String,
    session_id: String,
    session_auth: crate::SessionAuthConfig,
    signaling: MeshSignalingTransport,
    rtc_configuration: JsValue,
    peers: BTreeMap<String, MeshPeer>,
    inflight: BTreeMap<String, MeshOutbound>,
    outgoing_watches: BTreeMap<String, OutgoingWatch>,
    incoming_watches: BTreeMap<String, IncomingWatch>,
    recommendations: BTreeMap<String, PeerRecommendation>,
    pending_auth_challenges: BTreeMap<String, crate::AuthChallenge>,
    pending_auth_peers: BTreeMap<String, crate::PeerPresence>,
    verified_identities: BTreeMap<String, VerifiedIdentity>,
    applications: ApplicationRouteBus,
    next_message_seq: u64,
    relay_onmessage: Option<Closure<dyn FnMut(web_sys::MessageEvent)>>,
    relay_onopen: Option<Closure<dyn FnMut(web_sys::Event)>>,
    relay_onclose: Option<Closure<dyn FnMut(web_sys::CloseEvent)>>,
    relay_onerror: Option<Closure<dyn FnMut(web_sys::Event)>>,
}

struct WasmWebRtcNodeFetchScheduler {
    state: std::rc::Weak<RefCell<WebRtcMeshState>>,
}

#[derive(Clone)]
struct WasmNetworkHookCallbacks {
    on_connect: Option<js_sys::Function>,
    on_join_room: Option<js_sys::Function>,
    on_pull: Option<js_sys::Function>,
    on_watch: Option<js_sys::Function>,
    on_serve_result: Option<js_sys::Function>,
}

struct WasmNetworkHooks {
    callbacks: WasmNetworkHookCallbacks,
}

unsafe impl Send for WasmNetworkHooks {}
unsafe impl Sync for WasmNetworkHooks {}

impl NodeFetchScheduler for WasmWebSocketNodeFetchScheduler {
    fn fetch_nodes(&self, nodes: Vec<String>) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        spawn_local(async move {
            let peer_ids = state
                .borrow()
                .router
                .known_peers()
                .into_iter()
                .map(|peer| peer.peer_id)
                .collect::<Vec<_>>();
            for node_id in nodes {
                let mut fetched = false;
                for peer_id in &peer_ids {
                    match request_remote_result_state(
                        &state,
                        peer_id.clone(),
                        PullRequestKind::Node {
                            id: node_id.clone(),
                        },
                    )
                    .await
                    {
                        Ok(RemoteResult::Node { node: Some(node) }) => {
                            let db = state.borrow().db.clone();
                            let _ = db.apply_node_state(node);
                            fetched = true;
                            break;
                        }
                        Ok(RemoteResult::Node { node: None }) | Err(_) => {}
                        Ok(_) => {}
                    }
                }
                if !fetched {
                    let db = state.borrow().db.clone();
                    db.clear_scheduled_node_fetch(&node_id);
                }
            }
        });
    }
}

impl NodeFetchScheduler for WasmWebRtcNodeFetchScheduler {
    fn fetch_nodes(&self, nodes: Vec<String>) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        spawn_local(async move {
            let peer_ids = state
                .borrow()
                .peers
                .iter()
                .filter_map(|(peer_id, peer)| {
                    peer.channel
                        .as_ref()
                        .is_some_and(mesh_channel_is_open)
                        .then_some(peer_id.clone())
                })
                .collect::<Vec<_>>();
            for node_id in nodes {
                let mut fetched = false;
                for peer_id in &peer_ids {
                    let mut watch = match start_mesh_remote_watch_state(
                        &state,
                        peer_id.clone(),
                        PullRequestKind::Node {
                            id: node_id.clone(),
                        },
                    ) {
                        Ok(watch) => watch,
                        Err(_) => continue,
                    };
                    let receiver = watch.inner.as_ref().map(|inner| inner.receiver.clone());
                    let message = match receiver {
                        Some(receiver) => receiver.recv().await.ok(),
                        None => None,
                    };
                    watch.cancel();
                    if let Some(Ok(RemoteWatchMessage {
                        result: RemoteResult::Node { node: Some(node) },
                        ..
                    })) = message
                    {
                        let db = state.borrow().db.clone();
                        let _ = db.apply_node_state(node);
                        fetched = true;
                        break;
                    }
                }
                if !fetched {
                    let db = state.borrow().db.clone();
                    db.clear_scheduled_node_fetch(&node_id);
                }
            }
        });
    }
}

#[wasm_bindgen(js_name = WebRtcMesh)]
pub struct WasmWebRtcMesh {
    state: Rc<RefCell<WebRtcMeshState>>,
    change_subscription: Option<ChangeSubscription>,
    signaling_onmessage: Option<Closure<dyn FnMut(web_sys::MessageEvent)>>,
    retry_callback: Option<Closure<dyn FnMut()>>,
    retry_interval_id: Option<i32>,
    node_fetch_registration: Option<u64>,
}

impl crate::NetworkHooks for WasmNetworkHooks {
    fn on_connect(&self, context: &ConnectHookContext) -> crate::HookDecision<()> {
        match &self.callbacks.on_connect {
            Some(function) => parse_void_hook_decision(
                call_hook1(function, context),
                "connection denied by network hook",
            ),
            None => crate::HookDecision::allow(()),
        }
    }

    fn on_join_room(&self, context: &RoomHookContext) -> crate::HookDecision<()> {
        match &self.callbacks.on_join_room {
            Some(function) => parse_void_hook_decision(
                call_hook1(function, context),
                "room denied by network hook",
            ),
            None => crate::HookDecision::allow(()),
        }
    }

    fn on_pull(&self, context: &ServeRequestContext) -> crate::HookDecision<PullRequestKind> {
        match &self.callbacks.on_pull {
            Some(function) => parse_request_hook_decision(
                call_hook1(function, context),
                &context.request,
                "pull denied by network hook",
            ),
            None => crate::HookDecision::allow(context.request.clone()),
        }
    }

    fn on_watch(&self, context: &ServeRequestContext) -> crate::HookDecision<PullRequestKind> {
        match &self.callbacks.on_watch {
            Some(function) => parse_request_hook_decision(
                call_hook1(function, context),
                &context.request,
                "watch denied by network hook",
            ),
            None => crate::HookDecision::allow(context.request.clone()),
        }
    }

    fn on_serve_result(
        &self,
        context: &ServeResultContext,
        result: RemoteResult,
    ) -> crate::HookDecision<RemoteResult> {
        match &self.callbacks.on_serve_result {
            Some(function) => parse_result_hook_decision(
                call_hook2(function, context, &result),
                result,
                "served result denied by network hook",
            ),
            None => crate::HookDecision::allow(result),
        }
    }
}

#[wasm_bindgen(js_class = Primadb)]
impl WasmPrimadb {
    #[wasm_bindgen(constructor)]
    pub fn new(replica_id: Option<String>) -> Self {
        console_error_panic_hook::set_once();
        let inner = replica_id.map(Primadb::with_replica_id).unwrap_or_default();
        Self {
            inner,
            durable_storage_hooks: Rc::new(RefCell::new(Vec::new())),
            blob_storage: Rc::new(RefCell::new(None)),
        }
    }

    #[wasm_bindgen(js_name = replicaId)]
    pub fn replica_id(&self) -> String {
        self.inner.replica_id()
    }

    pub fn chain(&self, root: String) -> WasmChain {
        WasmChain {
            inner: self.inner.root(root),
            blob_storage: self.blob_storage.clone(),
        }
    }

    pub fn scope(&self, root: String) -> WasmScope {
        WasmScope {
            inner: self.inner.scope(root),
        }
    }

    pub fn transaction(&self, steps: JsValue) -> std::result::Result<JsValue, JsValue> {
        let steps: Vec<TransactionStep> = serde_wasm_bindgen::from_value(steps)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        to_js(
            &self
                .inner
                .apply_transaction_steps(steps)
                .map_err(to_js_error)?,
        )
    }

    #[cfg(feature = "scripting")]
    #[wasm_bindgen(js_name = attachNodeScript)]
    pub fn attach_node_script(
        &self,
        path: JsValue,
        script: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let script: NodeScript = serde_wasm_bindgen::from_value(script)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner
            .attach_node_script(path, script)
            .map_err(to_js_error)
    }

    #[cfg(feature = "scripting")]
    #[wasm_bindgen(js_name = removeNodeScript)]
    pub fn remove_node_script(
        &self,
        path: JsValue,
        script_id: String,
    ) -> std::result::Result<(), JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner
            .remove_node_script(&path, &script_id)
            .map_err(to_js_error)
    }

    #[cfg(feature = "scripting")]
    #[wasm_bindgen(js_name = nodeScripts)]
    pub fn node_scripts(&self, path: JsValue) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        to_js(&self.inner.node_scripts(&path).map_err(to_js_error)?)
    }

    #[cfg(feature = "scripting")]
    #[wasm_bindgen(js_name = executeNodeScripts)]
    pub fn execute_node_scripts(
        &self,
        path: JsValue,
        options: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let options = if options.is_null() || options.is_undefined() {
            ScriptExecutionOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|error| JsValue::from_str(&error.to_string()))?
        };
        to_js(
            &self
                .inner
                .execute_node_scripts(path, options)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = snapshot)]
    pub fn snapshot(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.snapshot())
    }

    #[wasm_bindgen(js_name = snapshotForRoot)]
    pub fn snapshot_for_root(&self, root: Option<String>) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.snapshot_for_root(root.as_deref()))
    }

    #[wasm_bindgen(js_name = exportSnapshotJson)]
    pub fn export_snapshot_json(&self) -> std::result::Result<String, JsValue> {
        self.inner.export_snapshot_json().map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = importSnapshotJson)]
    pub fn import_snapshot_json(&self, payload: &str) -> std::result::Result<(), JsValue> {
        self.inner
            .import_snapshot_json(payload)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = mergeSnapshotJson)]
    pub fn merge_snapshot_json(&self, payload: &str) -> std::result::Result<(), JsValue> {
        self.inner.merge_snapshot_json(payload).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = pendingOperations)]
    pub fn pending_operations(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.pending_operations())
    }

    #[wasm_bindgen(js_name = pendingEnvelope)]
    pub fn pending_envelope(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.sync_envelope())
    }

    #[wasm_bindgen(js_name = exportPendingOperationsJson)]
    pub fn export_pending_operations_json(&self) -> std::result::Result<String, JsValue> {
        self.inner
            .export_pending_operations_json()
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = drainPendingOperations)]
    pub fn drain_pending_operations(&self) -> std::result::Result<JsValue, JsValue> {
        let ops = self.inner.drain_pending_operations().map_err(to_js_error)?;
        to_js(&ops)
    }

    #[wasm_bindgen(js_name = drainPendingEnvelope)]
    pub fn drain_pending_envelope(&self) -> std::result::Result<JsValue, JsValue> {
        let envelope = self.inner.drain_sync_envelope().map_err(to_js_error)?;
        to_js(&envelope)
    }

    #[wasm_bindgen(js_name = drainPendingOperationsJson)]
    pub fn drain_pending_operations_json(&self) -> std::result::Result<String, JsValue> {
        self.inner
            .drain_pending_operations_json()
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = drainPendingEnvelopeJson)]
    pub fn drain_pending_envelope_json(&self) -> std::result::Result<String, JsValue> {
        self.inner
            .drain_pending_envelope_json()
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = applyOperations)]
    pub fn apply_operations(&self, operations: JsValue) -> std::result::Result<usize, JsValue> {
        let operations: Vec<Operation> = serde_wasm_bindgen::from_value(operations)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner.apply_operations(operations).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = applyEnvelope)]
    pub fn apply_envelope(&self, envelope: JsValue) -> std::result::Result<usize, JsValue> {
        let envelope: SyncEnvelope = serde_wasm_bindgen::from_value(envelope)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner
            .apply_sync_envelope(envelope)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = applyOperationsJson)]
    pub fn apply_operations_json(&self, payload: &str) -> std::result::Result<usize, JsValue> {
        self.inner
            .apply_operations_json(payload)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = useBrowserStorage)]
    pub fn use_browser_storage(&self, key: String) -> std::result::Result<bool, JsValue> {
        self.inner.use_browser_storage(key).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = openDurableStorage)]
    pub async fn open_durable_storage(
        &self,
        config: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let config: DurableStorageConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let binding = match config {
            DurableStorageConfig::BrowserStorage { key } => DurableStorageBinding {
                backend: "browser_storage".to_owned(),
                incremental: false,
                loaded_existing: self.inner.use_browser_storage(key).map_err(to_js_error)?,
                auto_persist: true,
                durability: None,
                lock_mode: None,
            },
            DurableStorageConfig::IndexedDbSnapshots {
                database_name,
                store_name,
                key,
                load_existing,
                auto_persist,
            } => {
                if auto_persist {
                    let hook = self
                        .enable_indexed_db_persistence(
                            database_name,
                            store_name,
                            key,
                            Some(load_existing),
                        )
                        .await?;
                    self.durable_storage_hooks
                        .borrow_mut()
                        .push(WasmDurableStorageHook::Snapshot { _hook: hook });
                } else if load_existing {
                    let _ = self
                        .load_indexed_db(database_name.clone(), store_name.clone(), key.clone())
                        .await?;
                }
                DurableStorageBinding {
                    backend: "indexeddb_snapshot".to_owned(),
                    incremental: false,
                    loaded_existing: load_existing,
                    auto_persist,
                    durability: None,
                    lock_mode: None,
                }
            }
            DurableStorageConfig::IndexedDbSegments {
                database_name,
                store_name,
                namespace,
                load_existing,
                auto_persist,
            } => {
                if auto_persist {
                    let hook = self
                        .enable_indexed_db_segment_persistence(
                            database_name,
                            store_name,
                            namespace,
                            Some(load_existing),
                        )
                        .await?;
                    self.durable_storage_hooks
                        .borrow_mut()
                        .push(WasmDurableStorageHook::Segment { _hook: hook });
                } else if load_existing {
                    let _ = self
                        .load_indexed_db_segments(
                            database_name.clone(),
                            store_name.clone(),
                            namespace.clone(),
                        )
                        .await?;
                }
                DurableStorageBinding {
                    backend: "indexeddb_segments".to_owned(),
                    incremental: true,
                    loaded_existing: load_existing,
                    auto_persist,
                    durability: None,
                    lock_mode: None,
                }
            }
            DurableStorageConfig::OpfsSegments {
                directory,
                namespace,
                load_existing,
                auto_persist,
            } => {
                if auto_persist {
                    let hook = self
                        .enable_opfs_segment_persistence(directory, namespace, Some(load_existing))
                        .await?;
                    self.durable_storage_hooks
                        .borrow_mut()
                        .push(WasmDurableStorageHook::OpfsSegment { _hook: hook });
                } else if load_existing {
                    let _ = self
                        .load_opfs_segments(directory.clone(), namespace.clone())
                        .await?;
                }
                DurableStorageBinding {
                    backend: "opfs_segments".to_owned(),
                    incremental: true,
                    loaded_existing: load_existing,
                    auto_persist,
                    durability: None,
                    lock_mode: None,
                }
            }
            DurableStorageConfig::SnapshotFile { .. }
            | DurableStorageConfig::SegmentFiles { .. } => {
                return Err(JsValue::from_str(
                    "native durable storage config is not available in the browser",
                ));
            }
        };
        to_js(&binding)
    }

    #[wasm_bindgen(js_name = putRecord)]
    pub fn put_record(&self, key: String, value: JsValue) -> std::result::Result<(), JsValue> {
        let value: JsonValue = serde_wasm_bindgen::from_value(value)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner.put_record_json(key, value).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = putRecordBytes)]
    pub fn put_record_bytes(
        &self,
        key: String,
        bytes: Vec<u8>,
    ) -> std::result::Result<(), JsValue> {
        self.inner.put_record_bytes(key, bytes).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = putRecordBlob)]
    pub fn put_record_blob(
        &self,
        key: String,
        bytes: Vec<u8>,
        media_type: Option<String>,
    ) -> std::result::Result<JsValue, JsValue> {
        let reference = self
            .inner
            .put_record_blob(key, bytes, media_type.as_deref())
            .map_err(to_js_error)?;
        to_js(&reference)
    }

    #[wasm_bindgen(js_name = getRecord)]
    pub fn get_record(&self, key: String) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.get_record(&key).map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = scanRecords)]
    pub fn scan_records(&self, scan: JsValue) -> std::result::Result<JsValue, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        to_js(&self.inner.scan_records(scan).map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = watchRecords)]
    pub fn watch_records(
        &self,
        scan: JsValue,
        callback: js_sys::Function,
    ) -> std::result::Result<WasmRecordWatchSubscription, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let subscription = self.inner.watch_records(scan).map_err(to_js_error)?;
        let receiver = subscription.receiver();
        let callback = callback.clone();

        spawn_local(async move {
            while let Ok(result) = receiver.recv().await {
                let js_value = to_js(&result).unwrap_or(JsValue::NULL);
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        });

        Ok(WasmRecordWatchSubscription {
            inner: Some(subscription),
        })
    }

    #[wasm_bindgen(js_name = createVectorCollection)]
    pub fn create_vector_collection(
        &self,
        name: String,
        config: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let config: VectorCollectionConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner
            .create_vector_collection(name, config)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = putVector)]
    pub fn put_vector(
        &self,
        collection: String,
        id: String,
        vector: JsValue,
        metadata: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let vector: Vec<f32> = serde_wasm_bindgen::from_value(vector)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let metadata: Option<JsonValue> = if metadata.is_null() || metadata.is_undefined() {
            None
        } else {
            Some(
                serde_wasm_bindgen::from_value(metadata)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?,
            )
        };
        self.inner
            .put_vector(collection, id, vector, metadata)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = deleteVector)]
    pub fn delete_vector(
        &self,
        collection: String,
        id: String,
    ) -> std::result::Result<(), JsValue> {
        self.inner
            .delete_vector(collection, id)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = getVector)]
    pub fn get_vector(
        &self,
        collection: String,
        id: String,
    ) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.get_vector(collection, id).map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = searchVectors)]
    pub fn search_vectors(
        &self,
        collection: String,
        query: JsValue,
        spec: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        to_js(
            &self
                .inner
                .search_vectors(collection, query, spec)
                .map_err(to_js_error)?,
        )
    }

    #[wasm_bindgen(js_name = watchVectorSearch)]
    pub fn watch_vector_search(
        &self,
        collection: String,
        query: JsValue,
        spec: JsValue,
        callback: js_sys::Function,
    ) -> std::result::Result<WasmVectorWatchSubscription, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let subscription = self
            .inner
            .watch_vector_search(collection, query, spec)
            .map_err(to_js_error)?;
        let receiver = subscription.receiver();
        let callback = callback.clone();

        spawn_local(async move {
            while let Ok(result) = receiver.recv().await {
                let js_value = to_js(&result).unwrap_or(JsValue::NULL);
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        });

        Ok(WasmVectorWatchSubscription {
            inner: Some(subscription),
        })
    }

    #[wasm_bindgen(js_name = saveVectorCacheOpfs)]
    pub async fn save_vector_cache_opfs(
        &self,
        directory: String,
        namespace: String,
        collection: String,
    ) -> std::result::Result<JsValue, JsValue> {
        let files = self
            .inner
            .export_vector_cache_files(&collection)
            .map_err(to_js_error)?;
        let summary = write_vector_cache_opfs(&directory, &namespace, &collection, &files).await?;
        to_js(&summary)
    }

    #[wasm_bindgen(js_name = loadVectorCacheOpfs)]
    pub async fn load_vector_cache_opfs(
        &self,
        directory: String,
        namespace: String,
        collection: String,
    ) -> std::result::Result<bool, JsValue> {
        let Some(files) = load_vector_cache_opfs(&directory, &namespace, &collection).await? else {
            return Ok(false);
        };
        self.inner
            .import_vector_cache_files(&collection, files)
            .map_err(to_js_error)?;
        Ok(true)
    }

    #[wasm_bindgen(js_name = applyRecordBatch)]
    pub fn apply_record_batch(&self, batch: JsValue) -> std::result::Result<JsValue, JsValue> {
        let batch: RecordBatch = serde_wasm_bindgen::from_value(batch)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        to_js(&self.inner.apply_record_batch(batch).map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = deleteRecord)]
    pub fn delete_record(&self, key: String) -> std::result::Result<(), JsValue> {
        self.inner.delete_record(key).map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = syncStorage)]
    pub fn sync_storage(&self) -> std::result::Result<JsValue, JsValue> {
        Err(JsValue::from_str(
            "syncStorage is not available for browser storage backends",
        ))
    }

    #[wasm_bindgen(js_name = storageRecoveryReport)]
    pub fn storage_recovery_report(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&Option::<crate::StorageRecoveryReport>::None)
    }

    #[wasm_bindgen(js_name = closeDurableStorage)]
    pub fn close_durable_storage(&self) {
        self.inner.close_durable_storage();
        self.durable_storage_hooks.borrow_mut().clear();
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = registerUser)]
    pub fn register_user(
        &self,
        alias: String,
        public_key_base64: String,
        roots: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let roots: Vec<String> = serde_wasm_bindgen::from_value(roots)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let grants = roots
            .into_iter()
            .map(crate::UserGrant::write_root)
            .collect::<Vec<_>>();
        let public_identity =
            crate::PublicIdentity::from_base64(&public_key_base64).map_err(to_js_error)?;
        self.inner
            .register_user(alias, public_identity, grants)
            .map_err(to_js_error)
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = authenticateLocalUser)]
    pub fn authenticate_local_user(
        &self,
        alias: String,
        secret_key_base64: String,
        roots: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let roots: Vec<String> = serde_wasm_bindgen::from_value(roots)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let grants = roots
            .into_iter()
            .map(crate::UserGrant::write_root)
            .collect::<Vec<_>>();
        let identity =
            crate::Identity::from_secret_key_base64(&secret_key_base64).map_err(to_js_error)?;
        self.inner
            .authenticate_local_user(alias, identity, grants)
            .map_err(to_js_error)
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = requireSignedSync)]
    pub fn require_signed_sync(&self, required: bool) {
        self.inner.set_require_signed_sync(required);
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = setSnapshotEncryptionKey)]
    pub fn set_snapshot_encryption_key(
        &self,
        key_base64: String,
    ) -> std::result::Result<(), JsValue> {
        let key = crate::SecretBoxKey::from_base64(&key_base64).map_err(to_js_error)?;
        self.inner.set_snapshot_encryption_key(key);
        Ok(())
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = setTransportEncryptionKey)]
    pub fn set_transport_encryption_key(
        &self,
        key_base64: String,
    ) -> std::result::Result<(), JsValue> {
        let key = crate::SecretBoxKey::from_base64(&key_base64).map_err(to_js_error)?;
        self.inner.set_transport_encryption_key(key);
        Ok(())
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = createWriteCertificate)]
    pub fn create_write_certificate(
        &self,
        certificants: JsValue,
        write_policy: JsValue,
        expires_at_millis: Option<f64>,
        write_block: JsValue,
    ) -> std::result::Result<String, JsValue> {
        let certificants: Vec<String> = serde_wasm_bindgen::from_value(certificants)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let write_policy = js_to_json(write_policy)?;
        let write_block = if write_block.is_null() || write_block.is_undefined() {
            None
        } else {
            Some(js_to_json(write_block)?)
        };
        self.inner
            .create_write_certificate(
                certificants,
                write_policy,
                expires_at_millis.map(|value| value as u64),
                write_block,
            )
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = setNetworkHooks)]
    pub fn set_network_hooks(&self, hooks: JsValue) -> std::result::Result<(), JsValue> {
        if hooks.is_null() || hooks.is_undefined() {
            self.inner.clear_network_hooks();
            return Ok(());
        }
        let callbacks = parse_wasm_network_hook_callbacks(hooks)?;
        self.inner
            .set_network_hooks(Arc::new(WasmNetworkHooks { callbacks }));
        Ok(())
    }

    #[wasm_bindgen(js_name = clearNetworkHooks)]
    pub fn clear_network_hooks(&self) {
        self.inner.clear_network_hooks();
    }

    #[wasm_bindgen(js_name = saveIndexedDb)]
    pub async fn save_indexed_db(
        &self,
        database_name: String,
        store_name: String,
        key: String,
    ) -> std::result::Result<(), JsValue> {
        let payload = self
            .inner
            .export_persisted_snapshot_json()
            .map_err(to_js_error)?;
        save_snapshot_string_indexed_db(&database_name, &store_name, &key, &payload).await
    }

    #[wasm_bindgen(js_name = loadIndexedDb)]
    pub async fn load_indexed_db(
        &self,
        database_name: String,
        store_name: String,
        key: String,
    ) -> std::result::Result<bool, JsValue> {
        match load_snapshot_string_indexed_db(&database_name, &store_name, &key).await? {
            Some(payload) => {
                self.inner
                    .import_persisted_snapshot_json(&payload)
                    .map_err(to_js_error)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[wasm_bindgen(js_name = enableIndexedDbPersistence)]
    pub async fn enable_indexed_db_persistence(
        &self,
        database_name: String,
        store_name: String,
        key: String,
        load_existing: Option<bool>,
    ) -> std::result::Result<WasmIndexedDbPersistence, JsValue> {
        if load_existing.unwrap_or(true) {
            let _ = self
                .load_indexed_db(database_name.clone(), store_name.clone(), key.clone())
                .await?;
        }

        let subscription = self.inner.subscribe_changes();
        let receiver = subscription.receiver();
        let db = self.inner.clone();
        let db_name = database_name.clone();
        let store = store_name.clone();
        let snapshot_key = key.clone();
        spawn_local(async move {
            while let Ok(mut event) = receiver.recv().await {
                while let Ok(next) = receiver.try_recv() {
                    event.merge(next);
                }
                if !event.data_changed && event.pending_ops == 0 {
                    continue;
                }
                if let Ok(payload) = db.export_persisted_snapshot_json() {
                    let _ =
                        save_snapshot_string_indexed_db(&db_name, &store, &snapshot_key, &payload)
                            .await;
                }
            }
        });

        let hook = WasmIndexedDbPersistence {
            db: self.inner.clone(),
            database_name,
            store_name,
            key,
            subscription: Some(subscription),
        };
        hook.flush().await?;
        Ok(hook)
    }

    #[wasm_bindgen(js_name = saveIndexedDbSegments)]
    pub async fn save_indexed_db_segments(
        &self,
        database_name: String,
        store_name: String,
        namespace: String,
    ) -> std::result::Result<(), JsValue> {
        let transaction = self.inner.full_storage_transaction_without_pending_ops();
        replace_segment_transaction_indexed_db(
            &database_name,
            &store_name,
            &namespace,
            &transaction,
        )
        .await?;
        self.inner
            .mark_storage_transaction_flushed(&transaction)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = loadIndexedDbSegments)]
    pub async fn load_indexed_db_segments(
        &self,
        database_name: String,
        store_name: String,
        namespace: String,
    ) -> std::result::Result<bool, JsValue> {
        match load_segment_snapshot_indexed_db(&database_name, &store_name, &namespace).await? {
            Some(snapshot) => {
                let payload = serde_json::to_string(&snapshot)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?;
                self.inner
                    .import_persisted_snapshot_json(&payload)
                    .map_err(to_js_error)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[wasm_bindgen(js_name = enableIndexedDbSegmentPersistence)]
    pub async fn enable_indexed_db_segment_persistence(
        &self,
        database_name: String,
        store_name: String,
        namespace: String,
        load_existing: Option<bool>,
    ) -> std::result::Result<WasmIndexedDbSegmentPersistence, JsValue> {
        if load_existing.unwrap_or(true) {
            let _ = self
                .load_indexed_db_segments(
                    database_name.clone(),
                    store_name.clone(),
                    namespace.clone(),
                )
                .await?;
        }

        self.inner.register_external_storage_hook();
        let subscription = self.inner.subscribe_changes();
        let receiver = subscription.receiver();
        let db = self.inner.clone();
        let db_name = database_name.clone();
        let store = store_name.clone();
        let namespace_key = namespace.clone();
        let stats = Rc::new(RefCell::new(WasmSegmentPersistenceStats::default()));
        let task_stats = Rc::clone(&stats);
        spawn_local(async move {
            while let Ok(mut event) = receiver.recv().await {
                {
                    let mut stats = task_stats.borrow_mut();
                    stats.queued_events = stats.queued_events.saturating_add(1);
                }
                while let Ok(next) = receiver.try_recv() {
                    event.merge(next);
                    let mut stats = task_stats.borrow_mut();
                    stats.coalesced_events = stats.coalesced_events.saturating_add(1);
                }
                if !event.data_changed {
                    continue;
                }
                let transaction = if event.full_refresh {
                    db.full_storage_transaction_without_pending_ops()
                } else {
                    db.incremental_storage_transaction_without_pending_ops()
                };
                let result = if event.full_refresh {
                    replace_segment_transaction_indexed_db(
                        &db_name,
                        &store,
                        &namespace_key,
                        &transaction,
                    )
                    .await
                } else {
                    apply_segment_transaction_indexed_db(
                        &db_name,
                        &store,
                        &namespace_key,
                        &transaction,
                    )
                    .await
                };
                match result {
                    Ok(summary) => match db.mark_storage_transaction_flushed(&transaction) {
                        Ok(()) => {
                            let kind = if event.full_refresh {
                                SegmentWriteKind::FullReplacement
                            } else {
                                SegmentWriteKind::Incremental
                            };
                            record_segment_write_success(&task_stats, summary, kind);
                        }
                        Err(error) => record_segment_write_error(&task_stats, error.to_string()),
                    },
                    Err(error) => record_segment_write_error(&task_stats, js_error_string(error)),
                }
            }
        });

        let hook = WasmIndexedDbSegmentPersistence {
            db: self.inner.clone(),
            database_name,
            store_name,
            namespace,
            stats,
            external_hook_registered: true,
            subscription: Some(subscription),
        };
        hook.flush().await?;
        Ok(hook)
    }

    #[wasm_bindgen(js_name = saveOpfsSegments)]
    pub async fn save_opfs_segments(
        &self,
        directory: String,
        namespace: String,
    ) -> std::result::Result<(), JsValue> {
        let transaction = self.inner.full_storage_transaction_without_pending_ops();
        replace_segment_transaction_opfs(&directory, &namespace, &transaction).await?;
        self.inner
            .mark_storage_transaction_flushed(&transaction)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = loadOpfsSegments)]
    pub async fn load_opfs_segments(
        &self,
        directory: String,
        namespace: String,
    ) -> std::result::Result<bool, JsValue> {
        match load_segment_snapshot_opfs(&directory, &namespace).await? {
            Some(snapshot) => {
                let payload = serde_json::to_string(&snapshot)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?;
                self.inner
                    .import_persisted_snapshot_json(&payload)
                    .map_err(to_js_error)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[wasm_bindgen(js_name = enableOpfsSegmentPersistence)]
    pub async fn enable_opfs_segment_persistence(
        &self,
        directory: String,
        namespace: String,
        load_existing: Option<bool>,
    ) -> std::result::Result<WasmOpfsSegmentPersistence, JsValue> {
        if load_existing.unwrap_or(true) {
            let _ = self
                .load_opfs_segments(directory.clone(), namespace.clone())
                .await?;
        }

        self.inner.register_external_storage_hook();
        let subscription = self.inner.subscribe_changes();
        let receiver = subscription.receiver();
        let db = self.inner.clone();
        let directory_key = directory.clone();
        let namespace_key = namespace.clone();
        let stats = Rc::new(RefCell::new(WasmSegmentPersistenceStats::default()));
        let task_stats = Rc::clone(&stats);
        spawn_local(async move {
            while let Ok(mut event) = receiver.recv().await {
                {
                    let mut stats = task_stats.borrow_mut();
                    stats.queued_events = stats.queued_events.saturating_add(1);
                }
                while let Ok(next) = receiver.try_recv() {
                    event.merge(next);
                    let mut stats = task_stats.borrow_mut();
                    stats.coalesced_events = stats.coalesced_events.saturating_add(1);
                }
                if !event.data_changed {
                    continue;
                }
                let transaction = if event.full_refresh {
                    db.full_storage_transaction_without_pending_ops()
                } else {
                    db.incremental_storage_transaction_without_pending_ops()
                };
                let result = if event.full_refresh {
                    replace_segment_transaction_opfs(&directory_key, &namespace_key, &transaction)
                        .await
                } else {
                    apply_segment_transaction_opfs(&directory_key, &namespace_key, &transaction)
                        .await
                };
                match result {
                    Ok(summary) => match db.mark_storage_transaction_flushed(&transaction) {
                        Ok(()) => {
                            let kind = if event.full_refresh {
                                SegmentWriteKind::FullReplacement
                            } else {
                                SegmentWriteKind::Incremental
                            };
                            record_segment_write_success(&task_stats, summary, kind);
                        }
                        Err(error) => record_segment_write_error(&task_stats, error.to_string()),
                    },
                    Err(error) => record_segment_write_error(&task_stats, js_error_string(error)),
                }
            }
        });

        let hook = WasmOpfsSegmentPersistence {
            db: self.inner.clone(),
            directory,
            namespace,
            stats,
            external_hook_registered: true,
            subscription: Some(subscription),
        };
        hook.flush().await?;
        Ok(hook)
    }

    #[wasm_bindgen(js_name = openBlobStorage)]
    pub fn open_blob_storage(&self, config: JsValue) -> std::result::Result<JsValue, JsValue> {
        let config: BlobStorageConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match config {
            BlobStorageConfig::IndexedDb {
                database_name,
                store_name,
                namespace,
            } => {
                *self.blob_storage.borrow_mut() = Some(WasmBlobStorageConfig {
                    database_name,
                    store_name,
                    namespace,
                });
                to_js(&BlobStorageBinding {
                    backend: "indexed_db".to_owned(),
                    content_addressed: true,
                    durability: None,
                })
            }
            BlobStorageConfig::Memory => {
                self.inner
                    .open_blob_storage(BlobStorageConfig::Memory)
                    .map_err(to_js_error)?;
                *self.blob_storage.borrow_mut() = None;
                to_js(&BlobStorageBinding {
                    backend: "memory".to_owned(),
                    content_addressed: true,
                    durability: None,
                })
            }
            #[cfg(not(target_arch = "wasm32"))]
            BlobStorageConfig::Files { .. } => Err(JsValue::from_str(
                "file blob storage is not available on wasm targets",
            )),
        }
    }

    #[wasm_bindgen(js_name = enableIndexedDbBlobStorage)]
    pub fn enable_indexed_db_blob_storage(
        &self,
        database_name: String,
        store_name: String,
        namespace: String,
    ) -> WasmIndexedDbBlobStorage {
        let config = WasmBlobStorageConfig {
            database_name,
            store_name,
            namespace,
        };
        *self.blob_storage.borrow_mut() = Some(config.clone());
        WasmIndexedDbBlobStorage { config }
    }

    #[wasm_bindgen(js_name = connectWebSocket)]
    pub fn connect_web_socket(
        &self,
        url: String,
        retry_interval_ms: Option<i32>,
    ) -> std::result::Result<WasmWebSocketSync, JsValue> {
        self.connect_relay_config(RelayClientConfig {
            url,
            retry_interval_ms: retry_interval_ms.unwrap_or(2_000).max(1) as u64,
            session_auth: crate::SessionAuthConfig::default(),
        })
    }

    #[wasm_bindgen(js_name = connectRelay)]
    pub fn connect_relay(
        &self,
        config: JsValue,
    ) -> std::result::Result<WasmWebSocketSync, JsValue> {
        let config: RelayClientConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.connect_relay_config(config)
    }

    #[wasm_bindgen(js_name = connectWebRtcMesh)]
    pub fn connect_web_rtc_mesh(
        &self,
        room: String,
        retry_interval_ms: Option<i32>,
        options: Option<JsValue>,
    ) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        let mut config = parse_mesh_config(room, retry_interval_ms, options)?;
        config.signaling = MeshSignalingMode::BroadcastChannel;
        self.connect_mesh_config(config)
    }

    #[wasm_bindgen(js_name = connectWebRtcMeshViaRelay)]
    pub fn connect_web_rtc_mesh_via_relay(
        &self,
        url: String,
        room: String,
        retry_interval_ms: Option<i32>,
        options: Option<JsValue>,
    ) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        let mut config = parse_mesh_config(room, retry_interval_ms, options)?;
        config.signaling = MeshSignalingMode::Relay;
        config.relay_url = Some(url);
        self.connect_mesh_config(config)
    }

    #[wasm_bindgen(js_name = connectMesh)]
    pub fn connect_mesh(&self, config: JsValue) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        let config: MeshConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.connect_mesh_config(config)
    }

    #[wasm_bindgen(js_name = connectMeshWithExternalSignaling)]
    pub fn connect_mesh_with_external_signaling(
        &self,
        config: JsValue,
        send_route: js_sys::Function,
    ) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        let config: MeshConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.connect_mesh_config_external(config, send_route)
    }
}

impl WasmPrimadb {
    fn connect_relay_config(
        &self,
        config: RelayClientConfig,
    ) -> std::result::Result<WasmWebSocketSync, JsValue> {
        let url = config.url;
        let session_auth = config.session_auth;
        let retry_interval_ms = config.retry_interval_ms.min(i32::MAX as u64) as i32;
        let socket = web_sys::WebSocket::new(&url)?;
        socket.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let state = Rc::new(RefCell::new(WebSocketSyncState {
            db: self.inner.clone(),
            router: Router::new(RouterConfig {
                peer_id: format!("browser:{}", self.inner.replica_id()),
                default_channel: "primadb-sync".to_owned(),
                default_ttl: 6,
                max_seen_routes: self.inner.limits().max_seen_routes,
            }),
            session_id: crate::session_auth::random_session_id(&format!(
                "browser:{}",
                self.inner.replica_id()
            )),
            session_auth,
            socket: socket.clone(),
            inflight: BTreeMap::new(),
            pending_requests: BTreeMap::new(),
            outgoing_watches: BTreeMap::new(),
            incoming_watches: BTreeMap::new(),
            recommendations: BTreeMap::new(),
            pending_auth_challenges: BTreeMap::new(),
            pending_auth_peers: BTreeMap::new(),
            verified_identities: BTreeMap::new(),
            applications: ApplicationRouteBus::default(),
            next_message_seq: 0,
        }));

        let onmessage_state = state.clone();
        let onmessage = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
            if let Some(payload) = event.data().as_string() {
                let _ = handle_websocket_message(&onmessage_state, &payload);
            }
        }) as Box<dyn FnMut(_)>);
        socket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        let onopen_state = state.clone();
        let relay_url = url.clone();
        let onopen = Closure::wrap(Box::new(move |_event: web_sys::Event| {
            let mut route = {
                let borrowed = onopen_state.borrow();
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
                    "application_routes".to_owned(),
                ];
                capabilities.extend(borrowed.db.vector_presence_capabilities());
                borrowed.router.presence(
                    borrowed.db.replica_id(),
                    "websocket",
                    capabilities,
                    vec!["primadb-sync".to_owned()],
                )
            };
            if let RoutePayload::Presence { peer } = &mut route.payload {
                peer.metadata
                    .insert("relay_url".to_owned(), relay_url.clone());
                let borrowed = onopen_state.borrow();
                peer.identity = borrowed.db.session_presence_identity(&borrowed.session_id);
            }
            let _ = send_route_state(&onopen_state, &route);
            let _ = retry_inflight_state(&onopen_state);
            let _ = flush_pending_state(&onopen_state);
        }) as Box<dyn FnMut(_)>);
        socket.set_onopen(Some(onopen.as_ref().unchecked_ref()));

        let onclose_state = state.clone();
        let onclose = Closure::wrap(Box::new(move |_event: web_sys::CloseEvent| {
            requeue_inflight_state(&onclose_state);
            fail_pending_requests_state(
                &onclose_state,
                "websocket closed while requests were pending",
            );
            fail_outgoing_watches_state(&onclose_state, "websocket closed");
            clear_incoming_watches_state(&onclose_state);
        }) as Box<dyn FnMut(_)>);
        socket.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        let onerror_state = state.clone();
        let onerror = Closure::wrap(Box::new(move |_event: web_sys::Event| {
            requeue_inflight_state(&onerror_state);
            fail_pending_requests_state(
                &onerror_state,
                "websocket errored while requests were pending",
            );
            fail_outgoing_watches_state(&onerror_state, "websocket errored");
            clear_incoming_watches_state(&onerror_state);
        }) as Box<dyn FnMut(_)>);
        socket.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        let change_subscription = self.inner.subscribe_changes();
        let receiver = change_subscription.receiver();
        let change_state = state.clone();
        spawn_local(async move {
            while let Ok(mut event) = receiver.recv().await {
                while let Ok(next) = receiver.try_recv() {
                    event.merge(next);
                }
                if event.pending_ops > 0 {
                    let _ = flush_pending_state(&change_state);
                }
                if event.data_changed {
                    let _ = emit_incoming_watch_updates_state(&change_state, &event);
                }
            }
        });

        let interval_ms = retry_interval_ms;
        let interval_state = state.clone();
        let interval_callback = Closure::wrap(Box::new(move || {
            let _ = retry_inflight_state(&interval_state);
            let _ = flush_pending_state(&interval_state);
        }) as Box<dyn FnMut()>);
        let interval_id = browser_window()?
            .set_interval_with_callback_and_timeout_and_arguments_0(
                interval_callback.as_ref().unchecked_ref(),
                interval_ms,
            )?;
        let node_fetch_registration =
            self.inner
                .register_node_fetch_scheduler(Arc::new(WasmWebSocketNodeFetchScheduler {
                    state: Rc::downgrade(&state),
                }));

        Ok(WasmWebSocketSync {
            state,
            change_subscription: Some(change_subscription),
            onmessage: Some(onmessage),
            onopen: Some(onopen),
            onclose: Some(onclose),
            onerror: Some(onerror),
            interval_callback: Some(interval_callback),
            interval_id: Some(interval_id),
            node_fetch_registration: Some(node_fetch_registration),
        })
    }

    fn connect_mesh_config(
        &self,
        config: MeshConfig,
    ) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        self.connect_mesh_config_internal(config, None)
    }

    fn connect_mesh_config_external(
        &self,
        config: MeshConfig,
        send_route: js_sys::Function,
    ) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        self.connect_mesh_config_internal(config, Some(send_route))
    }

    fn connect_mesh_config_internal(
        &self,
        config: MeshConfig,
        external_send_route: Option<js_sys::Function>,
    ) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        let room = config.room.clone();
        let session_auth = config.session_auth.clone();
        let rtc_configuration = build_web_rtc_configuration(&config.effective_ice_servers())?;
        let peer_id = format!(
            "mesh:{}:{}",
            self.inner.replica_id(),
            js_sys::Date::now() as u64
        );
        let signaling = if let Some(send_route) = external_send_route {
            let (mode, relay_url) = mesh_external_signaling_metadata(&config);
            MeshSignalingTransport::External {
                send_route,
                relay_url,
                mode,
            }
        } else {
            match config.signaling {
                MeshSignalingMode::BroadcastChannel => MeshSignalingTransport::BroadcastChannel(
                    web_sys::BroadcastChannel::new(&format!("primadb-mesh-{room}"))?,
                ),
                MeshSignalingMode::Relay => {
                    let url = mesh_websocket_relay_url(&config)?;
                    let socket = web_sys::WebSocket::new(&url)?;
                    socket.set_binary_type(web_sys::BinaryType::Arraybuffer);
                    MeshSignalingTransport::Relay {
                        socket,
                        relay_url: url,
                    }
                }
            }
        };
        let state = Rc::new(RefCell::new(WebRtcMeshState {
            db: self.inner.clone(),
            router: Router::new(RouterConfig {
                peer_id: peer_id.clone(),
                default_channel: format!("mesh:{room}"),
                default_ttl: 6,
                max_seen_routes: self.inner.limits().max_seen_routes,
            }),
            room: room.clone(),
            peer_id: peer_id.clone(),
            session_id: crate::session_auth::random_session_id(&format!(
                "mesh:browser:{}",
                self.inner.replica_id()
            )),
            session_auth,
            signaling,
            rtc_configuration,
            peers: BTreeMap::new(),
            inflight: BTreeMap::new(),
            outgoing_watches: BTreeMap::new(),
            incoming_watches: BTreeMap::new(),
            recommendations: BTreeMap::new(),
            pending_auth_challenges: BTreeMap::new(),
            pending_auth_peers: BTreeMap::new(),
            verified_identities: BTreeMap::new(),
            applications: ApplicationRouteBus::default(),
            next_message_seq: 0,
            relay_onmessage: None,
            relay_onopen: None,
            relay_onclose: None,
            relay_onerror: None,
        }));
        let signaling = { state.borrow().signaling.clone() };
        let signaling_onmessage = match signaling {
            MeshSignalingTransport::BroadcastChannel(signaling) => {
                let signal_state = state.clone();
                let onmessage = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                    let Ok(signal) = serde_wasm_bindgen::from_value::<MeshSignal>(event.data())
                    else {
                        return;
                    };
                    let _ = handle_mesh_signal_state(&signal_state, signal);
                }) as Box<dyn FnMut(_)>);
                signaling.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
                Some(onmessage)
            }
            MeshSignalingTransport::Relay { socket, relay_url } => {
                initialize_mesh_relay_callbacks(&state, relay_url);
                bind_mesh_relay_socket_callbacks(&state, &socket);
                None
            }
            MeshSignalingTransport::External { .. } => {
                let _ = send_mesh_presence_state(&state);
                announce_mesh_join_state(&state)?;
                None
            }
        };

        let change_subscription = self.inner.subscribe_changes();
        let receiver = change_subscription.receiver();
        let change_state = state.clone();
        spawn_local(async move {
            while let Ok(mut event) = receiver.recv().await {
                while let Ok(next) = receiver.try_recv() {
                    event.merge(next);
                }
                if event.pending_ops > 0 {
                    let _ = flush_mesh_pending_state(&change_state);
                }
                if event.data_changed {
                    let _ = emit_incoming_mesh_watch_updates_state(&change_state, &event);
                }
            }
        });

        let retry_ms = config.retry_interval_ms.min(i32::MAX as u64) as i32;
        let retry_state = state.clone();
        let retry_callback = Closure::wrap(Box::new(move || {
            let _ = ensure_mesh_relay_socket_connected_state(&retry_state);
            let _ = refresh_external_mesh_presence_state(&retry_state);
            let _ = announce_mesh_join_state(&retry_state);
            let _ = retry_mesh_inflight_state(&retry_state);
            let _ = flush_mesh_pending_state(&retry_state);
        }) as Box<dyn FnMut()>);
        let retry_interval_id = browser_window()?
            .set_interval_with_callback_and_timeout_and_arguments_0(
                retry_callback.as_ref().unchecked_ref(),
                retry_ms,
            )?;
        if matches!(
            &state.borrow().signaling,
            MeshSignalingTransport::BroadcastChannel(_)
        ) {
            announce_mesh_join_state(&state)?;
        }
        let node_fetch_registration =
            self.inner
                .register_node_fetch_scheduler(Arc::new(WasmWebRtcNodeFetchScheduler {
                    state: Rc::downgrade(&state),
                }));

        Ok(WasmWebRtcMesh {
            state,
            change_subscription: Some(change_subscription),
            signaling_onmessage,
            retry_callback: Some(retry_callback),
            retry_interval_id: Some(retry_interval_id),
            node_fetch_registration: Some(node_fetch_registration),
        })
    }
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = generateSeaPair)]
pub fn generate_sea_pair() -> std::result::Result<JsValue, JsValue> {
    to_js(&crate::SeaPair::generate())
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = derivePasswordKey)]
pub fn derive_password_key(
    password: String,
    options: JsValue,
) -> std::result::Result<JsValue, JsValue> {
    let options = if options.is_null() || options.is_undefined() {
        crate::PasswordKeyDerivationOptions::default()
    } else {
        serde_wasm_bindgen::from_value(options)
            .map_err(|error| JsValue::from_str(&error.to_string()))?
    };
    let derived = crate::derive_password_key(password, options).map_err(to_js_error)?;
    to_js(&derived)
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = seaPairFromPrivateKeys)]
pub fn sea_pair_from_private_keys(
    secret_key_base64: String,
    encryption_secret_key_base64: String,
) -> std::result::Result<JsValue, JsValue> {
    to_js(
        &crate::SeaPair::from_private_keys(&secret_key_base64, &encryption_secret_key_base64)
            .map_err(to_js_error)?,
    )
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = seaSign)]
pub fn sea_sign(pair: JsValue, payload: JsValue) -> std::result::Result<JsValue, JsValue> {
    let pair: crate::SeaPair = serde_wasm_bindgen::from_value(pair)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let payload = js_to_json(payload)?;
    let signed = pair.sign_payload(payload).map_err(to_js_error)?;
    to_js(&signed)
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = seaVerify)]
pub fn sea_verify(
    public_key_base64: String,
    signed: JsValue,
) -> std::result::Result<JsValue, JsValue> {
    let signed: crate::SignedPayload<JsonValue> = serde_wasm_bindgen::from_value(signed)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let identity = crate::PublicIdentity::from_base64(&public_key_base64).map_err(to_js_error)?;
    identity.verify_payload(&signed).map_err(to_js_error)?;
    to_js(&signed.payload)
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = seaSecret)]
pub fn sea_secret(
    pair: JsValue,
    other_epub_base64: String,
) -> std::result::Result<String, JsValue> {
    let pair: crate::SeaPair = serde_wasm_bindgen::from_value(pair)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let shared = pair
        .derive_secret_box(&other_epub_base64)
        .map_err(to_js_error)?;
    Ok(shared.to_base64())
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = seaEncrypt)]
pub fn sea_encrypt(key_base64: String, payload: JsValue) -> std::result::Result<JsValue, JsValue> {
    let key = crate::SecretBoxKey::from_base64(&key_base64).map_err(to_js_error)?;
    let encrypted = key
        .encrypt_json(&js_to_json(payload)?)
        .map_err(to_js_error)?;
    to_js(&encrypted)
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = seaDecrypt)]
pub fn sea_decrypt(key_base64: String, payload: JsValue) -> std::result::Result<JsValue, JsValue> {
    let key = crate::SecretBoxKey::from_base64(&key_base64).map_err(to_js_error)?;
    let payload: crate::EncryptedPayload = serde_wasm_bindgen::from_value(payload)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let decrypted: JsonValue = key.decrypt_json(&payload).map_err(to_js_error)?;
    to_js(&decrypted)
}

#[wasm_bindgen(js_class = Chain)]
impl WasmChain {
    pub fn field(&self, key: String) -> WasmChain {
        WasmChain {
            inner: self.inner.field(key),
            blob_storage: self.blob_storage.clone(),
        }
    }

    pub fn path(&self) -> String {
        self.inner.path()
    }

    pub fn put(&self, value: JsValue) -> std::result::Result<(), JsValue> {
        self.inner
            .put(js_to_supported_json(value)?)
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = putBytes)]
    pub fn put_bytes(&self, bytes: js_sys::Uint8Array) -> std::result::Result<(), JsValue> {
        self.inner.put_bytes(bytes.to_vec()).map_err(to_js_error)
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = putSigned)]
    pub fn put_signed(
        &self,
        value: JsValue,
        certificate: Option<String>,
    ) -> std::result::Result<(), JsValue> {
        self.inner
            .put_signed(js_to_supported_json(value)?, certificate)
            .map_err(to_js_error)
    }

    pub fn once(&self) -> std::result::Result<JsValue, JsValue> {
        match self.inner.once_json().map_err(to_js_error)? {
            Some(value) => json_to_supported_js(&value),
            None => Ok(JsValue::NULL),
        }
    }

    #[wasm_bindgen(js_name = onceBytes)]
    pub fn once_bytes(&self) -> std::result::Result<JsValue, JsValue> {
        match self.inner.once_bytes().map_err(to_js_error)? {
            Some(bytes) => Ok(js_sys::Uint8Array::from(bytes.as_slice()).into()),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn set(&self, value: JsValue) -> std::result::Result<String, JsValue> {
        self.inner
            .set(js_to_supported_json(value)?)
            .map_err(to_js_error)
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = setSigned)]
    pub fn set_signed(
        &self,
        value: JsValue,
        certificate: Option<String>,
    ) -> std::result::Result<String, JsValue> {
        self.inner
            .set_signed(js_to_supported_json(value)?, certificate)
            .map_err(to_js_error)
    }

    pub fn remove(&self, value: JsValue) -> std::result::Result<String, JsValue> {
        self.inner
            .remove(js_to_supported_json(value)?)
            .map_err(to_js_error)
    }

    pub fn unset(&self) -> std::result::Result<(), JsValue> {
        self.inner.unset().map_err(to_js_error)
    }

    pub fn map(&self) -> std::result::Result<JsValue, JsValue> {
        let entries = self.inner.map().map_err(to_js_error)?;
        map_entries_to_js(&entries)
    }

    pub fn query(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let entries = self.inner.query(spec).map_err(to_js_error)?;
        map_entries_to_js(&entries)
    }

    pub fn scan(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let entries = self.inner.scan(spec).map_err(to_js_error)?;
        lex_entries_to_js(&entries)
    }

    pub fn traverse(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: TraversalSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        to_js(&self.inner.traverse(spec).map_err(to_js_error)?)
    }

    #[wasm_bindgen(js_name = firstQuery)]
    pub fn first_query(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match self.inner.first(spec).map_err(to_js_error)? {
            Some(value) => map_entry_to_js(&value),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn on(&self, callback: js_sys::Function) -> std::result::Result<WasmSubscription, JsValue> {
        let subscription = self.inner.subscribe().map_err(to_js_error)?;
        let receiver = subscription.receiver();
        let callback = callback.clone();

        spawn_local(async move {
            while let Ok(snapshot) = receiver.recv().await {
                let js_value = match snapshot {
                    Some(value) => json_to_supported_js(&value).unwrap_or(JsValue::NULL),
                    None => JsValue::NULL,
                };
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        });

        Ok(WasmSubscription {
            inner: Some(subscription),
        })
    }

    #[wasm_bindgen(js_name = watchTraverse)]
    pub fn watch_traverse(
        &self,
        spec: JsValue,
        callback: js_sys::Function,
    ) -> std::result::Result<WasmTraversalSubscription, JsValue> {
        let spec: TraversalSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let subscription = self.inner.watch_traverse(spec).map_err(to_js_error)?;
        let receiver = subscription.receiver();
        let callback = callback.clone();

        spawn_local(async move {
            while let Ok(result) = receiver.recv().await {
                let js_value = to_js(&result).unwrap_or(JsValue::NULL);
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        });

        Ok(WasmTraversalSubscription {
            inner: Some(subscription),
        })
    }

    #[wasm_bindgen(js_name = putBlob)]
    pub async fn put_blob(
        &self,
        data: js_sys::Uint8Array,
        media_type: Option<String>,
    ) -> std::result::Result<JsValue, JsValue> {
        let Some(config) = self.blob_storage.borrow().clone() else {
            return Err(JsValue::from_str("blob storage is not configured"));
        };
        let reference = save_blob_indexed_db(
            &config.database_name,
            &config.store_name,
            &config.namespace,
            data.to_vec(),
            media_type,
        )
        .await?;
        self.inner
            .put(serde_json::json!({ "$blob": reference.clone() }))
            .map_err(to_js_error)?;
        to_js(&reference)
    }

    #[wasm_bindgen(js_name = blobRef)]
    pub fn blob_ref(&self) -> std::result::Result<JsValue, JsValue> {
        match self.inner.once_blob_ref().map_err(to_js_error)? {
            Some(reference) => to_js(&reference),
            None => Ok(JsValue::NULL),
        }
    }

    #[wasm_bindgen(js_name = getBlob)]
    pub async fn get_blob(&self) -> std::result::Result<JsValue, JsValue> {
        let Some(reference) = self.inner.once_blob_ref().map_err(to_js_error)? else {
            return Ok(JsValue::NULL);
        };
        let Some(config) = self.blob_storage.borrow().clone() else {
            return Err(JsValue::from_str("blob storage is not configured"));
        };
        match load_blob_indexed_db(
            &config.database_name,
            &config.store_name,
            &config.namespace,
            &reference.id,
        )
        .await?
        {
            Some(blob) => Ok(js_sys::Uint8Array::from(blob.data.as_slice()).into()),
            None => Ok(JsValue::NULL),
        }
    }
}

#[wasm_bindgen(js_class = Subscription)]
impl WasmSubscription {
    pub fn cancel(&mut self) {
        self.inner.take();
    }
}

#[wasm_bindgen(js_class = TraversalSubscription)]
impl WasmTraversalSubscription {
    pub fn cancel(&mut self) {
        self.inner.take();
    }
}

#[wasm_bindgen(js_class = RecordWatchSubscription)]
impl WasmRecordWatchSubscription {
    pub fn cancel(&mut self) {
        self.inner.take();
    }
}

#[wasm_bindgen(js_class = VectorWatchSubscription)]
impl WasmVectorWatchSubscription {
    pub fn cancel(&mut self) {
        self.inner.take();
    }
}

fn remote_result_kind(result: &RemoteResult) -> &'static str {
    match result {
        RemoteResult::Get { .. } => "get",
        RemoteResult::Map { .. } => "map",
        RemoteResult::Query { .. } => "query",
        RemoteResult::Lex { .. } => "lex",
        RemoteResult::Records { .. } => "records",
        RemoteResult::VectorSearch { .. } => "vector_search",
        RemoteResult::Node { .. } => "node",
        RemoteResult::Snapshot { .. } => "snapshot",
        RemoteResult::Transaction { .. } => "transaction",
    }
}

fn remote_result_to_js(result: &RemoteResult) -> std::result::Result<JsValue, JsValue> {
    match result {
        RemoteResult::Get { value } => match value {
            Some(value) => json_to_supported_js(value),
            None => Ok(JsValue::NULL),
        },
        RemoteResult::Map { entries } | RemoteResult::Query { entries } => {
            map_entries_to_js(entries)
        }
        RemoteResult::Lex { entries } => lex_entries_to_js(entries),
        RemoteResult::Records { result } => to_js(result),
        RemoteResult::VectorSearch { result } => to_js(result),
        RemoteResult::Node { node } => to_js(node),
        RemoteResult::Snapshot { snapshot } => to_js(snapshot),
        RemoteResult::Transaction { report } => to_js(report),
    }
}

fn remote_watch_payload_to_js(
    payload: Option<std::result::Result<RemoteWatchMessage, String>>,
) -> std::result::Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    let set = |key: &str, value: JsValue| -> std::result::Result<(), JsValue> {
        js_sys::Reflect::set(&object, &JsValue::from_str(key), &value).map(|_| ())
    };

    match payload {
        Some(Ok(message)) => {
            set("done", JsValue::FALSE)?;
            set("initial", JsValue::from_bool(message.initial))?;
            set(
                "kind",
                JsValue::from_str(remote_result_kind(&message.result)),
            )?;
            set("value", remote_result_to_js(&message.result)?)?;
            set("error", JsValue::NULL)?;
        }
        Some(Err(message)) => {
            set("done", JsValue::FALSE)?;
            set("initial", JsValue::FALSE)?;
            set("kind", JsValue::NULL)?;
            set("value", JsValue::NULL)?;
            set("error", JsValue::from_str(&message))?;
        }
        None => {
            set("done", JsValue::TRUE)?;
            set("initial", JsValue::FALSE)?;
            set("kind", JsValue::NULL)?;
            set("value", JsValue::NULL)?;
            set("error", JsValue::NULL)?;
        }
    }

    Ok(object.into())
}

#[wasm_bindgen(js_class = RemoteWatch)]
impl WasmRemoteWatch {
    pub async fn next(&self) -> std::result::Result<JsValue, JsValue> {
        let payload = if let Some(inner) = self.inner.as_ref() {
            inner.receiver.recv().await.ok()
        } else {
            None
        };
        remote_watch_payload_to_js(payload)
    }

    #[wasm_bindgen(js_name = tryNext)]
    pub fn try_next(&self) -> std::result::Result<JsValue, JsValue> {
        let payload = self
            .inner
            .as_ref()
            .and_then(|inner| inner.receiver.try_recv().ok());
        remote_watch_payload_to_js(payload)
    }

    pub fn cancel(&mut self) {
        if let Some(inner) = self.inner.take() {
            (inner.cancel)();
        }
    }
}

#[wasm_bindgen(js_class = ApplicationRouteSubscription)]
impl WasmApplicationRouteSubscription {
    pub async fn next(&self) -> std::result::Result<JsValue, JsValue> {
        let event = if let Some(inner) = self.inner.as_ref() {
            inner.recv().await
        } else {
            None
        };
        match event {
            Some(event) => to_js(&event),
            None => Ok(JsValue::NULL),
        }
    }

    #[wasm_bindgen(js_name = tryNext)]
    pub fn try_next(&self) -> std::result::Result<JsValue, JsValue> {
        match self.inner.as_ref().and_then(|inner| inner.try_recv()) {
            Some(event) => to_js(&event),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn drain(&self) -> std::result::Result<JsValue, JsValue> {
        let events = self
            .inner
            .as_ref()
            .map(|inner| inner.drain())
            .unwrap_or_default();
        to_js(&events)
    }

    pub fn close(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.close();
        }
    }
}

#[wasm_bindgen(js_class = RemoteFanInWatch)]
impl WasmRemoteFanInWatch {
    pub async fn next(&self) -> std::result::Result<JsValue, JsValue> {
        let event = if let Some(inner) = self.inner.as_ref() {
            inner.receiver.recv().await.ok()
        } else {
            None
        };
        match event {
            Some(event) => to_js(&event),
            None => Ok(JsValue::NULL),
        }
    }

    #[wasm_bindgen(js_name = tryNext)]
    pub fn try_next(&self) -> std::result::Result<JsValue, JsValue> {
        let event = self
            .inner
            .as_ref()
            .and_then(|inner| inner.receiver.try_recv().ok());
        match event {
            Some(event) => to_js(&event),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn drain(&self) -> std::result::Result<JsValue, JsValue> {
        let mut events = Vec::new();
        if let Some(inner) = self.inner.as_ref() {
            while let Ok(event) = inner.receiver.try_recv() {
                events.push(event);
            }
        }
        to_js(&events)
    }

    pub fn close(&mut self) {
        if let Some(inner) = self.inner.take() {
            (inner.close)();
        }
    }
}

#[wasm_bindgen(js_class = Scope)]
impl WasmScope {
    pub fn root(&self) -> String {
        self.inner.root().to_owned()
    }

    pub fn configure(&self, policy: JsValue) -> std::result::Result<(), JsValue> {
        let policy: ScopePolicy = serde_wasm_bindgen::from_value(policy)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        self.inner.configure(policy).map_err(to_js_error)
    }

    pub fn policy(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.policy())
    }

    pub fn proposals(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.inner.proposals())
    }

    pub fn transaction(
        &self,
        steps: JsValue,
        options: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let steps: Vec<TransactionStep> = serde_wasm_bindgen::from_value(steps)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let options = if options.is_null() || options.is_undefined() {
            TransactionOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|error| JsValue::from_str(&error.to_string()))?
        };
        to_js(
            &self
                .inner
                .transaction_steps(steps, options)
                .map_err(to_js_error)?,
        )
    }
}

#[wasm_bindgen(js_class = IndexedDbPersistence)]
impl WasmIndexedDbPersistence {
    pub async fn flush(&self) -> std::result::Result<(), JsValue> {
        let payload = self
            .db
            .export_persisted_snapshot_json()
            .map_err(to_js_error)?;
        save_snapshot_string_indexed_db(&self.database_name, &self.store_name, &self.key, &payload)
            .await
    }

    pub fn close(&mut self) {
        self.subscription.take();
    }
}

impl Drop for WasmIndexedDbPersistence {
    fn drop(&mut self) {
        self.subscription.take();
    }
}

#[wasm_bindgen(js_class = IndexedDbSegmentPersistence)]
impl WasmIndexedDbSegmentPersistence {
    pub async fn flush(&self) -> std::result::Result<(), JsValue> {
        let transaction = self.db.full_storage_transaction_without_pending_ops();
        let summary = replace_segment_transaction_indexed_db(
            &self.database_name,
            &self.store_name,
            &self.namespace,
            &transaction,
        )
        .await?;
        self.db
            .mark_storage_transaction_flushed(&transaction)
            .map_err(to_js_error)?;
        record_segment_write_success(&self.stats, summary, SegmentWriteKind::FullReplacement);
        Ok(())
    }

    pub fn stats(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&*self.stats.borrow())
    }

    #[wasm_bindgen(js_name = estimateStorage)]
    pub async fn estimate_storage(&self) -> std::result::Result<JsValue, JsValue> {
        let estimate = estimate_segment_namespace_indexed_db(
            &self.database_name,
            &self.store_name,
            &self.namespace,
        )
        .await?;
        to_js(&estimate)
    }

    pub fn close(&mut self) {
        self.subscription.take();
        if self.external_hook_registered {
            self.db.unregister_external_storage_hook();
            self.external_hook_registered = false;
        }
    }
}

impl Drop for WasmIndexedDbSegmentPersistence {
    fn drop(&mut self) {
        self.close();
    }
}

#[wasm_bindgen(js_class = OpfsSegmentPersistence)]
impl WasmOpfsSegmentPersistence {
    pub async fn flush(&self) -> std::result::Result<(), JsValue> {
        let transaction = self.db.full_storage_transaction_without_pending_ops();
        let summary =
            replace_segment_transaction_opfs(&self.directory, &self.namespace, &transaction)
                .await?;
        self.db
            .mark_storage_transaction_flushed(&transaction)
            .map_err(to_js_error)?;
        record_segment_write_success(&self.stats, summary, SegmentWriteKind::FullReplacement);
        Ok(())
    }

    pub fn stats(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&*self.stats.borrow())
    }

    #[wasm_bindgen(js_name = estimateStorage)]
    pub async fn estimate_storage(&self) -> std::result::Result<JsValue, JsValue> {
        let estimate = estimate_segment_namespace_opfs(&self.directory, &self.namespace).await?;
        to_js(&estimate)
    }

    pub fn close(&mut self) {
        self.subscription.take();
        if self.external_hook_registered {
            self.db.unregister_external_storage_hook();
            self.external_hook_registered = false;
        }
    }
}

impl Drop for WasmOpfsSegmentPersistence {
    fn drop(&mut self) {
        self.close();
    }
}

#[wasm_bindgen(js_class = IndexedDbBlobStorage)]
impl WasmIndexedDbBlobStorage {
    pub async fn put(
        &self,
        data: js_sys::Uint8Array,
        media_type: Option<String>,
    ) -> std::result::Result<JsValue, JsValue> {
        let reference = save_blob_indexed_db(
            &self.config.database_name,
            &self.config.store_name,
            &self.config.namespace,
            data.to_vec(),
            media_type,
        )
        .await?;
        to_js(&reference)
    }

    pub async fn get(&self, blob_id: String) -> std::result::Result<JsValue, JsValue> {
        match load_blob_indexed_db(
            &self.config.database_name,
            &self.config.store_name,
            &self.config.namespace,
            &blob_id,
        )
        .await?
        {
            Some(blob) => Ok(js_sys::Uint8Array::from(blob.data.as_slice()).into()),
            None => Ok(JsValue::NULL),
        }
    }

    #[wasm_bindgen(js_name = hasBlob)]
    pub async fn has_blob(&self, blob_id: String) -> std::result::Result<bool, JsValue> {
        has_blob_indexed_db(
            &self.config.database_name,
            &self.config.store_name,
            &self.config.namespace,
            &blob_id,
        )
        .await
    }
}

#[wasm_bindgen(js_class = WebSocketSync)]
impl WasmWebSocketSync {
    #[wasm_bindgen(js_name = readyState)]
    pub fn ready_state(&self) -> u16 {
        self.state.borrow().socket.ready_state()
    }

    pub fn url(&self) -> String {
        self.state.borrow().socket.url()
    }

    #[wasm_bindgen(js_name = pendingCount)]
    pub fn pending_count(&self) -> usize {
        self.state.borrow().db.pending_operations().len()
    }

    #[wasm_bindgen(js_name = inflightCount)]
    pub fn inflight_count(&self) -> usize {
        self.state.borrow().inflight.len()
    }

    #[wasm_bindgen(js_name = recommendedPeers)]
    pub fn recommended_peers(&self) -> std::result::Result<JsValue, JsValue> {
        let peers = self
            .state
            .borrow()
            .recommendations
            .values()
            .cloned()
            .collect::<Vec<_>>();
        to_js(&peers)
    }

    #[wasm_bindgen(js_name = publishApplication)]
    pub fn publish_application(
        &self,
        message: JsValue,
        target: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let message = application_message_from_js(message)?;
        let target = route_target_from_optional_js(target)?;
        publish_application_relay_state(&self.state, message, target)
            .and_then(|route| to_js(&route))
    }

    #[wasm_bindgen(js_name = sendApplication)]
    pub fn send_application(
        &self,
        namespace: String,
        protocol: String,
        topic: Option<String>,
        body: JsValue,
        metadata: Option<JsValue>,
        target: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let body = json_value_from_js(body)?;
        let metadata = metadata_map_from_js(metadata)?;
        let target = route_target_from_optional_js(target)?;
        publish_application_relay_state(
            &self.state,
            ApplicationRouteMessage::new(namespace, protocol, topic, body, metadata),
            target,
        )
        .and_then(|route| to_js(&route))
    }

    #[wasm_bindgen(js_name = subscribeApplications)]
    pub fn subscribe_applications(
        &self,
        filter: Option<JsValue>,
    ) -> std::result::Result<WasmApplicationRouteSubscription, JsValue> {
        Ok(WasmApplicationRouteSubscription {
            inner: Some(
                self.state
                    .borrow()
                    .applications
                    .subscribe(application_filter_from_js(filter)?),
            ),
        })
    }

    pub async fn get(
        &self,
        path: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Get { path },
        )
        .await?
        {
            RemoteResult::Get { value } => match value {
                Some(value) => json_to_supported_js(&value),
                None => Ok(JsValue::NULL),
            },
            other => Err(JsValue::from_str(&format!(
                "expected get result, received {other:?}"
            ))),
        }
    }

    pub async fn query(
        &self,
        path: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Query { path, spec },
        )
        .await?
        {
            RemoteResult::Query { entries } => map_entries_to_js(&entries),
            other => Err(JsValue::from_str(&format!(
                "expected query result, received {other:?}"
            ))),
        }
    }

    pub async fn lex(
        &self,
        path: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Lex { path, spec },
        )
        .await?
        {
            RemoteResult::Lex { entries } => lex_entries_to_js(&entries),
            other => Err(JsValue::from_str(&format!(
                "expected lex result, received {other:?}"
            ))),
        }
    }

    pub async fn records(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Records { scan },
        )
        .await?
        {
            RemoteResult::Records { result } => to_js(&result),
            other => Err(JsValue::from_str(&format!(
                "expected records result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = recordsFanIn)]
    pub async fn records_fan_in(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        let result = records_fan_in_relay_state(&self.state, scan, policy).await?;
        to_js(&result)
    }

    #[wasm_bindgen(js_name = vectorSearch)]
    pub async fn vector_search(
        &self,
        collection: String,
        query: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            },
        )
        .await?
        {
            RemoteResult::VectorSearch { result } => to_js(&result),
            other => Err(JsValue::from_str(&format!(
                "expected vector_search result, received {other:?}"
            ))),
        }
    }

    pub async fn node(
        &self,
        id: String,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Node { id },
        )
        .await?
        {
            RemoteResult::Node { node } => to_js(&node),
            other => Err(JsValue::from_str(&format!(
                "expected node result, received {other:?}"
            ))),
        }
    }

    pub async fn snapshot(
        &self,
        root: Option<String>,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let policy = remote_policy_from_js(policy)?;
        match request_remote_result_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Snapshot { root },
        )
        .await?
        {
            RemoteResult::Snapshot { snapshot } => to_js(&snapshot),
            other => Err(JsValue::from_str(&format!(
                "expected snapshot result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = watchGet)]
    pub fn watch_get(
        &self,
        path: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(&self.state, policy, PullRequestKind::Get { path })
    }

    #[wasm_bindgen(js_name = watchMap)]
    pub fn watch_map(
        &self,
        path: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(&self.state, policy, PullRequestKind::Map { path })
    }

    #[wasm_bindgen(js_name = watchQuery)]
    pub fn watch_query(
        &self,
        path: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Query { path, spec },
        )
    }

    #[wasm_bindgen(js_name = watchLex)]
    pub fn watch_lex(
        &self,
        path: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Lex { path, spec },
        )
    }

    #[wasm_bindgen(js_name = watchRecords)]
    pub fn watch_records(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(&self.state, policy, PullRequestKind::Records { scan })
    }

    #[wasm_bindgen(js_name = watchRecordsFanIn)]
    pub fn watch_records_fan_in(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteFanInWatch, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        watch_records_fan_in_relay_state(&self.state, scan, policy)
    }

    #[wasm_bindgen(js_name = watchVectorSearch)]
    pub fn watch_vector_search(
        &self,
        collection: String,
        query: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            },
        )
    }

    #[wasm_bindgen(js_name = watchNode)]
    pub fn watch_node(
        &self,
        id: String,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(&self.state, policy, PullRequestKind::Node { id })
    }

    #[wasm_bindgen(js_name = watchSnapshot)]
    pub fn watch_snapshot(
        &self,
        root: Option<String>,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let policy = remote_policy_from_js(policy)?;
        start_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Snapshot { root },
        )
    }

    #[wasm_bindgen(js_name = watchRemoteGet)]
    pub fn watch_remote_get(
        &self,
        peer_id: String,
        path: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Get { path })
    }

    #[wasm_bindgen(js_name = watchRemoteMap)]
    pub fn watch_remote_map(
        &self,
        peer_id: String,
        path: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Map { path })
    }

    #[wasm_bindgen(js_name = watchRemoteQuery)]
    pub fn watch_remote_query(
        &self,
        peer_id: String,
        path: JsValue,
        spec: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Query { path, spec })
    }

    #[wasm_bindgen(js_name = watchRemoteLex)]
    pub fn watch_remote_lex(
        &self,
        peer_id: String,
        path: JsValue,
        spec: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Lex { path, spec })
    }

    #[wasm_bindgen(js_name = watchRemoteRecords)]
    pub fn watch_remote_records(
        &self,
        peer_id: String,
        scan: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Records { scan })
    }

    #[wasm_bindgen(js_name = watchRemoteVectorSearch)]
    pub fn watch_remote_vector_search(
        &self,
        peer_id: String,
        collection: String,
        query: JsValue,
        spec: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_remote_watch_state(
            &self.state,
            peer_id,
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            },
        )
    }

    #[wasm_bindgen(js_name = watchRemoteNode)]
    pub fn watch_remote_node(
        &self,
        peer_id: String,
        id: String,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Node { id })
    }

    #[wasm_bindgen(js_name = watchRemoteSnapshot)]
    pub fn watch_remote_snapshot(
        &self,
        peer_id: String,
        root: Option<String>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        start_remote_watch_state(&self.state, peer_id, PullRequestKind::Snapshot { root })
    }

    #[wasm_bindgen(js_name = remoteGet)]
    pub async fn remote_get(
        &self,
        peer_id: String,
        path: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match request_remote_result_state(&self.state, peer_id, PullRequestKind::Get { path })
            .await?
        {
            RemoteResult::Get { value } => match value {
                Some(value) => json_to_supported_js(&value),
                None => Ok(JsValue::NULL),
            },
            other => Err(JsValue::from_str(&format!(
                "expected get result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteQuery)]
    pub async fn remote_query(
        &self,
        peer_id: String,
        path: JsValue,
        spec: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match request_remote_result_state(
            &self.state,
            peer_id,
            PullRequestKind::Query { path, spec },
        )
        .await?
        {
            RemoteResult::Query { entries } => map_entries_to_js(&entries),
            other => Err(JsValue::from_str(&format!(
                "expected query result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteLex)]
    pub async fn remote_lex(
        &self,
        peer_id: String,
        path: JsValue,
        spec: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match request_remote_result_state(&self.state, peer_id, PullRequestKind::Lex { path, spec })
            .await?
        {
            RemoteResult::Lex { entries } => lex_entries_to_js(&entries),
            other => Err(JsValue::from_str(&format!(
                "expected lex result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteRecords)]
    pub async fn remote_records(
        &self,
        peer_id: String,
        scan: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match request_remote_result_state(&self.state, peer_id, PullRequestKind::Records { scan })
            .await?
        {
            RemoteResult::Records { result } => to_js(&result),
            other => Err(JsValue::from_str(&format!(
                "expected records result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteVectorSearch)]
    pub async fn remote_vector_search(
        &self,
        peer_id: String,
        collection: String,
        query: JsValue,
        spec: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match request_remote_result_state(
            &self.state,
            peer_id,
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            },
        )
        .await?
        {
            RemoteResult::VectorSearch { result } => to_js(&result),
            other => Err(JsValue::from_str(&format!(
                "expected vector_search result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteNode)]
    pub async fn remote_node(
        &self,
        peer_id: String,
        id: String,
    ) -> std::result::Result<JsValue, JsValue> {
        match request_remote_result_state(&self.state, peer_id, PullRequestKind::Node { id })
            .await?
        {
            RemoteResult::Node { node } => to_js(&node),
            other => Err(JsValue::from_str(&format!(
                "expected node result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteSnapshot)]
    pub async fn remote_snapshot(
        &self,
        peer_id: String,
        root: Option<String>,
    ) -> std::result::Result<JsValue, JsValue> {
        match request_remote_result_state(&self.state, peer_id, PullRequestKind::Snapshot { root })
            .await?
        {
            RemoteResult::Snapshot { snapshot } => to_js(&snapshot),
            other => Err(JsValue::from_str(&format!(
                "expected snapshot result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = remoteTransaction)]
    pub async fn remote_transaction(
        &self,
        peer_id: String,
        scope: String,
        steps: JsValue,
        options: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let steps: Vec<TransactionStep> = serde_wasm_bindgen::from_value(steps)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let options = if options.is_null() || options.is_undefined() {
            TransactionOptions::default()
        } else {
            serde_wasm_bindgen::from_value(options)
                .map_err(|error| JsValue::from_str(&error.to_string()))?
        };
        match request_remote_result_state(
            &self.state,
            peer_id,
            PullRequestKind::Transaction {
                scope,
                steps,
                options,
            },
        )
        .await?
        {
            RemoteResult::Transaction { report } => to_js(&report),
            other => Err(JsValue::from_str(&format!(
                "expected transaction result, received {other:?}"
            ))),
        }
    }

    #[wasm_bindgen(js_name = flushPending)]
    pub fn flush_pending(&self) -> std::result::Result<usize, JsValue> {
        flush_pending_state(&self.state)
    }

    #[wasm_bindgen(js_name = retryInflight)]
    pub fn retry_inflight(&self) -> std::result::Result<usize, JsValue> {
        retry_inflight_state(&self.state)
    }

    pub fn close(&mut self) -> std::result::Result<(), JsValue> {
        self.teardown();
        self.state.borrow().socket.close()
    }
}

impl WasmWebSocketSync {
    fn teardown(&mut self) {
        if let Some(id) = self.node_fetch_registration.take() {
            self.state.borrow().db.unregister_node_fetch_scheduler(id);
        }
        self.state.borrow().socket.set_onmessage(None);
        self.state.borrow().socket.set_onopen(None);
        self.state.borrow().socket.set_onclose(None);
        self.state.borrow().socket.set_onerror(None);
        if let Some(interval_id) = self.interval_id.take() {
            if let Ok(window) = browser_window() {
                window.clear_interval_with_handle(interval_id);
            }
        }
        self.change_subscription.take();
        self.onmessage.take();
        self.onopen.take();
        self.onclose.take();
        self.onerror.take();
        self.interval_callback.take();
        requeue_inflight_state(&self.state);
        fail_pending_requests_state(&self.state, "websocket connection closed");
        fail_outgoing_watches_state(&self.state, "websocket connection closed");
        clear_incoming_watches_state(&self.state);
    }
}

impl Drop for WasmWebSocketSync {
    fn drop(&mut self) {
        self.teardown();
    }
}

#[wasm_bindgen(js_class = WebRtcMesh)]
impl WasmWebRtcMesh {
    #[wasm_bindgen(js_name = peerId)]
    pub fn peer_id(&self) -> String {
        self.state.borrow().peer_id.clone()
    }

    #[wasm_bindgen(js_name = signalingMode)]
    pub fn signaling_mode(&self) -> String {
        match &self.state.borrow().signaling {
            MeshSignalingTransport::BroadcastChannel(_) => "broadcast_channel".to_owned(),
            MeshSignalingTransport::Relay { .. } => "relay".to_owned(),
            MeshSignalingTransport::External { mode, .. } => mode.clone(),
        }
    }

    #[wasm_bindgen(js_name = relayUrl)]
    pub fn relay_url(&self) -> Option<String> {
        match &self.state.borrow().signaling {
            MeshSignalingTransport::BroadcastChannel(_) => None,
            MeshSignalingTransport::Relay { relay_url, .. } => Some(relay_url.clone()),
            MeshSignalingTransport::External { relay_url, .. } => relay_url.clone(),
        }
    }

    #[wasm_bindgen(js_name = signalingReadyState)]
    pub fn signaling_ready_state(&self) -> Option<u16> {
        match &self.state.borrow().signaling {
            MeshSignalingTransport::BroadcastChannel(_) => None,
            MeshSignalingTransport::Relay { socket, .. } => Some(socket.ready_state()),
            MeshSignalingTransport::External { .. } => None,
        }
    }

    #[wasm_bindgen(js_name = acceptSignalingRoute)]
    pub fn accept_signaling_route(&self, route: JsValue) -> std::result::Result<(), JsValue> {
        let route: RouteEnvelope = serde_wasm_bindgen::from_value(route)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        handle_mesh_signaling_route(&self.state, route)
    }

    #[wasm_bindgen(js_name = announceSignalingPresence)]
    pub fn announce_signaling_presence(&self) -> std::result::Result<(), JsValue> {
        send_mesh_presence_state(&self.state)
    }

    #[wasm_bindgen(js_name = peerCount)]
    pub fn peer_count(&self) -> usize {
        self.state.borrow().peers.len()
    }

    #[wasm_bindgen(js_name = openPeerCount)]
    pub fn open_peer_count(&self) -> usize {
        self.state
            .borrow()
            .peers
            .values()
            .filter(|peer| peer.channel.as_ref().is_some_and(mesh_channel_is_open))
            .count()
    }

    #[wasm_bindgen(js_name = inflightCount)]
    pub fn inflight_count(&self) -> usize {
        self.state.borrow().inflight.len()
    }

    #[wasm_bindgen(js_name = publishApplication)]
    pub fn publish_application(
        &self,
        message: JsValue,
        target: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let message = application_message_from_js(message)?;
        let target = route_target_from_optional_js(target)?;
        publish_application_mesh_state(&self.state, message, target).and_then(|route| to_js(&route))
    }

    #[wasm_bindgen(js_name = sendApplication)]
    pub fn send_application(
        &self,
        namespace: String,
        protocol: String,
        topic: Option<String>,
        body: JsValue,
        metadata: Option<JsValue>,
        target: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let body = json_value_from_js(body)?;
        let metadata = metadata_map_from_js(metadata)?;
        let target = route_target_from_optional_js(target)?;
        publish_application_mesh_state(
            &self.state,
            ApplicationRouteMessage::new(namespace, protocol, topic, body, metadata),
            target,
        )
        .and_then(|route| to_js(&route))
    }

    #[wasm_bindgen(js_name = subscribeApplications)]
    pub fn subscribe_applications(
        &self,
        filter: Option<JsValue>,
    ) -> std::result::Result<WasmApplicationRouteSubscription, JsValue> {
        Ok(WasmApplicationRouteSubscription {
            inner: Some(
                self.state
                    .borrow()
                    .applications
                    .subscribe(application_filter_from_js(filter)?),
            ),
        })
    }

    #[wasm_bindgen(js_name = recordsFanIn)]
    pub async fn records_fan_in(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<JsValue, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        let result = records_fan_in_mesh_state(&self.state, scan, policy).await?;
        to_js(&result)
    }

    #[wasm_bindgen(js_name = watchGet)]
    pub fn watch_get(
        &self,
        path: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Get { path },
        )
    }

    #[wasm_bindgen(js_name = watchMap)]
    pub fn watch_map(
        &self,
        path: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Map { path },
        )
    }

    #[wasm_bindgen(js_name = watchQuery)]
    pub fn watch_query(
        &self,
        path: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Query { path, spec },
        )
    }

    #[wasm_bindgen(js_name = watchLex)]
    pub fn watch_lex(
        &self,
        path: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Lex { path, spec },
        )
    }

    #[wasm_bindgen(js_name = watchRecords)]
    pub fn watch_records(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Records { scan },
        )
    }

    #[wasm_bindgen(js_name = watchRecordsFanIn)]
    pub fn watch_records_fan_in(
        &self,
        scan: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteFanInWatch, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        watch_records_fan_in_mesh_state(&self.state, scan, policy)
    }

    #[wasm_bindgen(js_name = watchVectorSearch)]
    pub fn watch_vector_search(
        &self,
        collection: String,
        query: JsValue,
        spec: JsValue,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            },
        )
    }

    #[wasm_bindgen(js_name = watchNode)]
    pub fn watch_node(
        &self,
        id: String,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(&self.state, policy, PullRequestKind::Node { id })
    }

    #[wasm_bindgen(js_name = watchSnapshot)]
    pub fn watch_snapshot(
        &self,
        root: Option<String>,
        policy: Option<JsValue>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let policy = remote_policy_from_js(policy)?;
        start_mesh_remote_watch_with_policy_state(
            &self.state,
            policy,
            PullRequestKind::Snapshot { root },
        )
    }

    #[wasm_bindgen(js_name = watchRemoteGet)]
    pub fn watch_remote_get(
        &self,
        peer_id: String,
        path: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Get { path })
    }

    #[wasm_bindgen(js_name = watchRemoteMap)]
    pub fn watch_remote_map(
        &self,
        peer_id: String,
        path: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Map { path })
    }

    #[wasm_bindgen(js_name = watchRemoteQuery)]
    pub fn watch_remote_query(
        &self,
        peer_id: String,
        path: JsValue,
        spec: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Query { path, spec })
    }

    #[wasm_bindgen(js_name = watchRemoteLex)]
    pub fn watch_remote_lex(
        &self,
        peer_id: String,
        path: JsValue,
        spec: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let path: RemotePath = serde_wasm_bindgen::from_value(path)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Lex { path, spec })
    }

    #[wasm_bindgen(js_name = watchRemoteRecords)]
    pub fn watch_remote_records(
        &self,
        peer_id: String,
        scan: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let scan: RecordScan = serde_wasm_bindgen::from_value(scan)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Records { scan })
    }

    #[wasm_bindgen(js_name = watchRemoteVectorSearch)]
    pub fn watch_remote_vector_search(
        &self,
        peer_id: String,
        collection: String,
        query: JsValue,
        spec: JsValue,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        let query: Vec<f32> = serde_wasm_bindgen::from_value(query)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let spec: VectorSearchSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        start_mesh_remote_watch_state(
            &self.state,
            peer_id,
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            },
        )
    }

    #[wasm_bindgen(js_name = watchRemoteNode)]
    pub fn watch_remote_node(
        &self,
        peer_id: String,
        id: String,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Node { id })
    }

    #[wasm_bindgen(js_name = watchRemoteSnapshot)]
    pub fn watch_remote_snapshot(
        &self,
        peer_id: String,
        root: Option<String>,
    ) -> std::result::Result<WasmRemoteWatch, JsValue> {
        start_mesh_remote_watch_state(&self.state, peer_id, PullRequestKind::Snapshot { root })
    }

    #[wasm_bindgen(js_name = flushPending)]
    pub fn flush_pending(&self) -> std::result::Result<usize, JsValue> {
        flush_mesh_pending_state(&self.state)
    }

    #[wasm_bindgen(js_name = retryInflight)]
    pub fn retry_inflight(&self) -> std::result::Result<usize, JsValue> {
        retry_mesh_inflight_state(&self.state)
    }

    pub fn close(&mut self) -> std::result::Result<(), JsValue> {
        self.teardown()
    }
}

impl WasmWebRtcMesh {
    fn teardown(&mut self) -> std::result::Result<(), JsValue> {
        if let Some(id) = self.node_fetch_registration.take() {
            self.state.borrow().db.unregister_node_fetch_scheduler(id);
        }
        let _ = post_mesh_leave_signal_state(&self.state);
        if let Some(interval_id) = self.retry_interval_id.take() {
            browser_window()?.clear_interval_with_handle(interval_id);
        }
        match &self.state.borrow().signaling {
            MeshSignalingTransport::BroadcastChannel(signaling) => {
                if let Some(onmessage) = &self.signaling_onmessage {
                    signaling.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
                }
                signaling.set_onmessage(None);
            }
            MeshSignalingTransport::Relay { socket, .. } => {
                socket.set_onmessage(None);
                socket.set_onopen(None);
                socket.set_onclose(None);
                socket.set_onerror(None);
                let _ = socket.close();
            }
            MeshSignalingTransport::External { .. } => {}
        }
        {
            let mut state = self.state.borrow_mut();
            state.relay_onmessage.take();
            state.relay_onopen.take();
            state.relay_onclose.take();
            state.relay_onerror.take();
        }
        {
            let mut state = self.state.borrow_mut();
            for peer in state.peers.values_mut() {
                peer.connection.set_onicecandidate(None);
                peer.connection.set_ondatachannel(None);
                if let Some(channel) = &peer.channel {
                    channel.set_onmessage(None);
                    channel.set_onopen(None);
                    channel.set_onclose(None);
                    let _ = channel.close();
                }
                let _ = peer.connection.close();
            }
            state.peers.clear();
        }
        self.change_subscription.take();
        self.signaling_onmessage.take();
        self.retry_callback.take();
        fail_outgoing_mesh_watches_state(&self.state, "mesh closed");
        clear_incoming_mesh_watches_state(&self.state);
        Ok(())
    }
}

impl Drop for WasmWebRtcMesh {
    fn drop(&mut self) {
        let _ = self.teardown();
    }
}

fn handle_websocket_message(
    state: &Rc<RefCell<WebSocketSyncState>>,
    payload: &str,
) -> std::result::Result<(), JsValue> {
    if let Ok(route) = serde_json::from_str::<RouteEnvelope>(payload) {
        return handle_route_message(state, route);
    }

    let frame: SyncFrame =
        serde_json::from_str(payload).map_err(|error| JsValue::from_str(&error.to_string()))?;
    handle_sync_frame(state, frame, None)
}

fn handle_route_message(
    state: &Rc<RefCell<WebSocketSyncState>>,
    route: RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let decision = {
        let borrowed = state.borrow();
        borrowed.router.accept(route.clone())
    };
    if !decision.deliver {
        return Ok(());
    }
    handle_route_payload(state, route)
}

fn handle_route_payload(
    state: &Rc<RefCell<WebSocketSyncState>>,
    route: RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let from = route.from;
    let route_id = route.route_id;
    let channel = route.channel;
    let target = route.target;
    let issued_at_millis = route.issued_at_millis;
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                maybe_send_auth_challenge_state(state, &peer)?;
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity_for_peer_state(state, &peer.peer_id).is_none()
                {
                    continue;
                }
                let verified_identity = verified_identity_for_peer_state(state, &peer.peer_id);
                let _ = accept_relay_peer_state(state, peer, verified_identity.as_ref())?;
            }
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let verified_identity = verified_identity_for_peer_state(state, &from);
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    continue;
                }
                let decision = state
                    .borrow()
                    .db
                    .serve_pull_request_for_peer(
                        &from,
                        HookTransport::Relay,
                        &format!("snapshot:{route_id}"),
                        &PullRequestKind::Snapshot { root: root.clone() },
                        verified_identity.as_ref(),
                    )
                    .map_err(to_js_error)?;
                if let crate::HookDecision::Allow {
                    value: RemoteResult::Snapshot { snapshot },
                } = decision
                {
                    let response = {
                        let borrowed = state.borrow();
                        borrowed.router.snapshot_response(
                            root,
                            snapshot,
                            RouteTarget::Peer(from.clone()),
                        )
                    };
                    send_route_state(state, &response)?;
                }
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state
                    .borrow()
                    .db
                    .load_snapshot(snapshot)
                    .map_err(to_js_error)?;
            }
            RoutePayload::Sync { encoding, payload } => {
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity_for_peer_state(state, &from).is_none()
                {
                    continue;
                }
                let frame = {
                    let borrowed = state.borrow();
                    decode_sync_payload(&borrowed.db, &encoding, payload)?
                };
                handle_sync_frame(state, frame, Some(from.clone()))?;
            }
            RoutePayload::PullRequest { request } => {
                let (router, db, batch_size) = {
                    let borrowed = state.borrow();
                    (
                        borrowed.router.clone(),
                        borrowed.db.clone(),
                        borrowed.db.limits().max_batch_items_per_route.max(1),
                    )
                };
                let verified_identity = verified_identity_for_peer_state(state, &from);
                let items = if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    vec![RouteBatchItem::PullResponse {
                        response: error_pull_response(
                            &request.request_id,
                            "peer is not authenticated",
                        ),
                    }]
                } else {
                    match db
                        .serve_pull_request_for_peer(
                            &from,
                            HookTransport::Relay,
                            &request.request_id,
                            &request.request,
                            verified_identity.as_ref(),
                        )
                        .map_err(to_js_error)?
                    {
                        crate::HookDecision::Allow { value } => db
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
                    &router,
                    RouteTarget::Peer(from.clone()),
                    Some(route_id.clone()),
                    items,
                    batch_size,
                ) {
                    send_route_state(state, &route)?;
                }
            }
            RoutePayload::PullResponse { response } => {
                accept_pull_response_state(state, response)?;
            }
            RoutePayload::WatchRequest { request } => {
                handle_watch_request_state(state, &from, request)?;
            }
            RoutePayload::WatchEvent { event } => {
                accept_watch_event_state(state, event)?;
            }
            RoutePayload::Application { message } => {
                let verified_identity = verified_identity_for_peer_state(state, &from);
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    continue;
                }
                let event = ApplicationRouteEvent {
                    route_id: route_id.clone(),
                    from: from.clone(),
                    channel: channel.clone(),
                    target: target.clone(),
                    issued_at_millis,
                    received_at_millis: js_sys::Date::now() as u64,
                    transport: RouteTransportKind::WebSocket,
                    verified_identity,
                    message,
                };
                state.borrow().applications.publish(event);
            }
            RoutePayload::PeerExchange { peers } => {
                for recommendation in peers {
                    maybe_send_auth_challenge_state(state, &recommendation.peer)?;
                    if state.borrow().session_auth.require_authenticated_peers
                        && verified_identity_for_peer_state(state, &recommendation.peer.peer_id)
                            .is_none()
                    {
                        continue;
                    }
                    let verified_identity =
                        verified_identity_for_peer_state(state, &recommendation.peer.peer_id);
                    let _ = accept_relay_recommendation_state(
                        state,
                        recommendation,
                        verified_identity.as_ref(),
                    )?;
                }
            }
            RoutePayload::AuthChallenge { challenge } => {
                handle_auth_challenge_state(state, challenge)?;
            }
            RoutePayload::AuthResponse { response } => {
                handle_auth_response_state(state, response)?;
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

fn verified_identity_for_peer_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    peer_id: &str,
) -> Option<VerifiedIdentity> {
    state.borrow().verified_identities.get(peer_id).cloned()
}

fn remove_relay_peer_state(state: &Rc<RefCell<WebSocketSyncState>>, peer_id: &str) {
    state.borrow().router.forget_peer(peer_id);
    let mut borrowed = state.borrow_mut();
    borrowed.recommendations.remove(peer_id);
    borrowed.verified_identities.remove(peer_id);
}

fn accept_relay_peer_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    peer: crate::PeerPresence,
    verified_identity: Option<&VerifiedIdentity>,
) -> std::result::Result<bool, JsValue> {
    let relay_url = peer.metadata.get("relay_url").cloned();
    let allowed = state
        .borrow()
        .db
        .allow_peer_connection(&crate::ConnectHookContext {
            peer: peer.clone(),
            transport: HookTransport::Relay,
            relay_url,
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if allowed.is_err() {
        remove_relay_peer_state(state, &peer.peer_id);
        return Ok(false);
    }
    let recommendation = peer_recommendation_from_presence(&peer);
    let peer_id = recommendation.peer.peer_id.clone();
    store_peer_recommendations_state(state, vec![recommendation]);
    let _ = replay_outgoing_watches_state(state, &peer_id);
    Ok(true)
}

fn accept_relay_recommendation_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    recommendation: PeerRecommendation,
    verified_identity: Option<&VerifiedIdentity>,
) -> std::result::Result<bool, JsValue> {
    let relay_url = recommendation.relay_urls.first().cloned();
    let allowed = state
        .borrow()
        .db
        .allow_peer_connection(&crate::ConnectHookContext {
            peer: recommendation.peer.clone(),
            transport: HookTransport::Relay,
            relay_url,
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if allowed.is_err() {
        remove_relay_peer_state(state, &recommendation.peer.peer_id);
        return Ok(false);
    }
    let peer_id = recommendation.peer.peer_id.clone();
    store_peer_recommendations_state(state, vec![recommendation]);
    let _ = replay_outgoing_watches_state(state, &peer_id);
    Ok(true)
}

fn maybe_send_auth_challenge_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    peer: &crate::PeerPresence,
) -> std::result::Result<(), JsValue> {
    let should_skip = {
        let borrowed = state.borrow();
        peer.peer_id == borrowed.router.peer_id()
            || borrowed.verified_identities.contains_key(&peer.peer_id)
    };
    if should_skip {
        return Ok(());
    }
    let Some(identity) = peer.identity.as_ref() else {
        if !state.borrow().session_auth.allow_unauthenticated_presence {
            remove_relay_peer_state(state, &peer.peer_id);
        }
        return Ok(());
    };

    #[cfg(feature = "crypto")]
    {
        let (challenge, route) = {
            let borrowed = state.borrow();
            let challenge = crate::session_auth::create_auth_challenge(
                borrowed.router.peer_id(),
                &borrowed.db.replica_id(),
                &borrowed.session_id,
                &peer.peer_id,
                &peer.replica_id,
                identity,
                "relay",
                &borrowed.session_auth,
            );
            let route = borrowed
                .router
                .auth_challenge(challenge.clone(), RouteTarget::Peer(peer.peer_id.clone()));
            (challenge, route)
        };
        send_route_state(state, &route)?;
        let mut borrowed = state.borrow_mut();
        borrowed
            .pending_auth_challenges
            .insert(challenge.challenge_id.clone(), challenge.clone());
        borrowed
            .pending_auth_peers
            .insert(challenge.challenge_id.clone(), peer.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = identity;
    }

    Ok(())
}

fn handle_auth_challenge_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    challenge: crate::AuthChallenge,
) -> std::result::Result<(), JsValue> {
    if challenge.target_peer_id != state.borrow().router.peer_id() {
        return Ok(());
    }
    let response = {
        let borrowed = state.borrow();
        borrowed
            .db
            .sign_session_auth_response(
                &challenge,
                borrowed.router.peer_id(),
                &borrowed.session_id,
                &borrowed.session_auth,
            )
            .map_err(to_js_error)?
    };
    let Some(response) = response else {
        return Ok(());
    };
    let route = state
        .borrow()
        .router
        .auth_response(response, RouteTarget::Peer(challenge.issuer_peer_id));
    send_route_state(state, &route)
}

fn handle_auth_response_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    response: crate::AuthResponse,
) -> std::result::Result<(), JsValue> {
    let Some(challenge) = state
        .borrow_mut()
        .pending_auth_challenges
        .remove(&response.challenge_id)
    else {
        return Ok(());
    };
    let peer = state
        .borrow_mut()
        .pending_auth_peers
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
    let verified = {
        let borrowed = state.borrow();
        crate::session_auth::verify_auth_response(&challenge, &response, &borrowed.session_auth)
            .map_err(to_js_error)?
    };
    state
        .borrow_mut()
        .verified_identities
        .insert(verified.peer_id.clone(), verified.clone());
    if !accept_relay_peer_state(state, peer, Some(&verified))? {
        state
            .borrow_mut()
            .verified_identities
            .remove(&verified.peer_id);
    }
    Ok(())
}

fn handle_sync_frame(
    state: &Rc<RefCell<WebSocketSyncState>>,
    frame: SyncFrame,
    reply_peer: Option<String>,
) -> std::result::Result<(), JsValue> {
    match frame {
        SyncFrame::Sync {
            from,
            message_id,
            ops,
        } => {
            if state.borrow().session_auth.require_authenticated_peers {
                let authenticated = reply_peer
                    .as_ref()
                    .and_then(|peer_id| verified_identity_for_peer_state(state, peer_id))
                    .is_some();
                if !authenticated {
                    return Ok(());
                }
            }
            let db = state.borrow().db.clone();
            let applied = db
                .apply_sync_envelope(SyncEnvelope { from, ops })
                .map_err(to_js_error)?;
            let ack = SyncFrame::Ack {
                from: db.replica_id(),
                message_id,
                applied,
            };
            send_sync_frame_state(
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
            state.borrow_mut().inflight.remove(&message_id);
            let _ = flush_pending_state(state);
        }
    }
    Ok(())
}

fn flush_pending_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
) -> std::result::Result<usize, JsValue> {
    let (db, socket, replica_id) = {
        let state = state.borrow();
        (
            state.db.clone(),
            state.socket.clone(),
            state.db.replica_id(),
        )
    };

    if socket.ready_state() != web_sys::WebSocket::OPEN {
        return Ok(0);
    }

    let mut envelope = db.drain_sync_envelope().map_err(to_js_error)?;
    if envelope.ops.is_empty() {
        return Ok(0);
    }

    let max_ops = db.limits().max_ops_per_message.max(1);
    if envelope.ops.len() > max_ops {
        let remainder = envelope.ops.split_off(max_ops);
        let _ = db.requeue_pending_operations(remainder);
    }

    let count = envelope.ops.len();

    let message_id = {
        let mut state = state.borrow_mut();
        state.next_message_seq = state.next_message_seq.saturating_add(1);
        format!("{replica_id}/ws/{:x}", state.next_message_seq)
    };

    let frame = SyncFrame::Sync {
        from: envelope.from.clone(),
        message_id: message_id.clone(),
        ops: envelope.ops.clone(),
    };
    let (encoding, payload) = encode_sync_payload(&db, frame)?;
    let outbound = OutboundSync {
        encoding: encoding.clone(),
        payload: payload.clone(),
        target: RouteTarget::Broadcast,
    };
    let route = {
        let borrowed = state.borrow();
        borrowed
            .router
            .wrap_sync(encoding, payload, RouteTarget::Broadcast)
    };
    if let Err(error) = send_route_state(state, &route) {
        let _ = db.requeue_pending_operations(envelope.ops);
        return Err(error);
    }

    state.borrow_mut().inflight.insert(message_id, outbound);
    Ok(count)
}

fn retry_inflight_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
) -> std::result::Result<usize, JsValue> {
    let routes = {
        let state = state.borrow();
        if state.socket.ready_state() != web_sys::WebSocket::OPEN {
            return Ok(0);
        }
        state
            .inflight
            .iter()
            .map(|(_, outbound)| {
                state.router.wrap_sync(
                    outbound.encoding.clone(),
                    outbound.payload.clone(),
                    outbound.target.clone(),
                )
            })
            .collect::<Vec<_>>()
    };

    for route in &routes {
        send_route_state(state, route)?;
    }

    Ok(routes.len())
}

fn requeue_inflight_state(state: &Rc<RefCell<WebSocketSyncState>>) {
    let (db, inflight) = {
        let mut state = state.borrow_mut();
        let inflight = std::mem::take(&mut state.inflight);
        (state.db.clone(), inflight)
    };
    for outbound in inflight.into_values() {
        if let Ok(frame) = decode_sync_payload(&db, &outbound.encoding, outbound.payload) {
            if let SyncFrame::Sync { ops, .. } = frame {
                let _ = db.requeue_pending_operations(ops);
            }
        }
    }
}

fn send_sync_frame_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    frame: SyncFrame,
    target: RouteTarget,
) -> std::result::Result<(), JsValue> {
    let db = state.borrow().db.clone();
    let (encoding, payload) = encode_sync_payload(&db, frame)?;
    let route = state.borrow().router.wrap_sync(encoding, payload, target);
    send_route_state(state, &route)
}

fn send_route_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    route: &RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let (socket, max_bytes) = {
        let borrowed = state.borrow();
        (
            borrowed.socket.clone(),
            borrowed.db.limits().max_route_payload_bytes,
        )
    };
    send_websocket_route(&socket, max_bytes, route)
}

fn send_websocket_route(
    socket: &web_sys::WebSocket,
    max_bytes: usize,
    route: &RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let payload = serde_json::to_string(route).map_err(to_js_error)?;
    if payload.len() > max_bytes {
        return Err(JsValue::from_str(&format!(
            "route payload exceeds {max_bytes} bytes"
        )));
    }
    socket.send_with_str(&payload)
}

async fn request_remote_result_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    target_peer_id: String,
    request_kind: PullRequestKind,
) -> std::result::Result<RemoteResult, JsValue> {
    let (request_id, receiver) = {
        let mut borrowed = state.borrow_mut();
        if borrowed.socket.ready_state() != web_sys::WebSocket::OPEN {
            return Err(JsValue::from_str("websocket is not connected"));
        }
        borrowed.next_message_seq = borrowed.next_message_seq.saturating_add(1);
        let request_id = format!(
            "{}/pull/{:x}",
            borrowed.db.replica_id(),
            borrowed.next_message_seq
        );
        let (sender, receiver) = bounded(1);
        borrowed.pending_requests.insert(
            request_id.clone(),
            PendingPullRequest {
                sender,
                accumulator: PullAccumulator::new(&request_kind),
            },
        );
        (request_id, receiver)
    };

    let request = PullRequest {
        request_id: request_id.clone(),
        request: request_kind,
    };
    let route = state
        .borrow()
        .router
        .wrap_pull_request(request, RouteTarget::Peer(target_peer_id));
    if let Err(error) = send_route_state(state, &route) {
        state.borrow_mut().pending_requests.remove(&request_id);
        return Err(error);
    }

    let result = receiver
        .recv()
        .await
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    result.map_err(|message| JsValue::from_str(&message))
}

async fn request_remote_result_with_policy_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    policy: RemoteInterestPolicy,
    request_kind: PullRequestKind,
) -> std::result::Result<RemoteResult, JsValue> {
    let capability = pull_capability_for_request(&request_kind);
    let peer_id =
        select_relay_peer_for_policy_state(state, &policy, capability, Some(&request_kind))?;
    request_remote_result_state(state, peer_id, request_kind).await
}

async fn records_fan_in_relay_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    scan: RecordScan,
    policy: RemoteInterestPolicy,
) -> std::result::Result<crate::RemoteRecordsFanIn, JsValue> {
    let request_kind = PullRequestKind::Records { scan: scan.clone() };
    let peers = resolve_relay_peers_for_policy_state(
        state,
        &policy,
        Some("pull_records"),
        Some(&request_kind),
    )?;
    if peers.is_empty() {
        return Err(JsValue::from_str(
            "remote interest policy did not select any peers",
        ));
    }
    let request_id = next_route_request_id_state(state, "records-fan-in");
    let mut records = Vec::new();
    let mut failures = Vec::new();
    for peer in peers {
        let transport = route_transport_for_peer(&peer);
        match request_remote_result_state(
            state,
            peer.peer_id.clone(),
            PullRequestKind::Records { scan: scan.clone() },
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
                message: error.as_string().unwrap_or_else(|| format!("{error:?}")),
            }),
        }
    }
    Ok(merge_remote_records_fan_in(request_id, records, failures))
}

fn watch_records_fan_in_relay_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    scan: RecordScan,
    policy: RemoteInterestPolicy,
) -> std::result::Result<WasmRemoteFanInWatch, JsValue> {
    let request_kind = PullRequestKind::Records { scan: scan.clone() };
    let peers = resolve_relay_peers_for_policy_state(
        state,
        &policy,
        Some("watch_records"),
        Some(&request_kind),
    )?;
    if peers.is_empty() {
        return Err(JsValue::from_str(
            "remote interest policy did not select any peers",
        ));
    }

    let (sender, receiver) = unbounded();
    let watches = Rc::new(RefCell::new(Vec::<WasmRemoteWatch>::new()));
    for peer in peers {
        let transport = route_transport_for_peer(&peer);
        match start_remote_watch_state(
            state,
            peer.peer_id.clone(),
            PullRequestKind::Records { scan: scan.clone() },
        ) {
            Ok(watch) => {
                let Some(child_receiver) = watch.inner.as_ref().map(|inner| inner.receiver.clone())
                else {
                    continue;
                };
                watches.borrow_mut().push(watch);
                let child_sender = sender.clone();
                let peer_id = peer.peer_id;
                spawn_local(async move {
                    let mut sequence = 0_u64;
                    while let Ok(message) = child_receiver.recv().await {
                        let event = match message {
                            Ok(message) => {
                                sequence = sequence.saturating_add(1);
                                RemoteFanInWatchEvent::Update {
                                    peer_id: peer_id.clone(),
                                    transport: transport.clone(),
                                    initial: message.initial,
                                    sequence,
                                    result: message.result,
                                }
                            }
                            Err(message) => RemoteFanInWatchEvent::Failure {
                                peer_id: peer_id.clone(),
                                transport: transport.clone(),
                                message,
                                terminal: false,
                            },
                        };
                        if child_sender.send(event).await.is_err() {
                            break;
                        }
                    }
                });
            }
            Err(error) => {
                let _ = sender.try_send(RemoteFanInWatchEvent::Failure {
                    peer_id: peer.peer_id,
                    transport,
                    message: error.as_string().unwrap_or_else(|| format!("{error:?}")),
                    terminal: true,
                });
            }
        }
    }
    let close_sender = sender.clone();
    Ok(WasmRemoteFanInWatch {
        inner: Some(WasmRemoteFanInWatchInner {
            receiver,
            close: Box::new(move || {
                for watch in watches.borrow_mut().iter_mut() {
                    watch.cancel();
                }
                close_sender.close();
            }),
        }),
    })
}

fn publish_application_relay_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    message: ApplicationRouteMessage,
    target: RouteTarget,
) -> std::result::Result<RouteEnvelope, JsValue> {
    let route = state
        .borrow()
        .router
        .wrap_application(message, target, None);
    send_route_state(state, &route)?;
    Ok(route)
}

fn publish_application_mesh_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    message: ApplicationRouteMessage,
    target: RouteTarget,
) -> std::result::Result<RouteEnvelope, JsValue> {
    let route = state
        .borrow()
        .router
        .wrap_application(message, target, None);
    send_mesh_application_route_state(state, &route)?;
    Ok(route)
}

fn send_mesh_application_route_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    route: &RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    match &route.target {
        RouteTarget::Peer(peer_id) => {
            if send_mesh_route_to_peer(state, peer_id, route).is_ok() {
                return Ok(());
            }
            send_mesh_signal_route_state(state, route)
        }
        RouteTarget::Broadcast | RouteTarget::Topic(_) => {
            let peer_ids = state.borrow().peers.keys().cloned().collect::<Vec<_>>();
            for peer_id in peer_ids {
                let _ = send_mesh_route_to_peer(state, &peer_id, route);
            }
            send_mesh_signal_route_state(state, route)
        }
    }
}

async fn records_fan_in_mesh_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    scan: RecordScan,
    policy: RemoteInterestPolicy,
) -> std::result::Result<crate::RemoteRecordsFanIn, JsValue> {
    let request_kind = PullRequestKind::Records { scan: scan.clone() };
    let peers = resolve_mesh_peers_for_policy_state(
        state,
        &policy,
        Some("watch_records"),
        Some(&request_kind),
    )?;
    if peers.is_empty() {
        return Err(JsValue::from_str(
            "remote interest policy did not select any peers",
        ));
    }

    let request_id = next_mesh_request_id_state(state, "records-fan-in");
    let mut records = Vec::new();
    let mut failures = Vec::new();
    for peer in peers {
        let transport = route_transport_for_peer(&peer);
        match start_mesh_remote_watch_state(
            state,
            peer.peer_id.clone(),
            PullRequestKind::Records { scan: scan.clone() },
        ) {
            Ok(mut watch) => {
                let child_receiver = watch.inner.as_ref().map(|inner| inner.receiver.clone());
                let message = match child_receiver {
                    Some(receiver) => receiver.recv().await.ok(),
                    None => None,
                };
                watch.cancel();
                match message {
                    Some(Ok(RemoteWatchMessage {
                        result: RemoteResult::Records { result },
                        ..
                    })) => records.push(RemotePeerRecords {
                        peer_id: peer.peer_id,
                        transport,
                        result,
                    }),
                    Some(Ok(message)) => failures.push(RemotePeerFailure {
                        peer_id: peer.peer_id,
                        transport,
                        message: format!("expected records result, received {:?}", message.result),
                    }),
                    Some(Err(message)) => failures.push(RemotePeerFailure {
                        peer_id: peer.peer_id,
                        transport,
                        message,
                    }),
                    None => failures.push(RemotePeerFailure {
                        peer_id: peer.peer_id,
                        transport,
                        message: "watch closed before records result".to_owned(),
                    }),
                }
            }
            Err(error) => failures.push(RemotePeerFailure {
                peer_id: peer.peer_id,
                transport,
                message: error.as_string().unwrap_or_else(|| format!("{error:?}")),
            }),
        }
    }
    Ok(merge_remote_records_fan_in(request_id, records, failures))
}

fn watch_records_fan_in_mesh_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    scan: RecordScan,
    policy: RemoteInterestPolicy,
) -> std::result::Result<WasmRemoteFanInWatch, JsValue> {
    let request_kind = PullRequestKind::Records { scan: scan.clone() };
    let peers = resolve_mesh_peers_for_policy_state(
        state,
        &policy,
        Some("watch_records"),
        Some(&request_kind),
    )?;
    if peers.is_empty() {
        return Err(JsValue::from_str(
            "remote interest policy did not select any peers",
        ));
    }

    let (sender, receiver) = unbounded();
    let watches = Rc::new(RefCell::new(Vec::<WasmRemoteWatch>::new()));
    for peer in peers {
        let transport = route_transport_for_peer(&peer);
        match start_mesh_remote_watch_state(
            state,
            peer.peer_id.clone(),
            PullRequestKind::Records { scan: scan.clone() },
        ) {
            Ok(watch) => {
                let Some(child_receiver) = watch.inner.as_ref().map(|inner| inner.receiver.clone())
                else {
                    continue;
                };
                watches.borrow_mut().push(watch);
                let child_sender = sender.clone();
                let peer_id = peer.peer_id;
                spawn_local(async move {
                    let mut sequence = 0_u64;
                    while let Ok(message) = child_receiver.recv().await {
                        let event = match message {
                            Ok(message) => {
                                sequence = sequence.saturating_add(1);
                                RemoteFanInWatchEvent::Update {
                                    peer_id: peer_id.clone(),
                                    transport: transport.clone(),
                                    initial: message.initial,
                                    sequence,
                                    result: message.result,
                                }
                            }
                            Err(message) => RemoteFanInWatchEvent::Failure {
                                peer_id: peer_id.clone(),
                                transport: transport.clone(),
                                message,
                                terminal: false,
                            },
                        };
                        if child_sender.send(event).await.is_err() {
                            break;
                        }
                    }
                });
            }
            Err(error) => {
                let _ = sender.try_send(RemoteFanInWatchEvent::Failure {
                    peer_id: peer.peer_id,
                    transport,
                    message: error.as_string().unwrap_or_else(|| format!("{error:?}")),
                    terminal: true,
                });
            }
        }
    }
    let close_sender = sender.clone();
    Ok(WasmRemoteFanInWatch {
        inner: Some(WasmRemoteFanInWatchInner {
            receiver,
            close: Box::new(move || {
                for watch in watches.borrow_mut().iter_mut() {
                    watch.cancel();
                }
                close_sender.close();
            }),
        }),
    })
}

fn next_route_request_id_state(state: &Rc<RefCell<WebSocketSyncState>>, purpose: &str) -> String {
    let mut borrowed = state.borrow_mut();
    borrowed.next_message_seq = borrowed.next_message_seq.saturating_add(1);
    format!(
        "{}/{}/{:x}",
        borrowed.router.peer_id(),
        purpose,
        borrowed.next_message_seq
    )
}

fn next_mesh_request_id_state(state: &Rc<RefCell<WebRtcMeshState>>, purpose: &str) -> String {
    let mut borrowed = state.borrow_mut();
    borrowed.next_message_seq = borrowed.next_message_seq.saturating_add(1);
    format!(
        "{}/{}/{:x}",
        borrowed.peer_id, purpose, borrowed.next_message_seq
    )
}

fn start_remote_watch_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    target_peer_id: String,
    request_kind: PullRequestKind,
) -> std::result::Result<WasmRemoteWatch, JsValue> {
    let limit = state.borrow().db.limits().max_active_remote_watches.max(1);
    let (watch_id, receiver) = {
        let mut borrowed = state.borrow_mut();
        if borrowed.socket.ready_state() != web_sys::WebSocket::OPEN {
            return Err(JsValue::from_str("websocket is not connected"));
        }
        if borrowed.outgoing_watches.len() >= limit {
            return Err(JsValue::from_str(&format!(
                "too many active remote watches (limit {limit})"
            )));
        }
        borrowed.next_message_seq = borrowed.next_message_seq.saturating_add(1);
        let watch_id = format!(
            "{}/watch/{:x}",
            borrowed.db.replica_id(),
            borrowed.next_message_seq
        );
        let (sender, receiver) = unbounded();
        borrowed.outgoing_watches.insert(
            watch_id.clone(),
            OutgoingWatch {
                sender,
                target_peer_id: target_peer_id.clone(),
                request_kind: request_kind.clone(),
                pending_sequence: None,
                last_delivered_sequence: None,
            },
        );
        (watch_id, receiver)
    };

    if let Err(error) = send_watch_request_state(
        state,
        &target_peer_id,
        WatchRequest {
            watch_id: watch_id.clone(),
            request: WatchRequestKind::Subscribe {
                request: request_kind,
            },
        },
    ) {
        state.borrow_mut().outgoing_watches.remove(&watch_id);
        return Err(error);
    }

    let cancel_state = state.clone();
    Ok(WasmRemoteWatch {
        inner: Some(WasmRemoteWatchInner {
            receiver,
            cancel: Box::new(move || {
                cancel_remote_watch_state(&cancel_state, &watch_id);
            }),
        }),
    })
}

fn start_remote_watch_with_policy_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    policy: RemoteInterestPolicy,
    request_kind: PullRequestKind,
) -> std::result::Result<WasmRemoteWatch, JsValue> {
    let capability = format!("watch_{}", request_kind.kind_name());
    let peer_id =
        select_relay_peer_for_policy_state(state, &policy, Some(&capability), Some(&request_kind))?;
    start_remote_watch_state(state, peer_id, request_kind)
}

fn application_message_from_js(
    message: JsValue,
) -> std::result::Result<ApplicationRouteMessage, JsValue> {
    serde_wasm_bindgen::from_value(message).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn application_filter_from_js(
    filter: Option<JsValue>,
) -> std::result::Result<ApplicationRouteFilter, JsValue> {
    match filter {
        Some(value) if !value.is_undefined() && !value.is_null() => {
            serde_wasm_bindgen::from_value(value)
                .map_err(|error| JsValue::from_str(&error.to_string()))
        }
        _ => Ok(ApplicationRouteFilter::default()),
    }
}

fn route_target_from_optional_js(
    target: Option<JsValue>,
) -> std::result::Result<RouteTarget, JsValue> {
    match target {
        Some(value) if !value.is_undefined() && !value.is_null() => {
            serde_wasm_bindgen::from_value(value)
                .map_err(|error| JsValue::from_str(&error.to_string()))
        }
        _ => Ok(RouteTarget::Broadcast),
    }
}

fn json_value_from_js(value: JsValue) -> std::result::Result<JsonValue, JsValue> {
    if value.is_undefined() {
        Ok(JsonValue::Null)
    } else {
        serde_wasm_bindgen::from_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
    }
}

fn metadata_map_from_js(
    metadata: Option<JsValue>,
) -> std::result::Result<BTreeMap<String, JsonValue>, JsValue> {
    match metadata {
        Some(value) if !value.is_undefined() && !value.is_null() => {
            serde_wasm_bindgen::from_value(value)
                .map_err(|error| JsValue::from_str(&error.to_string()))
        }
        _ => Ok(BTreeMap::new()),
    }
}

fn remote_policy_from_js(
    policy: Option<JsValue>,
) -> std::result::Result<RemoteInterestPolicy, JsValue> {
    match policy {
        Some(value) if !value.is_undefined() && !value.is_null() => {
            serde_wasm_bindgen::from_value(value)
                .map_err(|error| JsValue::from_str(&error.to_string()))
        }
        _ => Ok(RemoteInterestPolicy::default()),
    }
}

fn select_relay_peer_for_policy_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&PullRequestKind>,
) -> std::result::Result<String, JsValue> {
    if let Some(peer) = resolve_relay_peers_for_policy_state(state, policy, capability, request)?
        .into_iter()
        .next()
    {
        return Ok(peer.peer_id);
    }
    let message = match capability {
        Some(capability) => format!("no connected peer advertises capability `{capability}`"),
        None => "no connected peer is available for remote interest".to_owned(),
    };
    Err(JsValue::from_str(&message))
}

fn resolve_relay_peers_for_policy_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&PullRequestKind>,
) -> std::result::Result<Vec<crate::PeerPresence>, JsValue> {
    if let Some(peer_ids) = explicit_policy_peers(policy)? {
        if peer_ids.is_empty() {
            return Err(JsValue::from_str(
                "remote interest policy did not include any peer ids",
            ));
        }
        let borrowed = state.borrow();
        let known = borrowed.router.known_peers();
        let mut peers = Vec::new();
        for peer_id in peer_ids {
            if let Some(peer) = known.iter().find(|peer| peer.peer_id == peer_id).cloned() {
                if !policy.require_capability || peer_supports_request(&peer, capability, request) {
                    peers.push(peer);
                }
            } else if !policy.require_capability {
                peers.push(crate::PeerPresence {
                    peer_id,
                    replica_id: String::new(),
                    transport: "websocket".to_owned(),
                    identity: None,
                    capabilities: Vec::new(),
                    topics: Vec::new(),
                    metadata: BTreeMap::new(),
                });
            }
        }
        if policy.require_capability && peers.is_empty() {
            return Err(JsValue::from_str(&format!(
                "no requested peer advertises required capability `{}`",
                capability.unwrap_or("unknown")
            )));
        }
        return Ok(peers);
    }

    let mut candidates = relay_peer_candidates_state(state);
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

fn explicit_policy_peers(
    policy: &RemoteInterestPolicy,
) -> std::result::Result<Option<Vec<String>>, JsValue> {
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
                JsValue::from_str("remote interest policy target `peer` requires peerId")
            }),
        RemoteInterestTarget::Peers => Ok(Some(policy.peers.clone())),
    }
}

fn relay_peer_candidates_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
) -> Vec<crate::PeerPresence> {
    let borrowed = state.borrow();
    let mut recommendations = borrowed
        .recommendations
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
        if !candidates
            .iter()
            .any(|peer: &crate::PeerPresence| peer.peer_id == recommendation.peer.peer_id)
        {
            candidates.push(recommendation.peer);
        }
    }
    for peer in borrowed.router.known_peers() {
        if !candidates
            .iter()
            .any(|candidate| candidate.peer_id == peer.peer_id)
        {
            candidates.push(peer);
        }
    }
    if borrowed.session_auth.require_authenticated_peers {
        candidates.retain(|peer| borrowed.verified_identities.contains_key(&peer.peer_id));
    }
    candidates
}

fn route_transport_for_peer(peer: &crate::PeerPresence) -> RouteTransportKind {
    match peer.transport.as_str() {
        "moq" => RouteTransportKind::Moq,
        "webrtc" => RouteTransportKind::WebRtc,
        "broadcast_channel" => RouteTransportKind::BroadcastChannel,
        "in_memory" => RouteTransportKind::InMemory,
        _ => RouteTransportKind::WebSocket,
    }
}

fn mesh_signaling_transport_kind_state(state: &Rc<RefCell<WebRtcMeshState>>) -> RouteTransportKind {
    match &state.borrow().signaling {
        MeshSignalingTransport::BroadcastChannel(_) => RouteTransportKind::BroadcastChannel,
        MeshSignalingTransport::Relay { .. } => RouteTransportKind::WebSocket,
        MeshSignalingTransport::External { mode, .. } => {
            if mode.contains("moq") {
                RouteTransportKind::Moq
            } else if mode.contains("websocket") || mode.contains("relay") {
                RouteTransportKind::WebSocket
            } else {
                RouteTransportKind::InMemory
            }
        }
    }
}

fn peer_supports_capability(peer: &crate::PeerPresence, capability: Option<&str>) -> bool {
    capability.is_none_or(|capability| peer.capabilities.iter().any(|item| item == capability))
}

fn peer_supports_request(
    peer: &crate::PeerPresence,
    capability: Option<&str>,
    request: Option<&PullRequestKind>,
) -> bool {
    if !peer_supports_capability(peer, capability) {
        return false;
    }
    vector_request_hint_score(peer, request) != Some(0)
}

fn prefer_vector_request_candidates(
    candidates: &mut [crate::PeerPresence],
    request: Option<&PullRequestKind>,
) {
    candidates.sort_by(|left, right| {
        vector_request_hint_score(right, request)
            .unwrap_or(1)
            .cmp(&vector_request_hint_score(left, request).unwrap_or(1))
    });
}

fn vector_request_hint_score(
    peer: &crate::PeerPresence,
    request: Option<&PullRequestKind>,
) -> Option<u8> {
    let Some(PullRequestKind::VectorSearch {
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

fn pull_capability_for_request(request: &PullRequestKind) -> Option<&'static str> {
    match request {
        PullRequestKind::Get { .. } => Some("pull_get"),
        PullRequestKind::Query { .. } => Some("pull_query"),
        PullRequestKind::Lex { .. } => Some("pull_lex"),
        PullRequestKind::Records { .. } => Some("pull_records"),
        PullRequestKind::VectorSearch { .. } => Some("pull_vector_search"),
        PullRequestKind::Snapshot { .. } => Some("snapshot"),
        PullRequestKind::Node { .. } => Some("pull_node"),
        PullRequestKind::Map { .. } => Some("pull_map"),
        PullRequestKind::Transaction { .. } => None,
    }
}

fn cancel_remote_watch_state(state: &Rc<RefCell<WebSocketSyncState>>, watch_id: &str) {
    let Some(watch) = state.borrow_mut().outgoing_watches.remove(watch_id) else {
        return;
    };
    watch.sender.close();
    let _ = send_watch_request_state(
        state,
        &watch.target_peer_id,
        WatchRequest {
            watch_id: watch_id.to_owned(),
            request: WatchRequestKind::Cancel,
        },
    );
}

fn send_watch_request_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    target_peer_id: &str,
    request: WatchRequest,
) -> std::result::Result<(), JsValue> {
    let route = state.borrow().router.wrap_watch_request(
        request,
        RouteTarget::Peer(target_peer_id.to_owned()),
        None,
    );
    send_route_state(state, &route)
}

fn replay_outgoing_watches_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    peer_id: &str,
) -> std::result::Result<usize, JsValue> {
    let requests = {
        let borrowed = state.borrow();
        borrowed
            .outgoing_watches
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
        send_watch_request_state(state, peer_id, request.clone())?;
    }
    Ok(requests.len())
}

fn fail_pending_requests_state(state: &Rc<RefCell<WebSocketSyncState>>, message: &str) {
    let pending = {
        let mut borrowed = state.borrow_mut();
        std::mem::take(&mut borrowed.pending_requests)
    };
    for request in pending.into_values() {
        let _ = request.sender.try_send(Err(message.to_owned()));
    }
}

fn fail_outgoing_watches_state(state: &Rc<RefCell<WebSocketSyncState>>, message: &str) {
    let outgoing = {
        let mut borrowed = state.borrow_mut();
        std::mem::take(&mut borrowed.outgoing_watches)
    };
    for watch in outgoing.into_values() {
        let _ = watch.sender.try_send(Err(message.to_owned()));
    }
}

fn clear_incoming_watches_state(state: &Rc<RefCell<WebSocketSyncState>>) {
    state.borrow_mut().incoming_watches.clear();
}

fn accept_pull_response_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    response: PullResponse,
) -> std::result::Result<(), JsValue> {
    let mut borrowed = state.borrow_mut();
    let Some(request) = borrowed.pending_requests.get_mut(&response.request_id) else {
        return Ok(());
    };

    if let Some(message) = apply_response_body_state(&mut request.accumulator, &response.result) {
        let request = borrowed
            .pending_requests
            .remove(&response.request_id)
            .unwrap();
        let _ = request.sender.try_send(Err(message));
        return Ok(());
    }

    if response.is_final() {
        let request = borrowed
            .pending_requests
            .remove(&response.request_id)
            .unwrap();
        let result = request.accumulator.into_result().map_err(to_js_error)?;
        let _ = request.sender.try_send(Ok(result));
    }
    Ok(())
}

fn handle_watch_request_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    from: &str,
    request: WatchRequest,
) -> std::result::Result<(), JsValue> {
    match request.request {
        WatchRequestKind::Subscribe {
            request: incoming_request_kind,
        } => {
            let verified_identity = verified_identity_for_peer_state(state, from);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                let route = state.borrow().router.wrap_watch_event(
                    error_watch_event(&request.watch_id, 0, true, "peer is not authenticated"),
                    RouteTarget::Peer(from.to_owned()),
                    None,
                );
                send_route_state(state, &route)?;
                return Ok(());
            }
            let request_kind = match state
                .borrow()
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
                    let route = state.borrow().router.wrap_watch_event(
                        error_watch_event(&request.watch_id, 0, true, message),
                        RouteTarget::Peer(from.to_owned()),
                        None,
                    );
                    send_route_state(state, &route)?;
                    return Ok(());
                }
            };
            let limit = state.borrow().db.limits().max_active_remote_watches.max(1);
            {
                let mut borrowed = state.borrow_mut();
                if borrowed.incoming_watches.len() >= limit
                    && !borrowed.incoming_watches.contains_key(&request.watch_id)
                {
                    return Err(JsValue::from_str(&format!(
                        "too many active remote watches (limit {limit})"
                    )));
                }
                let interest_path = request_kind.interest_path();
                borrowed.incoming_watches.insert(
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
            let _ = emit_single_incoming_watch_update_state(state, &request.watch_id, true)?;
        }
        WatchRequestKind::Cancel => {
            state
                .borrow_mut()
                .incoming_watches
                .remove(&request.watch_id);
        }
    }
    Ok(())
}

fn accept_watch_event_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    event: WatchEvent,
) -> std::result::Result<(), JsValue> {
    let mut deliver: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        RemoteWatchMessage,
    )> = None;
    let mut failure: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        String,
    )> = None;

    {
        let mut borrowed = state.borrow_mut();
        let Some(watch) = borrowed.outgoing_watches.get_mut(&event.watch_id) else {
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
        if let Some(message) = apply_response_body_state(&mut pending.accumulator, &event.result) {
            let sender = watch.sender.clone();
            borrowed.outgoing_watches.remove(&event.watch_id);
            failure = Some((sender, message));
        } else if event.done || event.chunk.index.saturating_add(1) >= event.chunk.total {
            let sender = watch.sender.clone();
            let pending = watch.pending_sequence.take().unwrap();
            let result = pending.accumulator.into_result().map_err(to_js_error)?;
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

fn emit_incoming_watch_updates_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    event: &crate::ChangeEvent,
) -> std::result::Result<usize, JsValue> {
    let watch_ids = {
        let borrowed = state.borrow();
        borrowed
            .incoming_watches
            .iter()
            .filter_map(|(watch_id, watch)| {
                incoming_watch_overlaps_event(watch, event).then_some(watch_id.clone())
            })
            .collect::<Vec<_>>()
    };
    let mut emitted = 0;
    for watch_id in watch_ids {
        if emit_single_incoming_watch_update_state(state, &watch_id, false)? {
            emitted += 1;
        }
    }
    Ok(emitted)
}

fn emit_single_incoming_watch_update_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    watch_id: &str,
    initial: bool,
) -> std::result::Result<bool, JsValue> {
    let Some(watch) = state.borrow().incoming_watches.get(watch_id).cloned() else {
        return Ok(false);
    };
    let verified_identity = verified_identity_for_peer_state(state, &watch.target_peer_id);
    if state.borrow().session_auth.require_authenticated_peers && verified_identity.is_none() {
        let route = state.borrow().router.wrap_watch_event(
            error_watch_event(
                watch_id,
                watch.next_sequence,
                initial,
                "peer is not authenticated",
            ),
            RouteTarget::Peer(watch.target_peer_id.clone()),
            None,
        );
        send_route_state(state, &route)?;
        state.borrow_mut().incoming_watches.remove(watch_id);
        return Ok(true);
    }
    let decision = state
        .borrow()
        .db
        .serve_watch_result_for_peer(
            &watch.target_peer_id,
            HookTransport::Relay,
            watch_id,
            &watch.request_kind,
            initial,
            verified_identity.as_ref(),
        )
        .map_err(to_js_error)?;
    let (result, content_hash, denied_message) = match decision {
        crate::HookDecision::Allow { value } => {
            let content_hash = crate::stable_content_hash(&value);
            (value, content_hash, None)
        }
        crate::HookDecision::Deny { message } => {
            (RemoteResult::Get { value: None }, None, Some(message))
        }
    };
    if let Some(message) = denied_message {
        let route = state.borrow().router.wrap_watch_event(
            error_watch_event(watch_id, watch.next_sequence, initial, message),
            RouteTarget::Peer(watch.target_peer_id.clone()),
            None,
        );
        send_route_state(state, &route)?;
        state.borrow_mut().incoming_watches.remove(watch_id);
        return Ok(true);
    }
    if !initial && content_hash == watch.last_hash {
        return Ok(false);
    }

    let items = state
        .borrow()
        .db
        .chunk_watch_result(watch_id, watch.next_sequence, initial, result)
        .into_iter()
        .map(|event| RouteBatchItem::WatchEvent { event })
        .collect::<Vec<_>>();
    let (router, batch_size) = {
        let borrowed = state.borrow();
        (
            borrowed.router.clone(),
            borrowed.db.limits().max_batch_items_per_route.max(1),
        )
    };
    for route in pack_batch_routes(
        &router,
        RouteTarget::Peer(watch.target_peer_id.clone()),
        None,
        items,
        batch_size,
    ) {
        send_route_state(state, &route)?;
    }

    if let Some(entry) = state.borrow_mut().incoming_watches.get_mut(watch_id) {
        entry.last_hash = content_hash;
        entry.next_sequence = entry.next_sequence.saturating_add(1);
    }
    Ok(true)
}

fn start_mesh_remote_watch_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    target_peer_id: String,
    request_kind: PullRequestKind,
) -> std::result::Result<WasmRemoteWatch, JsValue> {
    let limit = state.borrow().db.limits().max_active_remote_watches.max(1);
    let (watch_id, receiver) = {
        let mut borrowed = state.borrow_mut();
        if borrowed.outgoing_watches.len() >= limit {
            return Err(JsValue::from_str(&format!(
                "too many active remote watches (limit {limit})"
            )));
        }
        borrowed.next_message_seq = borrowed.next_message_seq.saturating_add(1);
        let watch_id = format!(
            "{}/watch/{:x}",
            borrowed.db.replica_id(),
            borrowed.next_message_seq
        );
        let (sender, receiver) = unbounded();
        borrowed.outgoing_watches.insert(
            watch_id.clone(),
            OutgoingWatch {
                sender,
                target_peer_id: target_peer_id.clone(),
                request_kind: request_kind.clone(),
                pending_sequence: None,
                last_delivered_sequence: None,
            },
        );
        (watch_id, receiver)
    };

    let _ = send_mesh_watch_request_state(
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
    Ok(WasmRemoteWatch {
        inner: Some(WasmRemoteWatchInner {
            receiver,
            cancel: Box::new(move || {
                cancel_mesh_remote_watch_state(&cancel_state, &watch_id);
            }),
        }),
    })
}

fn start_mesh_remote_watch_with_policy_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    policy: RemoteInterestPolicy,
    request_kind: PullRequestKind,
) -> std::result::Result<WasmRemoteWatch, JsValue> {
    let capability = format!("watch_{}", request_kind.kind_name());
    let peer_id =
        select_mesh_peer_for_policy_state(state, &policy, Some(&capability), Some(&request_kind))?;
    start_mesh_remote_watch_state(state, peer_id, request_kind)
}

fn select_mesh_peer_for_policy_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&PullRequestKind>,
) -> std::result::Result<String, JsValue> {
    if let Some(peer) = resolve_mesh_peers_for_policy_state(state, policy, capability, request)?
        .into_iter()
        .next()
    {
        return Ok(peer.peer_id);
    }
    let message = match capability {
        Some(capability) => format!("no open mesh peer advertises capability `{capability}`"),
        None => "no open mesh peer is available for remote interest".to_owned(),
    };
    Err(JsValue::from_str(&message))
}

fn resolve_mesh_peers_for_policy_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    policy: &RemoteInterestPolicy,
    capability: Option<&str>,
    request: Option<&PullRequestKind>,
) -> std::result::Result<Vec<crate::PeerPresence>, JsValue> {
    if let Some(peer_ids) = explicit_policy_peers(policy)? {
        if peer_ids.is_empty() {
            return Err(JsValue::from_str(
                "remote interest policy did not include any peer ids",
            ));
        }
        let borrowed = state.borrow();
        let mut peers = Vec::new();
        for peer_id in peer_ids {
            if let Some(recommendation) = borrowed.recommendations.get(&peer_id) {
                if !policy.require_capability
                    || peer_supports_request(&recommendation.peer, capability, request)
                {
                    peers.push(recommendation.peer.clone());
                }
            } else if !policy.require_capability {
                peers.push(crate::PeerPresence {
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
        if policy.require_capability && peers.is_empty() {
            return Err(JsValue::from_str(&format!(
                "no requested peer advertises required capability `{}`",
                capability.unwrap_or("unknown")
            )));
        }
        return Ok(peers);
    }

    let mut candidates = mesh_peer_candidates_state(state);
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

fn mesh_peer_candidates_state(state: &Rc<RefCell<WebRtcMeshState>>) -> Vec<crate::PeerPresence> {
    let borrowed = state.borrow();
    let open_peer_ids = borrowed
        .peers
        .iter()
        .filter_map(|(peer_id, peer)| {
            peer.channel
                .as_ref()
                .is_some_and(mesh_channel_is_open)
                .then_some(peer_id.clone())
        })
        .collect::<Vec<_>>();
    let mut recommendations = borrowed
        .recommendations
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
    if borrowed.session_auth.require_authenticated_peers {
        candidates.retain(|peer| borrowed.verified_identities.contains_key(&peer.peer_id));
    }
    candidates
}

fn cancel_mesh_remote_watch_state(state: &Rc<RefCell<WebRtcMeshState>>, watch_id: &str) {
    let Some(watch) = state.borrow_mut().outgoing_watches.remove(watch_id) else {
        return;
    };
    watch.sender.close();
    let _ = send_mesh_watch_request_state(
        state,
        &watch.target_peer_id,
        WatchRequest {
            watch_id: watch_id.to_owned(),
            request: WatchRequestKind::Cancel,
        },
    );
}

fn send_mesh_watch_request_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    target_peer_id: &str,
    request: WatchRequest,
) -> std::result::Result<(), JsValue> {
    let route = state.borrow().router.wrap_watch_request(
        request,
        RouteTarget::Peer(target_peer_id.to_owned()),
        None,
    );
    send_mesh_route_to_peer(state, target_peer_id, &route)
}

fn replay_outgoing_mesh_watches_for_peer(
    state: &Rc<RefCell<WebRtcMeshState>>,
    peer_id: &str,
) -> std::result::Result<usize, JsValue> {
    let requests = {
        let borrowed = state.borrow();
        borrowed
            .outgoing_watches
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
        send_mesh_watch_request_state(state, peer_id, request.clone())?;
    }
    Ok(requests.len())
}

fn fail_outgoing_mesh_watches_state(state: &Rc<RefCell<WebRtcMeshState>>, message: &str) {
    let outgoing = {
        let mut borrowed = state.borrow_mut();
        std::mem::take(&mut borrowed.outgoing_watches)
    };
    for watch in outgoing.into_values() {
        let _ = watch.sender.try_send(Err(message.to_owned()));
    }
}

fn clear_incoming_mesh_watches_state(state: &Rc<RefCell<WebRtcMeshState>>) {
    state.borrow_mut().incoming_watches.clear();
}

fn handle_mesh_watch_request_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    request: WatchRequest,
) -> std::result::Result<(), JsValue> {
    match request.request {
        WatchRequestKind::Subscribe {
            request: incoming_request_kind,
        } => {
            let verified_identity = verified_mesh_identity_for_peer_state(state, remote_peer);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                let route = state.borrow().router.wrap_watch_event(
                    error_watch_event(&request.watch_id, 0, true, "peer is not authenticated"),
                    RouteTarget::Peer(remote_peer.to_owned()),
                    None,
                );
                send_mesh_route_to_peer(state, remote_peer, &route)?;
                return Ok(());
            }
            let request_kind = match state
                .borrow()
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
                    let route = state.borrow().router.wrap_watch_event(
                        error_watch_event(&request.watch_id, 0, true, message),
                        RouteTarget::Peer(remote_peer.to_owned()),
                        None,
                    );
                    send_mesh_route_to_peer(state, remote_peer, &route)?;
                    return Ok(());
                }
            };
            let limit = state.borrow().db.limits().max_active_remote_watches.max(1);
            {
                let mut borrowed = state.borrow_mut();
                if borrowed.incoming_watches.len() >= limit
                    && !borrowed.incoming_watches.contains_key(&request.watch_id)
                {
                    return Err(JsValue::from_str(&format!(
                        "too many active remote watches (limit {limit})"
                    )));
                }
                let interest_path = request_kind.interest_path();
                borrowed.incoming_watches.insert(
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
            let _ = emit_single_incoming_mesh_watch_update_state(state, &request.watch_id, true)?;
        }
        WatchRequestKind::Cancel => {
            state
                .borrow_mut()
                .incoming_watches
                .remove(&request.watch_id);
        }
    }
    Ok(())
}

fn accept_mesh_watch_event_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    event: WatchEvent,
) -> std::result::Result<(), JsValue> {
    let mut deliver: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        RemoteWatchMessage,
    )> = None;
    let mut failure: Option<(
        Sender<std::result::Result<RemoteWatchMessage, String>>,
        String,
    )> = None;

    {
        let mut borrowed = state.borrow_mut();
        let Some(watch) = borrowed.outgoing_watches.get_mut(&event.watch_id) else {
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
        if let Some(message) = apply_response_body_state(&mut pending.accumulator, &event.result) {
            let sender = watch.sender.clone();
            borrowed.outgoing_watches.remove(&event.watch_id);
            failure = Some((sender, message));
        } else if event.done || event.chunk.index.saturating_add(1) >= event.chunk.total {
            let sender = watch.sender.clone();
            let pending = watch.pending_sequence.take().unwrap();
            let result = pending.accumulator.into_result().map_err(to_js_error)?;
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

fn emit_incoming_mesh_watch_updates_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    event: &crate::ChangeEvent,
) -> std::result::Result<usize, JsValue> {
    let watch_ids = {
        let borrowed = state.borrow();
        borrowed
            .incoming_watches
            .iter()
            .filter_map(|(watch_id, watch)| {
                incoming_watch_overlaps_event(watch, event).then_some(watch_id.clone())
            })
            .collect::<Vec<_>>()
    };
    let mut emitted = 0;
    for watch_id in watch_ids {
        if emit_single_incoming_mesh_watch_update_state(state, &watch_id, false)? {
            emitted += 1;
        }
    }
    Ok(emitted)
}

fn emit_single_incoming_mesh_watch_update_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    watch_id: &str,
    initial: bool,
) -> std::result::Result<bool, JsValue> {
    let Some(watch) = state.borrow().incoming_watches.get(watch_id).cloned() else {
        return Ok(false);
    };
    let verified_identity = verified_mesh_identity_for_peer_state(state, &watch.target_peer_id);
    if state.borrow().session_auth.require_authenticated_peers && verified_identity.is_none() {
        let route = state.borrow().router.wrap_watch_event(
            error_watch_event(
                watch_id,
                watch.next_sequence,
                initial,
                "peer is not authenticated",
            ),
            RouteTarget::Peer(watch.target_peer_id.clone()),
            None,
        );
        send_mesh_route_to_peer(state, &watch.target_peer_id, &route)?;
        state.borrow_mut().incoming_watches.remove(watch_id);
        return Ok(true);
    }
    let decision = state
        .borrow()
        .db
        .serve_watch_result_for_peer(
            &watch.target_peer_id,
            HookTransport::Mesh,
            watch_id,
            &watch.request_kind,
            initial,
            verified_identity.as_ref(),
        )
        .map_err(to_js_error)?;
    let (result, content_hash, denied_message) = match decision {
        crate::HookDecision::Allow { value } => {
            let content_hash = crate::stable_content_hash(&value);
            (value, content_hash, None)
        }
        crate::HookDecision::Deny { message } => {
            (RemoteResult::Get { value: None }, None, Some(message))
        }
    };
    if let Some(message) = denied_message {
        let route = state.borrow().router.wrap_watch_event(
            error_watch_event(watch_id, watch.next_sequence, initial, message),
            RouteTarget::Peer(watch.target_peer_id.clone()),
            None,
        );
        send_mesh_route_to_peer(state, &watch.target_peer_id, &route)?;
        state.borrow_mut().incoming_watches.remove(watch_id);
        return Ok(true);
    }
    if !initial && content_hash == watch.last_hash {
        return Ok(false);
    }

    let items = state
        .borrow()
        .db
        .chunk_watch_result(watch_id, watch.next_sequence, initial, result)
        .into_iter()
        .map(|event| RouteBatchItem::WatchEvent { event })
        .collect::<Vec<_>>();
    let (router, batch_size) = {
        let borrowed = state.borrow();
        (
            borrowed.router.clone(),
            borrowed.db.limits().max_batch_items_per_route.max(1),
        )
    };
    for route in pack_batch_routes(
        &router,
        RouteTarget::Peer(watch.target_peer_id.clone()),
        None,
        items,
        batch_size,
    ) {
        send_mesh_route_to_peer(state, &watch.target_peer_id, &route)?;
    }

    if let Some(entry) = state.borrow_mut().incoming_watches.get_mut(watch_id) {
        entry.last_hash = content_hash;
        entry.next_sequence = entry.next_sequence.saturating_add(1);
    }
    Ok(true)
}

fn store_peer_recommendations_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    peers: Vec<PeerRecommendation>,
) {
    let max = state.borrow().db.limits().max_peer_recommendations.max(1);
    let mut borrowed = state.borrow_mut();
    for peer in peers {
        borrowed
            .recommendations
            .insert(peer.peer.peer_id.clone(), peer);
    }
    while borrowed.recommendations.len() > max {
        let Some(oldest) = borrowed.recommendations.keys().next().cloned() else {
            break;
        };
        borrowed.recommendations.remove(&oldest);
    }
}

fn peer_recommendation_from_presence(peer: &crate::PeerPresence) -> PeerRecommendation {
    const MAX_HINTS: usize = 8;
    const MAX_TOPICS: usize = 8;
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
            + peer.capabilities.len().min(MAX_HINTS) as u16 * 5
            + peer.topics.len().min(MAX_TOPICS) as u16 * 5,
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

fn apply_response_body_state(
    accumulator: &mut PullAccumulator,
    result: &PullResponseBody,
) -> Option<String> {
    match result {
        PullResponseBody::Get { value } => {
            *accumulator = PullAccumulator::Get {
                value: value.clone(),
            };
            None
        }
        PullResponseBody::Map { entries } => {
            if let PullAccumulator::Map { entries: current } = accumulator {
                current.extend(entries.clone());
            }
            None
        }
        PullResponseBody::Query { entries } => {
            if let PullAccumulator::Query { entries: current } = accumulator {
                current.extend(entries.clone());
            }
            None
        }
        PullResponseBody::Lex { entries } => {
            if let PullAccumulator::Lex { entries: current } = accumulator {
                current.extend(entries.clone());
            }
            None
        }
        PullResponseBody::Records {
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
        PullResponseBody::VectorSearch { result } => {
            *accumulator = PullAccumulator::VectorSearch {
                result: Some(result.clone()),
            };
            None
        }
        PullResponseBody::Node { node } => {
            *accumulator = PullAccumulator::Node { node: node.clone() };
            None
        }
        PullResponseBody::Snapshot {
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
        PullResponseBody::Transaction { report } => {
            *accumulator = PullAccumulator::Transaction {
                report: Some(report.clone()),
            };
            None
        }
        PullResponseBody::Error { message } => Some(message.clone()),
    }
}

impl PullAccumulator {
    fn new(request: &PullRequestKind) -> Self {
        match request {
            PullRequestKind::Get { .. } => Self::Get { value: None },
            PullRequestKind::Map { .. } => Self::Map {
                entries: Vec::new(),
            },
            PullRequestKind::Query { .. } => Self::Query {
                entries: Vec::new(),
            },
            PullRequestKind::Lex { .. } => Self::Lex {
                entries: Vec::new(),
            },
            PullRequestKind::Records { .. } => Self::Records {
                entries: Vec::new(),
                next_cursor: None,
            },
            PullRequestKind::VectorSearch { .. } => Self::VectorSearch { result: None },
            PullRequestKind::Node { .. } => Self::Node { node: None },
            PullRequestKind::Snapshot { .. } => Self::Snapshot {
                clock: None,
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
                scope_policies: BTreeMap::new(),
            },
            PullRequestKind::Transaction { .. } => Self::Transaction { report: None },
        }
    }

    fn into_result(self) -> crate::Result<RemoteResult> {
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
                    crate::PrimadbError::Message(
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
                        crate::PrimadbError::Message(
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
                    crate::PrimadbError::Message(
                        "transaction response completed without a report".to_owned(),
                    )
                })?,
            }),
        }
    }
}

fn encode_sync_payload(
    _db: &Primadb,
    frame: SyncFrame,
) -> std::result::Result<(String, JsonValue), JsValue> {
    #[cfg(feature = "crypto")]
    {
        let frame = _db.secure_sync_frame(frame).map_err(to_js_error)?;
        return match frame {
            SecureSyncFrame::Plain(frame) => Ok((
                "sync_frame".to_owned(),
                serde_json::to_value(frame).map_err(to_js_error)?,
            )),
            secure => Ok((
                "secure_sync_frame".to_owned(),
                serde_json::to_value(secure).map_err(to_js_error)?,
            )),
        };
    }

    #[cfg(not(feature = "crypto"))]
    {
        Ok((
            "sync_frame".to_owned(),
            serde_json::to_value(frame).map_err(to_js_error)?,
        ))
    }
}

fn decode_sync_payload(
    _db: &Primadb,
    encoding: &str,
    payload: JsonValue,
) -> std::result::Result<SyncFrame, JsValue> {
    match encoding {
        "sync_frame" => serde_json::from_value(payload).map_err(to_js_error),
        #[cfg(feature = "crypto")]
        "secure_sync_frame" => _db
            .decode_secure_sync_frame(serde_json::from_value(payload).map_err(to_js_error)?)
            .map_err(to_js_error),
        #[cfg(not(feature = "crypto"))]
        "secure_sync_frame" => Err(JsValue::from_str(
            "received secure sync frame without crypto support",
        )),
        other => Err(JsValue::from_str(&format!(
            "unsupported sync encoding `{other}`"
        ))),
    }
}

fn post_mesh_signal_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    signal: &MeshSignal,
) -> std::result::Result<(), JsValue> {
    let signaling = state.borrow().signaling.clone();
    match signaling {
        MeshSignalingTransport::BroadcastChannel(signaling) => signaling.post_message(
            &signal
                .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
                .map_err(|error| JsValue::from_str(&error.to_string()))?,
        ),
        MeshSignalingTransport::Relay { .. } | MeshSignalingTransport::External { .. } => {
            let (router, room) = {
                let borrowed = state.borrow();
                (borrowed.router.clone(), borrowed.room.clone())
            };
            let route = router.wrap_signal(
                room,
                serde_json::to_value(signal).map_err(to_js_error)?,
                mesh_signal_target(signal),
            );
            send_mesh_signal_route_state(state, &route)
        }
    }
}

fn announce_mesh_join_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<(), JsValue> {
    let (room, from) = {
        let borrowed = state.borrow();
        (borrowed.room.clone(), borrowed.peer_id.clone())
    };
    post_mesh_signal_state(state, &MeshSignal::Join { room, from })
}

fn post_mesh_leave_signal_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<(), JsValue> {
    let (room, from) = {
        let borrowed = state.borrow();
        (borrowed.room.clone(), borrowed.peer_id.clone())
    };
    post_mesh_signal_state(state, &MeshSignal::Leave { room, from })
}

fn mesh_signal_target(signal: &MeshSignal) -> RouteTarget {
    match signal {
        MeshSignal::Join { .. } | MeshSignal::Leave { .. } => RouteTarget::Broadcast,
        MeshSignal::Offer { to, .. }
        | MeshSignal::Answer { to, .. }
        | MeshSignal::Ice { to, .. } => RouteTarget::Peer(to.clone()),
    }
}

fn initialize_mesh_relay_callbacks(state: &Rc<RefCell<WebRtcMeshState>>, _relay_url: String) {
    let onmessage_state = state.clone();
    let onmessage = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
        if let Some(payload) = event.data().as_string() {
            let _ = handle_mesh_signaling_websocket_message(&onmessage_state, &payload);
        }
    }) as Box<dyn FnMut(_)>);

    let onopen_state = state.clone();
    let onopen = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        let _ = send_mesh_presence_state(&onopen_state);
        let _ = announce_mesh_join_state(&onopen_state);
        let _ = retry_mesh_inflight_state(&onopen_state);
        let _ = flush_mesh_pending_state(&onopen_state);
    }) as Box<dyn FnMut(_)>);

    let onclose = Closure::wrap(Box::new(move |_event: web_sys::CloseEvent| {
        // Existing peer data channels may remain alive after signaling disconnects.
    }) as Box<dyn FnMut(_)>);

    let onerror = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        // Keep the current mesh alive for already-open data channels.
    }) as Box<dyn FnMut(_)>);

    let mut borrowed = state.borrow_mut();
    borrowed.relay_onmessage = Some(onmessage);
    borrowed.relay_onopen = Some(onopen);
    borrowed.relay_onclose = Some(onclose);
    borrowed.relay_onerror = Some(onerror);
}

fn bind_mesh_relay_socket_callbacks(
    state: &Rc<RefCell<WebRtcMeshState>>,
    socket: &web_sys::WebSocket,
) {
    let borrowed = state.borrow();
    socket.set_onmessage(
        borrowed
            .relay_onmessage
            .as_ref()
            .map(|callback| callback.as_ref().unchecked_ref()),
    );
    socket.set_onopen(
        borrowed
            .relay_onopen
            .as_ref()
            .map(|callback| callback.as_ref().unchecked_ref()),
    );
    socket.set_onclose(
        borrowed
            .relay_onclose
            .as_ref()
            .map(|callback| callback.as_ref().unchecked_ref()),
    );
    socket.set_onerror(
        borrowed
            .relay_onerror
            .as_ref()
            .map(|callback| callback.as_ref().unchecked_ref()),
    );
}

fn ensure_mesh_relay_socket_connected_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<(), JsValue> {
    let relay_url = {
        let borrowed = state.borrow();
        let MeshSignalingTransport::Relay { socket, relay_url } = &borrowed.signaling else {
            return Ok(());
        };
        if matches!(
            socket.ready_state(),
            web_sys::WebSocket::OPEN | web_sys::WebSocket::CONNECTING
        ) {
            return Ok(());
        }
        socket.set_onmessage(None);
        socket.set_onopen(None);
        socket.set_onclose(None);
        socket.set_onerror(None);
        relay_url.clone()
    };

    let socket = web_sys::WebSocket::new(&relay_url)?;
    socket.set_binary_type(web_sys::BinaryType::Arraybuffer);
    bind_mesh_relay_socket_callbacks(state, &socket);
    state.borrow_mut().signaling = MeshSignalingTransport::Relay { socket, relay_url };
    Ok(())
}

fn refresh_external_mesh_presence_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<(), JsValue> {
    if !matches!(
        &state.borrow().signaling,
        MeshSignalingTransport::External { .. }
    ) {
        return Ok(());
    }
    send_mesh_presence_state(state)
}

fn send_mesh_presence_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<(), JsValue> {
    let route = {
        let borrowed = state.borrow();
        let (transport, signaling_mode, relay_url) = match &borrowed.signaling {
            MeshSignalingTransport::BroadcastChannel(_) => return Ok(()),
            MeshSignalingTransport::Relay { relay_url, .. } => (
                "webrtc-relay".to_owned(),
                "relay".to_owned(),
                Some(relay_url.clone()),
            ),
            MeshSignalingTransport::External {
                relay_url, mode, ..
            } => {
                let transport = if mode == "moq" {
                    "webrtc-moq".to_owned()
                } else {
                    "webrtc-external".to_owned()
                };
                (transport, mode.clone(), relay_url.clone())
            }
        };
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
            "application_routes".to_owned(),
        ];
        capabilities.extend(borrowed.db.vector_presence_capabilities());
        let mut route = borrowed.router.presence(
            borrowed.db.replica_id(),
            transport,
            capabilities,
            vec![format!("mesh:{}", borrowed.room)],
        );
        if let RoutePayload::Presence { peer } = &mut route.payload {
            if let Some(relay_url) = relay_url {
                peer.metadata.insert("relay_url".to_owned(), relay_url);
            }
            peer.metadata
                .insert("mesh_room".to_owned(), borrowed.room.clone());
            peer.metadata.insert("signaling".to_owned(), signaling_mode);
            peer.identity = borrowed.db.session_presence_identity(&borrowed.session_id);
        }
        route
    };
    send_mesh_signal_route_state(state, &route)
}

fn handle_mesh_signaling_websocket_message(
    state: &Rc<RefCell<WebRtcMeshState>>,
    payload: &str,
) -> std::result::Result<(), JsValue> {
    let route: RouteEnvelope =
        serde_json::from_str(payload).map_err(|error| JsValue::from_str(&error.to_string()))?;
    handle_mesh_signaling_route(state, route)
}

fn handle_mesh_signaling_route(
    state: &Rc<RefCell<WebRtcMeshState>>,
    route: RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let decision = {
        let borrowed = state.borrow();
        borrowed.router.accept(route.clone())
    };
    if !decision.deliver {
        return Ok(());
    }

    let room = state.borrow().room.clone();
    let from = route.from;
    let route_id = route.route_id;
    let channel = route.channel;
    let target = route.target;
    let issued_at_millis = route.issued_at_millis;
    let transport = mesh_signaling_transport_kind_state(state);
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                maybe_send_mesh_auth_challenge_relay_state(state, &peer)?;
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_mesh_identity_for_peer_state(state, &peer.peer_id).is_none()
                {
                    continue;
                }
                let recommendation = peer_recommendation_from_presence(&peer);
                let verified_identity = verified_mesh_identity_for_peer_state(state, &peer.peer_id);
                let (_, peer_to_join) = accept_mesh_recommendation_state(
                    state,
                    recommendation,
                    verified_identity.as_ref(),
                )?;
                if let Some(peer_id) = peer_to_join {
                    handle_mesh_signal_state(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer_id,
                        },
                    )?;
                }
            }
            RoutePayload::Signal {
                room: signal_room,
                payload,
            } => {
                if signal_room != room {
                    continue;
                }
                let signal: MeshSignal = serde_json::from_value(payload).map_err(to_js_error)?;
                handle_mesh_signal_state(state, signal)?;
            }
            RoutePayload::PeerExchange { peers } => {
                for recommendation in peers {
                    maybe_send_mesh_auth_challenge_relay_state(state, &recommendation.peer)?;
                    if state.borrow().session_auth.require_authenticated_peers
                        && verified_mesh_identity_for_peer_state(
                            state,
                            &recommendation.peer.peer_id,
                        )
                        .is_none()
                    {
                        continue;
                    }
                    let verified_identity =
                        verified_mesh_identity_for_peer_state(state, &recommendation.peer.peer_id);
                    let (_, peer_to_join) = accept_mesh_recommendation_state(
                        state,
                        recommendation,
                        verified_identity.as_ref(),
                    )?;
                    if let Some(peer_id) = peer_to_join {
                        handle_mesh_signal_state(
                            state,
                            MeshSignal::Join {
                                room: room.clone(),
                                from: peer_id,
                            },
                        )?;
                    }
                }
            }
            RoutePayload::AuthChallenge { challenge } => {
                handle_mesh_auth_challenge_relay_state(state, challenge)?;
            }
            RoutePayload::AuthResponse { response } => {
                if let Some(peer_id) = handle_mesh_auth_response_state(state, response)? {
                    handle_mesh_signal_state(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer_id,
                        },
                    )?;
                }
            }
            RoutePayload::Application { message } => {
                let verified_identity = verified_mesh_identity_for_peer_state(state, &from);
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    continue;
                }
                let event = ApplicationRouteEvent {
                    route_id: route_id.clone(),
                    from: from.clone(),
                    channel: channel.clone(),
                    target: target.clone(),
                    issued_at_millis,
                    received_at_millis: js_sys::Date::now() as u64,
                    transport: transport.clone(),
                    verified_identity,
                    message,
                };
                state.borrow().applications.publish(event);
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

fn send_mesh_signal_route_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    route: &RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let signaling = state.borrow().signaling.clone();
    match signaling {
        MeshSignalingTransport::BroadcastChannel(_) => Ok(()),
        MeshSignalingTransport::Relay { socket, .. } => {
            let max_bytes = state.borrow().db.limits().max_route_payload_bytes;
            if socket.ready_state() != web_sys::WebSocket::OPEN {
                return Err(JsValue::from_str("mesh relay websocket is not connected"));
            }
            send_websocket_route(&socket, max_bytes, route)
        }
        MeshSignalingTransport::External { send_route, .. } => {
            let route = to_js(route)?;
            send_route.call1(&JsValue::NULL, &route).map(|_| ())
        }
    }
}

fn verified_mesh_identity_for_peer_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    peer_id: &str,
) -> Option<VerifiedIdentity> {
    state.borrow().verified_identities.get(peer_id).cloned()
}

fn remove_mesh_peer_identity_state(state: &Rc<RefCell<WebRtcMeshState>>, peer_id: &str) {
    state.borrow().router.forget_peer(peer_id);
    let mut borrowed = state.borrow_mut();
    borrowed.recommendations.remove(peer_id);
    borrowed.verified_identities.remove(peer_id);
}

fn accept_mesh_recommendation_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    recommendation: PeerRecommendation,
    verified_identity: Option<&VerifiedIdentity>,
) -> std::result::Result<(bool, Option<String>), JsValue> {
    let relay_url = recommendation.relay_urls.first().cloned();
    let allowed = state
        .borrow()
        .db
        .allow_peer_connection(&crate::ConnectHookContext {
            peer: recommendation.peer.clone(),
            transport: HookTransport::Mesh,
            relay_url,
            verified_identity: verified_identity.cloned(),
        })
        .into_result();
    if allowed.is_err() {
        remove_mesh_peer_identity_state(state, &recommendation.peer.peer_id);
        return Ok((false, None));
    }

    let peer = recommendation.peer.clone();
    store_mesh_recommendations_state(state, vec![recommendation]);

    let room = state.borrow().room.clone();
    let in_room = peer
        .topics
        .iter()
        .any(|topic| topic == &format!("mesh:{room}"))
        || peer
            .metadata
            .get("mesh_room")
            .is_some_and(|candidate| candidate == &room);
    if !in_room {
        return Ok((true, None));
    }

    let room_allowed = state
        .borrow()
        .db
        .allow_room_join(&crate::RoomHookContext {
            peer_id: peer.peer_id.clone(),
            room,
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

fn store_mesh_recommendations_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    peers: Vec<PeerRecommendation>,
) {
    let max = state.borrow().db.limits().max_peer_recommendations.max(1);
    let mut borrowed = state.borrow_mut();
    for peer in peers {
        borrowed
            .recommendations
            .insert(peer.peer.peer_id.clone(), peer);
    }
    while borrowed.recommendations.len() > max {
        let Some(oldest) = borrowed.recommendations.keys().next().cloned() else {
            break;
        };
        borrowed.recommendations.remove(&oldest);
    }
}

fn maybe_send_mesh_auth_challenge_relay_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    peer: &crate::PeerPresence,
) -> std::result::Result<(), JsValue> {
    let should_skip = {
        let borrowed = state.borrow();
        peer.peer_id == borrowed.router.peer_id()
            || borrowed.verified_identities.contains_key(&peer.peer_id)
    };
    if should_skip {
        return Ok(());
    }
    let Some(identity) = peer.identity.as_ref() else {
        if !state.borrow().session_auth.allow_unauthenticated_presence {
            remove_mesh_peer_identity_state(state, &peer.peer_id);
        }
        return Ok(());
    };

    #[cfg(feature = "crypto")]
    {
        let (challenge, route) = {
            let borrowed = state.borrow();
            let challenge = crate::session_auth::create_auth_challenge(
                borrowed.router.peer_id(),
                &borrowed.db.replica_id(),
                &borrowed.session_id,
                &peer.peer_id,
                &peer.replica_id,
                identity,
                "mesh",
                &borrowed.session_auth,
            );
            let route = borrowed
                .router
                .auth_challenge(challenge.clone(), RouteTarget::Peer(peer.peer_id.clone()));
            (challenge, route)
        };
        send_mesh_signal_route_state(state, &route)?;
        let mut borrowed = state.borrow_mut();
        borrowed
            .pending_auth_challenges
            .insert(challenge.challenge_id.clone(), challenge.clone());
        borrowed
            .pending_auth_peers
            .insert(challenge.challenge_id.clone(), peer.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = identity;
    }

    Ok(())
}

fn maybe_send_mesh_auth_challenge_to_peer_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    peer: &crate::PeerPresence,
) -> std::result::Result<(), JsValue> {
    let should_skip = {
        let borrowed = state.borrow();
        peer.peer_id == borrowed.router.peer_id()
            || borrowed.verified_identities.contains_key(&peer.peer_id)
    };
    if should_skip {
        return Ok(());
    }
    let Some(identity) = peer.identity.as_ref() else {
        if !state.borrow().session_auth.allow_unauthenticated_presence {
            remove_mesh_peer_identity_state(state, &peer.peer_id);
        }
        return Ok(());
    };

    #[cfg(feature = "crypto")]
    {
        let (challenge, route) = {
            let borrowed = state.borrow();
            let challenge = crate::session_auth::create_auth_challenge(
                borrowed.router.peer_id(),
                &borrowed.db.replica_id(),
                &borrowed.session_id,
                &peer.peer_id,
                &peer.replica_id,
                identity,
                "mesh",
                &borrowed.session_auth,
            );
            let route = borrowed
                .router
                .auth_challenge(challenge.clone(), RouteTarget::Peer(peer.peer_id.clone()));
            (challenge, route)
        };
        send_mesh_route_to_peer(state, remote_peer, &route)?;
        let mut borrowed = state.borrow_mut();
        borrowed
            .pending_auth_challenges
            .insert(challenge.challenge_id.clone(), challenge.clone());
        borrowed
            .pending_auth_peers
            .insert(challenge.challenge_id.clone(), peer.clone());
    }

    #[cfg(not(feature = "crypto"))]
    {
        let _ = identity;
        let _ = remote_peer;
    }

    Ok(())
}

fn handle_mesh_auth_challenge_relay_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    challenge: crate::AuthChallenge,
) -> std::result::Result<(), JsValue> {
    if challenge.target_peer_id != state.borrow().router.peer_id() {
        return Ok(());
    }
    let response = {
        let borrowed = state.borrow();
        borrowed
            .db
            .sign_session_auth_response(
                &challenge,
                borrowed.router.peer_id(),
                &borrowed.session_id,
                &borrowed.session_auth,
            )
            .map_err(to_js_error)?
    };
    let Some(response) = response else {
        return Ok(());
    };
    let route = state
        .borrow()
        .router
        .auth_response(response, RouteTarget::Peer(challenge.issuer_peer_id));
    send_mesh_signal_route_state(state, &route)
}

fn handle_mesh_auth_challenge_to_peer_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    challenge: crate::AuthChallenge,
) -> std::result::Result<(), JsValue> {
    if challenge.target_peer_id != state.borrow().router.peer_id() {
        return Ok(());
    }
    let response = {
        let borrowed = state.borrow();
        borrowed
            .db
            .sign_session_auth_response(
                &challenge,
                borrowed.router.peer_id(),
                &borrowed.session_id,
                &borrowed.session_auth,
            )
            .map_err(to_js_error)?
    };
    let Some(response) = response else {
        return Ok(());
    };
    let route = state
        .borrow()
        .router
        .auth_response(response, RouteTarget::Peer(challenge.issuer_peer_id));
    send_mesh_route_to_peer(state, remote_peer, &route)
}

fn handle_mesh_auth_response_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    response: crate::AuthResponse,
) -> std::result::Result<Option<String>, JsValue> {
    let Some(challenge) = state
        .borrow_mut()
        .pending_auth_challenges
        .remove(&response.challenge_id)
    else {
        return Ok(None);
    };
    let peer = state
        .borrow_mut()
        .pending_auth_peers
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
    let verified = {
        let borrowed = state.borrow();
        crate::session_auth::verify_auth_response(&challenge, &response, &borrowed.session_auth)
            .map_err(to_js_error)?
    };
    state
        .borrow_mut()
        .verified_identities
        .insert(verified.peer_id.clone(), verified.clone());
    let recommendation = peer_recommendation_from_presence(&peer);
    let (accepted, peer_to_join) =
        accept_mesh_recommendation_state(state, recommendation, Some(&verified))?;
    if !accepted {
        state
            .borrow_mut()
            .verified_identities
            .remove(&verified.peer_id);
    }
    Ok(peer_to_join)
}

fn incoming_watch_overlaps_event(watch: &IncomingWatch, event: &crate::ChangeEvent) -> bool {
    if event.full_refresh {
        return true;
    }
    if let PullRequestKind::Records { scan } = &watch.request_kind {
        return event.records_changed
            && (event.touched_record_keys.is_empty()
                || event
                    .touched_record_keys
                    .iter()
                    .any(|key| scan.matches_key(key)));
    }
    if let PullRequestKind::VectorSearch { collection, .. } = &watch.request_kind {
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

fn handle_mesh_signal_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    signal: MeshSignal,
) -> std::result::Result<(), JsValue> {
    let (room, peer_id) = {
        let borrowed = state.borrow();
        (borrowed.room.clone(), borrowed.peer_id.clone())
    };

    match signal {
        MeshSignal::Join {
            room: join_room,
            from,
        } => {
            if join_room != room || from == peer_id {
                return Ok(());
            }
            let verified_identity = verified_mesh_identity_for_peer_state(state, &from);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                return Ok(());
            }
            if state
                .borrow()
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
                let borrowed = state.borrow();
                let peer = borrowed.peers.get(&from);
                let already_open = peer
                    .and_then(|peer| peer.channel.as_ref())
                    .is_some_and(mesh_channel_is_open);
                let is_stale = peer.is_some_and(|peer| {
                    !peer.channel.as_ref().is_some_and(mesh_channel_is_open)
                        && (js_sys::Date::now() as u64).saturating_sub(peer.created_at_millis)
                            >= STALE_MESH_PEER_MILLIS
                });
                (already_open, is_stale)
            };
            if already_open {
                return Ok(());
            }
            if is_stale {
                remove_mesh_peer_state(state, &from);
            }
            if peer_id < from {
                let state = state.clone();
                spawn_local(async move {
                    let _ = create_mesh_offer(&state, from).await;
                });
            } else {
                let (room, from_local) = {
                    let borrowed = state.borrow();
                    (borrowed.room.clone(), borrowed.peer_id.clone())
                };
                let _ = post_mesh_signal_state(
                    state,
                    &MeshSignal::Join {
                        room,
                        from: from_local,
                    },
                );
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
            let verified_identity = verified_mesh_identity_for_peer_state(state, &from);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                return Ok(());
            }
            if state
                .borrow()
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
            let state = state.clone();
            spawn_local(async move {
                let _ = accept_mesh_offer(&state, from, sdp).await;
            });
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
            let verified_identity = verified_mesh_identity_for_peer_state(state, &from);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                return Ok(());
            }
            if state
                .borrow()
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
            let state = state.clone();
            spawn_local(async move {
                let _ = accept_mesh_answer(&state, from, sdp).await;
            });
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
            let verified_identity = verified_mesh_identity_for_peer_state(state, &from);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                return Ok(());
            }
            if state
                .borrow()
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
            let state = state.clone();
            spawn_local(async move {
                let _ =
                    add_mesh_ice_candidate(&state, from, candidate, sdp_mid, sdp_mline_index).await;
            });
        }
        MeshSignal::Leave {
            room: leave_room,
            from,
        } => {
            if leave_room != room || from == peer_id {
                return Ok(());
            }
            let verified_identity = verified_mesh_identity_for_peer_state(state, &from);
            if state.borrow().session_auth.require_authenticated_peers
                && verified_identity.is_none()
            {
                return Ok(());
            }
            if state
                .borrow()
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
            remove_mesh_peer_state(state, &from);
        }
    }

    Ok(())
}

async fn create_mesh_offer(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: String,
) -> std::result::Result<(), JsValue> {
    let connection = ensure_mesh_peer(state, &remote_peer)?;
    if state
        .borrow()
        .peers
        .get(&remote_peer)
        .and_then(|peer| peer.channel.as_ref())
        .is_none()
    {
        let channel = connection.create_data_channel("primadb");
        attach_mesh_channel_handlers(state, &remote_peer, channel)?;
    }

    let offer_value = JsFuture::from(connection.create_offer()).await?;
    let offer_sdp = session_description_sdp_value(&offer_value)?;
    let offer = session_description_init(web_sys::RtcSdpType::Offer, &offer_sdp);
    JsFuture::from(connection.set_local_description(&offer)).await?;
    let (room, from) = {
        let borrowed = state.borrow();
        (borrowed.room.clone(), borrowed.peer_id.clone())
    };
    post_mesh_signal_state(
        state,
        &MeshSignal::Offer {
            room,
            from,
            to: remote_peer,
            sdp: offer_sdp,
        },
    )?;
    Ok(())
}

async fn accept_mesh_offer(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: String,
    sdp: String,
) -> std::result::Result<(), JsValue> {
    let connection = ensure_mesh_peer(state, &remote_peer)?;
    let offer = web_sys::RtcSessionDescriptionInit::new(web_sys::RtcSdpType::Offer);
    offer.set_sdp(&sdp);
    JsFuture::from(connection.set_remote_description(&offer)).await?;

    let answer_value = JsFuture::from(connection.create_answer()).await?;
    let answer_sdp = session_description_sdp_value(&answer_value)?;
    let answer = session_description_init(web_sys::RtcSdpType::Answer, &answer_sdp);
    JsFuture::from(connection.set_local_description(&answer)).await?;
    let (room, from) = {
        let borrowed = state.borrow();
        (borrowed.room.clone(), borrowed.peer_id.clone())
    };
    post_mesh_signal_state(
        state,
        &MeshSignal::Answer {
            room,
            from,
            to: remote_peer,
            sdp: answer_sdp,
        },
    )?;
    Ok(())
}

async fn accept_mesh_answer(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: String,
    sdp: String,
) -> std::result::Result<(), JsValue> {
    let connection = ensure_mesh_peer(state, &remote_peer)?;
    let answer = web_sys::RtcSessionDescriptionInit::new(web_sys::RtcSdpType::Answer);
    answer.set_sdp(&sdp);
    JsFuture::from(connection.set_remote_description(&answer)).await?;
    Ok(())
}

async fn add_mesh_ice_candidate(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: String,
    candidate: String,
    sdp_mid: Option<String>,
    sdp_mline_index: Option<u16>,
) -> std::result::Result<(), JsValue> {
    let connection = ensure_mesh_peer(state, &remote_peer)?;
    let init = web_sys::RtcIceCandidateInit::new(&candidate);
    if let Some(sdp_mid) = sdp_mid.as_deref() {
        init.set_sdp_mid(Some(sdp_mid));
    }
    if let Some(index) = sdp_mline_index {
        init.set_sdp_m_line_index(Some(index));
    }
    let candidate = web_sys::RtcIceCandidate::new(&init)?;
    JsFuture::from(connection.add_ice_candidate_with_opt_rtc_ice_candidate(Some(&candidate)))
        .await?;
    Ok(())
}

fn ensure_mesh_peer(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
) -> std::result::Result<web_sys::RtcPeerConnection, JsValue> {
    if let Some(existing) = state.borrow().peers.get(remote_peer) {
        return Ok(existing.connection.clone());
    }

    let configuration = state.borrow().rtc_configuration.clone();
    let configuration: web_sys::RtcConfiguration = configuration.unchecked_into();
    let connection = web_sys::RtcPeerConnection::new_with_configuration(&configuration)?;
    let remote = remote_peer.to_owned();
    let state_for_ice = state.clone();
    let onicecandidate = Closure::wrap(Box::new(move |event: web_sys::RtcPeerConnectionIceEvent| {
        let Some(candidate) = event.candidate() else {
            return;
        };
        let (room, from) = {
            let borrowed = state_for_ice.borrow();
            (borrowed.room.clone(), borrowed.peer_id.clone())
        };
        let signal = MeshSignal::Ice {
            room,
            from,
            to: remote.clone(),
            candidate: candidate.candidate(),
            sdp_mid: candidate.sdp_mid(),
            sdp_mline_index: candidate.sdp_m_line_index(),
        };
        let _ = post_mesh_signal_state(&state_for_ice, &signal);
    }) as Box<dyn FnMut(_)>);
    connection.set_onicecandidate(Some(onicecandidate.as_ref().unchecked_ref()));

    let remote_for_data = remote_peer.to_owned();
    let state_for_data = state.clone();
    let ondatachannel = Closure::wrap(Box::new(move |event: web_sys::RtcDataChannelEvent| {
        let channel = event.channel();
        let _ = attach_mesh_channel_handlers(&state_for_data, &remote_for_data, channel);
    }) as Box<dyn FnMut(_)>);
    connection.set_ondatachannel(Some(ondatachannel.as_ref().unchecked_ref()));

    state.borrow_mut().peers.insert(
        remote_peer.to_owned(),
        MeshPeer {
            connection: connection.clone(),
            channel: None,
            created_at_millis: js_sys::Date::now() as u64,
            onicecandidate: Some(onicecandidate),
            ondatachannel: Some(ondatachannel),
            onmessage: None,
            onopen: None,
            onclose: None,
        },
    );
    Ok(connection)
}

fn attach_mesh_channel_handlers(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    channel: web_sys::RtcDataChannel,
) -> std::result::Result<(), JsValue> {
    let remote_for_message = remote_peer.to_owned();
    let message_state = state.clone();
    let onmessage = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
        if let Some(payload) = event.data().as_string() {
            let _ = handle_mesh_data_message(&message_state, &remote_for_message, &payload);
        }
    }) as Box<dyn FnMut(_)>);
    channel.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

    let open_state = state.clone();
    let remote_for_open = remote_peer.to_owned();
    let onopen = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        let _ = flush_mesh_pending_state(&open_state);
        let _ = replay_outgoing_mesh_watches_for_peer(&open_state, &remote_for_open);
    }) as Box<dyn FnMut(_)>);
    channel.set_onopen(Some(onopen.as_ref().unchecked_ref()));

    let remote_for_close = remote_peer.to_owned();
    let close_state = state.clone();
    let onclose = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        remove_mesh_peer_state(&close_state, &remote_for_close);
    }) as Box<dyn FnMut(_)>);
    channel.set_onclose(Some(onclose.as_ref().unchecked_ref()));

    if let Some(peer) = state.borrow_mut().peers.get_mut(remote_peer) {
        peer.channel = Some(channel);
        peer.onmessage = Some(onmessage);
        peer.onopen = Some(onopen);
        peer.onclose = Some(onclose);
    }
    Ok(())
}

fn remove_mesh_peer_state(state: &Rc<RefCell<WebRtcMeshState>>, remote_peer: &str) {
    let mut to_requeue = Vec::new();
    {
        let mut borrowed = state.borrow_mut();
        if let Some(peer) = borrowed.peers.remove(remote_peer) {
            peer.connection.set_onicecandidate(None);
            peer.connection.set_ondatachannel(None);
            if let Some(channel) = &peer.channel {
                channel.set_onmessage(None);
                channel.set_onopen(None);
                channel.set_onclose(None);
                let _ = channel.close();
            }
            let _ = peer.connection.close();
        }

        let mut empty = Vec::new();
        for (message_id, outbound) in &mut borrowed.inflight {
            outbound.awaiting.remove(remote_peer);
            if outbound.awaiting.is_empty() {
                empty.push(message_id.clone());
            }
        }
        for message_id in empty {
            if let Some(outbound) = borrowed.inflight.remove(&message_id) {
                to_requeue.push(outbound);
            }
        }
        borrowed
            .incoming_watches
            .retain(|_, watch| watch.target_peer_id != remote_peer);
    }

    let db = state.borrow().db.clone();
    for outbound in to_requeue {
        if let Ok(frame) = decode_sync_payload(&db, &outbound.encoding, outbound.payload) {
            if let SyncFrame::Sync { ops, .. } = frame {
                let _ = db.requeue_pending_operations(ops);
            }
        }
    }
}

fn handle_mesh_data_message(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    payload: &str,
) -> std::result::Result<(), JsValue> {
    if let Ok(route) = serde_json::from_str::<RouteEnvelope>(payload) {
        return handle_mesh_route_message(state, remote_peer, route);
    }
    let frame: SyncFrame =
        serde_json::from_str(payload).map_err(|error| JsValue::from_str(&error.to_string()))?;
    handle_mesh_sync_frame(state, remote_peer, frame)
}

fn handle_mesh_route_message(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    route: RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let decision = {
        let borrowed = state.borrow();
        borrowed.router.accept(route.clone())
    };
    if !decision.deliver {
        return Ok(());
    }
    let from = route.from;
    let route_id = route.route_id;
    let channel = route.channel;
    let target = route.target;
    let issued_at_millis = route.issued_at_millis;
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                maybe_send_mesh_auth_challenge_to_peer_state(state, remote_peer, &peer)?;
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_mesh_identity_for_peer_state(state, &peer.peer_id).is_none()
                {
                    continue;
                }
                let recommendation = peer_recommendation_from_presence(&peer);
                let verified_identity = verified_mesh_identity_for_peer_state(state, &peer.peer_id);
                let _ = accept_mesh_recommendation_state(
                    state,
                    recommendation,
                    verified_identity.as_ref(),
                )?;
            }
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let verified_identity = verified_mesh_identity_for_peer_state(state, remote_peer);
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    continue;
                }
                let decision = state
                    .borrow()
                    .db
                    .serve_pull_request_for_peer(
                        remote_peer,
                        HookTransport::Mesh,
                        &format!("snapshot:{remote_peer}"),
                        &PullRequestKind::Snapshot { root: root.clone() },
                        verified_identity.as_ref(),
                    )
                    .map_err(to_js_error)?;
                if let crate::HookDecision::Allow {
                    value: RemoteResult::Snapshot { snapshot },
                } = decision
                {
                    let response = {
                        let borrowed = state.borrow();
                        borrowed.router.snapshot_response(
                            root,
                            snapshot,
                            RouteTarget::Peer(remote_peer.to_owned()),
                        )
                    };
                    send_mesh_route_to_peer(state, remote_peer, &response)?;
                }
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state
                    .borrow()
                    .db
                    .load_snapshot(snapshot)
                    .map_err(to_js_error)?;
            }
            RoutePayload::Sync { encoding, payload } => {
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_mesh_identity_for_peer_state(state, remote_peer).is_none()
                {
                    continue;
                }
                let frame = {
                    let borrowed = state.borrow();
                    decode_sync_payload(&borrowed.db, &encoding, payload)?
                };
                handle_mesh_sync_frame(state, remote_peer, frame)?;
            }
            RoutePayload::PullRequest { request } => {
                let (router, db, batch_size) = {
                    let borrowed = state.borrow();
                    (
                        borrowed.router.clone(),
                        borrowed.db.clone(),
                        borrowed.db.limits().max_batch_items_per_route.max(1),
                    )
                };
                let verified_identity = verified_mesh_identity_for_peer_state(state, remote_peer);
                let items = if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    vec![RouteBatchItem::PullResponse {
                        response: error_pull_response(
                            &request.request_id,
                            "peer is not authenticated",
                        ),
                    }]
                } else {
                    match db
                        .serve_pull_request_for_peer(
                            remote_peer,
                            HookTransport::Mesh,
                            &request.request_id,
                            &request.request,
                            verified_identity.as_ref(),
                        )
                        .map_err(to_js_error)?
                    {
                        crate::HookDecision::Allow { value } => db
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
                    &router,
                    RouteTarget::Peer(remote_peer.to_owned()),
                    None,
                    items,
                    batch_size,
                ) {
                    send_mesh_route_to_peer(state, remote_peer, &route)?;
                }
            }
            RoutePayload::PullResponse { .. } => {}
            RoutePayload::WatchRequest { request } => {
                handle_mesh_watch_request_state(state, remote_peer, request)?;
            }
            RoutePayload::WatchEvent { event } => {
                accept_mesh_watch_event_state(state, event)?;
            }
            RoutePayload::Application { message } => {
                let verified_identity = verified_mesh_identity_for_peer_state(state, remote_peer);
                if state.borrow().session_auth.require_authenticated_peers
                    && verified_identity.is_none()
                {
                    continue;
                }
                let event = ApplicationRouteEvent {
                    route_id: route_id.clone(),
                    from: from.clone(),
                    channel: channel.clone(),
                    target: target.clone(),
                    issued_at_millis,
                    received_at_millis: js_sys::Date::now() as u64,
                    transport: RouteTransportKind::WebRtc,
                    verified_identity,
                    message,
                };
                state.borrow().applications.publish(event);
            }
            RoutePayload::PeerExchange { peers } => {
                for recommendation in peers {
                    maybe_send_mesh_auth_challenge_to_peer_state(
                        state,
                        remote_peer,
                        &recommendation.peer,
                    )?;
                    if state.borrow().session_auth.require_authenticated_peers
                        && verified_mesh_identity_for_peer_state(
                            state,
                            &recommendation.peer.peer_id,
                        )
                        .is_none()
                    {
                        continue;
                    }
                    let verified_identity =
                        verified_mesh_identity_for_peer_state(state, &recommendation.peer.peer_id);
                    let _ = accept_mesh_recommendation_state(
                        state,
                        recommendation,
                        verified_identity.as_ref(),
                    )?;
                }
            }
            RoutePayload::AuthChallenge { challenge } => {
                handle_mesh_auth_challenge_to_peer_state(state, remote_peer, challenge)?;
            }
            RoutePayload::AuthResponse { response } => {
                let _ = handle_mesh_auth_response_state(state, response)?;
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

fn handle_mesh_sync_frame(
    state: &Rc<RefCell<WebRtcMeshState>>,
    remote_peer: &str,
    frame: SyncFrame,
) -> std::result::Result<(), JsValue> {
    match frame {
        SyncFrame::Sync {
            from,
            message_id,
            ops,
        } => {
            if state.borrow().session_auth.require_authenticated_peers
                && verified_mesh_identity_for_peer_state(state, remote_peer).is_none()
            {
                return Ok(());
            }
            let db = state.borrow().db.clone();
            let applied = db
                .apply_sync_envelope(SyncEnvelope { from, ops })
                .map_err(to_js_error)?;
            let ack = SyncFrame::Ack {
                from: db.replica_id(),
                message_id,
                applied,
            };
            send_mesh_sync_frame(state, ack, RouteTarget::Peer(remote_peer.to_owned()))
        }
        SyncFrame::Ack {
            from: _,
            message_id,
            applied: _,
        } => {
            let mut borrowed = state.borrow_mut();
            if let Some(outbound) = borrowed.inflight.get_mut(&message_id) {
                outbound.awaiting.remove(remote_peer);
                if outbound.awaiting.is_empty() {
                    borrowed.inflight.remove(&message_id);
                }
            }
            Ok(())
        }
    }
}

fn flush_mesh_pending_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<usize, JsValue> {
    let db = state.borrow().db.clone();
    let peer_ids = state
        .borrow()
        .peers
        .iter()
        .filter_map(|(peer_id, peer)| {
            peer.channel
                .as_ref()
                .is_some_and(mesh_channel_is_open)
                .then_some(peer_id.clone())
        })
        .collect::<Vec<_>>();
    if peer_ids.is_empty() {
        return Ok(0);
    }

    let mut envelope = db.drain_sync_envelope().map_err(to_js_error)?;
    if envelope.ops.is_empty() {
        return Ok(0);
    }

    let max_ops = db.limits().max_ops_per_message.max(1);
    if envelope.ops.len() > max_ops {
        let remainder = envelope.ops.split_off(max_ops);
        let _ = db.requeue_pending_operations(remainder);
    }

    let message_id = {
        let mut borrowed = state.borrow_mut();
        borrowed.next_message_seq = borrowed.next_message_seq.saturating_add(1);
        format!(
            "{}/mesh/{:x}",
            borrowed.db.replica_id(),
            borrowed.next_message_seq
        )
    };
    let frame = SyncFrame::Sync {
        from: envelope.from.clone(),
        message_id: message_id.clone(),
        ops: envelope.ops.clone(),
    };
    let (encoding, payload) = encode_sync_payload(&db, frame)?;
    let route =
        state
            .borrow()
            .router
            .wrap_sync(encoding.clone(), payload.clone(), RouteTarget::Broadcast);

    let mut awaiting = BTreeMap::new();
    for peer_id in &peer_ids {
        if send_mesh_route_to_peer(state, peer_id, &route).is_ok() {
            awaiting.insert(peer_id.clone(), false);
        }
    }

    if awaiting.is_empty() {
        let _ = db.requeue_pending_operations(envelope.ops);
        return Ok(0);
    }

    state.borrow_mut().inflight.insert(
        message_id,
        MeshOutbound {
            encoding,
            payload,
            awaiting,
        },
    );
    Ok(peer_ids.len())
}

fn retry_mesh_inflight_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
) -> std::result::Result<usize, JsValue> {
    let outbound = state
        .borrow()
        .inflight
        .iter()
        .map(|(_, outbound)| outbound.clone())
        .collect::<Vec<_>>();
    for item in &outbound {
        let route = state.borrow().router.wrap_sync(
            item.encoding.clone(),
            item.payload.clone(),
            RouteTarget::Broadcast,
        );
        for peer_id in item.awaiting.keys() {
            let _ = send_mesh_route_to_peer(state, peer_id, &route);
        }
    }
    Ok(outbound.len())
}

fn send_mesh_sync_frame(
    state: &Rc<RefCell<WebRtcMeshState>>,
    frame: SyncFrame,
    target: RouteTarget,
) -> std::result::Result<(), JsValue> {
    let db = state.borrow().db.clone();
    let (encoding, payload) = encode_sync_payload(&db, frame)?;
    let route = state
        .borrow()
        .router
        .wrap_sync(encoding, payload, target.clone());
    match target {
        RouteTarget::Peer(peer_id) => send_mesh_route_to_peer(state, &peer_id, &route),
        RouteTarget::Broadcast | RouteTarget::Topic(_) => {
            let peer_ids = state.borrow().peers.keys().cloned().collect::<Vec<_>>();
            for peer_id in peer_ids {
                let _ = send_mesh_route_to_peer(state, &peer_id, &route);
            }
            Ok(())
        }
    }
}

fn send_mesh_route_to_peer(
    state: &Rc<RefCell<WebRtcMeshState>>,
    peer_id: &str,
    route: &RouteEnvelope,
) -> std::result::Result<(), JsValue> {
    let payload = serde_json::to_string(route).map_err(to_js_error)?;
    let max_bytes = state.borrow().db.limits().max_route_payload_bytes;
    if payload.len() > max_bytes {
        return Err(JsValue::from_str(&format!(
            "route payload exceeds {max_bytes} bytes"
        )));
    }

    let channel = state
        .borrow()
        .peers
        .get(peer_id)
        .and_then(|peer| peer.channel.clone())
        .ok_or_else(|| JsValue::from_str("mesh peer channel is unavailable"))?;
    if !mesh_channel_is_open(&channel) {
        return Err(JsValue::from_str("mesh peer channel is not open"));
    }
    channel.send_with_str(&payload)
}

fn mesh_channel_is_open(channel: &web_sys::RtcDataChannel) -> bool {
    channel.ready_state() == web_sys::RtcDataChannelState::Open
}

fn session_description_sdp_value(value: &JsValue) -> std::result::Result<String, JsValue> {
    js_sys::Reflect::get(value, &JsValue::from_str("sdp"))?
        .as_string()
        .ok_or_else(|| JsValue::from_str("session description is missing sdp"))
}

fn session_description_init(
    kind: web_sys::RtcSdpType,
    sdp: &str,
) -> web_sys::RtcSessionDescriptionInit {
    let description = web_sys::RtcSessionDescriptionInit::new(kind);
    description.set_sdp(sdp);
    description
}

async fn save_snapshot_string_indexed_db(
    database_name: &str,
    store_name: &str,
    key: &str,
    payload: &str,
) -> std::result::Result<(), JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let transaction =
        db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readwrite)?;
    let store = transaction.object_store(store_name)?;
    let request = store.put_with_key(&JsValue::from_str(payload), &JsValue::from_str(key))?;
    let _ = await_idb_request(request.unchecked_ref()).await?;
    await_idb_transaction(&transaction).await?;
    Ok(())
}

async fn load_snapshot_string_indexed_db(
    database_name: &str,
    store_name: &str,
    key: &str,
) -> std::result::Result<Option<String>, JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let transaction =
        db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readonly)?;
    let store = transaction.object_store(store_name)?;
    let request = store.get(&JsValue::from_str(key))?;
    let value = await_idb_request(request.unchecked_ref()).await?;
    await_idb_transaction(&transaction).await?;

    if value.is_undefined() || value.is_null() {
        Ok(None)
    } else {
        value
            .as_string()
            .map(Some)
            .ok_or_else(|| JsValue::from_str("IndexedDB value is not a snapshot string"))
    }
}

fn blob_namespace_prefix(namespace: &str) -> String {
    format!("blob/{namespace}/")
}

async fn save_blob_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
    data: Vec<u8>,
    media_type: Option<String>,
) -> std::result::Result<BlobRef, JsValue> {
    let reference = crate::blob_ref_for_data(&data, media_type.as_deref());
    let db = open_indexed_db(database_name, store_name).await?;
    let transaction =
        db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readwrite)?;
    let store = transaction.object_store(store_name)?;
    let prefix = blob_namespace_prefix(namespace);
    let meta_key = format!("{prefix}meta/{}", encode_component(&reference.id));
    let data_key = format!("{prefix}data/{}", encode_component(&reference.id));
    let meta_value = serde_wasm_bindgen::to_value(&reference)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let meta_request = store.put_with_key(&meta_value, &JsValue::from_str(&meta_key))?;
    let data_value = js_sys::Uint8Array::from(data.as_slice());
    let data_request = store.put_with_key(&data_value, &JsValue::from_str(&data_key))?;
    let _ = await_idb_request(meta_request.unchecked_ref()).await?;
    let _ = await_idb_request(data_request.unchecked_ref()).await?;
    await_idb_transaction(&transaction).await?;
    Ok(reference)
}

async fn load_blob_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
    blob_id: &str,
) -> std::result::Result<Option<crate::StoredBlob>, JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let transaction =
        db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readonly)?;
    let store = transaction.object_store(store_name)?;
    let prefix = blob_namespace_prefix(namespace);
    let meta_key = format!("{prefix}meta/{}", encode_component(blob_id));
    let data_key = format!("{prefix}data/{}", encode_component(blob_id));
    let meta_request = store.get(&JsValue::from_str(&meta_key))?;
    let data_request = store.get(&JsValue::from_str(&data_key))?;
    let meta_value = await_idb_request(meta_request.unchecked_ref()).await?;
    if meta_value.is_undefined() || meta_value.is_null() {
        await_idb_transaction(&transaction).await?;
        return Ok(None);
    }
    let data_value = await_idb_request(data_request.unchecked_ref()).await?;
    await_idb_transaction(&transaction).await?;
    if data_value.is_undefined() || data_value.is_null() {
        return Ok(None);
    }
    let reference: BlobRef = serde_wasm_bindgen::from_value(meta_value)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    Ok(Some(crate::StoredBlob {
        reference,
        data: crate::BinaryBytes::from(js_sys::Uint8Array::new(&data_value).to_vec()),
    }))
}

async fn has_blob_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
    blob_id: &str,
) -> std::result::Result<bool, JsValue> {
    Ok(
        load_blob_indexed_db(database_name, store_name, namespace, blob_id)
            .await?
            .is_some(),
    )
}

fn segment_namespace_prefix(namespace: &str) -> String {
    format!("segment/{namespace}/")
}

fn indexed_db_prefix_upper_bound(prefix: &str) -> String {
    format!("{prefix}\u{10ffff}")
}

fn indexed_db_prefix_range(prefix: &str) -> std::result::Result<web_sys::IdbKeyRange, JsValue> {
    web_sys::IdbKeyRange::bound(
        &JsValue::from_str(prefix),
        &JsValue::from_str(&indexed_db_prefix_upper_bound(prefix)),
    )
}

async fn list_indexed_db_keys_with_prefix(
    database_name: &str,
    store_name: &str,
    prefix: &str,
) -> std::result::Result<Vec<String>, JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let tx = db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readonly)?;
    let store = tx.object_store(store_name)?;
    let range = indexed_db_prefix_range(prefix)?;
    let request = store.get_all_keys_with_key(&range)?;
    let keys_value = await_idb_request(request.unchecked_ref()).await?;
    await_idb_transaction(&tx).await?;
    let keys_array = js_sys::Array::from(&keys_value);
    let mut keys = Vec::with_capacity(keys_array.length() as usize);
    for value in keys_array.iter() {
        let key = value
            .as_string()
            .ok_or_else(|| JsValue::from_str("IndexedDB key is not a string"))?;
        keys.push(key);
    }
    Ok(keys)
}

fn build_segment_transaction_entries(
    namespace: &str,
    transaction: &crate::StorageTransaction,
) -> std::result::Result<Vec<(String, JsValue)>, JsValue> {
    let prefix = segment_namespace_prefix(namespace);
    let mut entries = Vec::with_capacity(1 + transaction.nodes.len() + transaction.auth_meta.len());

    entries.push((format!("{prefix}meta"), to_js(&transaction.metadata)?));

    for (node_id, node_state) in &transaction.nodes {
        entries.push((
            format!("{prefix}node/{}", encode_component(node_id)),
            to_js(node_state)?,
        ));
    }

    for (node_id, auth_meta) in &transaction.auth_meta {
        entries.push((
            format!("{prefix}auth/{}", encode_component(node_id)),
            to_js(auth_meta)?,
        ));
    }

    Ok(entries)
}

fn estimated_indexed_db_entry_bytes(key: &str, value: &JsValue) -> u64 {
    let value_bytes = js_sys::JSON::stringify(value)
        .ok()
        .and_then(|value| value.as_string())
        .map(|value| value.len() as u64)
        .unwrap_or(0);
    key.len() as u64 + value_bytes
}

async fn replace_segment_transaction_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
    transaction: &crate::StorageTransaction,
) -> std::result::Result<WasmSegmentWriteSummary, JsValue> {
    let prefix = segment_namespace_prefix(namespace);
    let stale_keys = list_indexed_db_keys_with_prefix(database_name, store_name, &prefix).await?;
    let entries = build_segment_transaction_entries(namespace, transaction)?;
    let estimated_bytes_written = entries
        .iter()
        .map(|(key, value)| estimated_indexed_db_entry_bytes(key, value))
        .sum();
    let db = open_indexed_db(database_name, store_name).await?;
    let tx =
        db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readwrite)?;
    let store = tx.object_store(store_name)?;
    let entries_deleted = stale_keys.len() as u64;
    let entries_written = entries.len() as u64;

    for key in stale_keys {
        let _ = store.delete(&JsValue::from_str(&key))?;
    }

    for (key, value) in entries {
        let _ = store.put_with_key(&value, &JsValue::from_str(&key))?;
    }

    await_idb_transaction(&tx).await?;
    Ok(WasmSegmentWriteSummary {
        entries_written,
        entries_deleted,
        estimated_bytes_written,
    })
}

async fn apply_segment_transaction_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
    transaction: &crate::StorageTransaction,
) -> std::result::Result<WasmSegmentWriteSummary, JsValue> {
    let prefix = segment_namespace_prefix(namespace);
    let mut touched_nodes = crate::touched_nodes(&transaction.journal_ops);
    touched_nodes.extend(transaction.nodes.keys().cloned());
    let entries = build_segment_transaction_entries(namespace, transaction)?;
    let estimated_bytes_written = entries
        .iter()
        .map(|(key, value)| estimated_indexed_db_entry_bytes(key, value))
        .sum();

    let db = open_indexed_db(database_name, store_name).await?;
    let tx =
        db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readwrite)?;
    let store = tx.object_store(store_name)?;
    let mut entries_deleted = 0_u64;

    for node_id in &touched_nodes {
        let encoded_node = encode_component(node_id);
        if !transaction.nodes.contains_key(node_id) {
            for key in [
                format!("{prefix}node/{encoded_node}"),
                format!("{prefix}auth/{encoded_node}"),
            ] {
                let _ = store.delete(&JsValue::from_str(&key))?;
                entries_deleted = entries_deleted.saturating_add(1);
            }
        }
    }

    let entries_written = entries.len() as u64;
    for (key, value) in entries {
        let _ = store.put_with_key(&value, &JsValue::from_str(&key))?;
    }

    await_idb_transaction(&tx).await?;
    Ok(WasmSegmentWriteSummary {
        entries_written,
        entries_deleted,
        estimated_bytes_written,
    })
}

async fn estimate_segment_namespace_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
) -> std::result::Result<WasmSegmentStorageEstimate, JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let tx = db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readonly)?;
    let store = tx.object_store(store_name)?;
    let prefix = segment_namespace_prefix(namespace);
    let range = indexed_db_prefix_range(&prefix)?;
    let keys_request = store.get_all_keys_with_key(&range)?;
    let values_request = store.get_all_with_key(&range)?;
    let keys_value = await_idb_request(keys_request.unchecked_ref()).await?;
    let values_value = await_idb_request(values_request.unchecked_ref()).await?;
    await_idb_transaction(&tx).await?;

    let keys_array = js_sys::Array::from(&keys_value);
    let values_array = js_sys::Array::from(&values_value);
    if keys_array.length() != values_array.length() {
        return Err(JsValue::from_str(
            "IndexedDB returned mismatched key/value counts for segment estimate",
        ));
    }

    let mut estimated_bytes = 0_u64;
    for index in 0..keys_array.length() {
        let key = keys_array
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("IndexedDB key is not a string"))?;
        let value = values_array.get(index);
        estimated_bytes =
            estimated_bytes.saturating_add(estimated_indexed_db_entry_bytes(&key, &value));
    }

    Ok(WasmSegmentStorageEstimate {
        key_count: keys_array.length() as u64,
        estimated_bytes,
        origin_usage: None,
        origin_quota: None,
    })
}

async fn load_segment_snapshot_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
) -> std::result::Result<Option<crate::DatabaseSnapshot>, JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let tx = db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readonly)?;
    let store = tx.object_store(store_name)?;
    let prefix = segment_namespace_prefix(namespace);
    let range = indexed_db_prefix_range(&prefix)?;
    let keys_request = store.get_all_keys_with_key(&range)?;
    let values_request = store.get_all_with_key(&range)?;
    let node_prefix = format!("{prefix}node/");
    let mut metadata: Option<crate::StorageMetadata> = None;
    let mut nodes = BTreeMap::new();

    let keys_value = await_idb_request(keys_request.unchecked_ref()).await?;
    let values_value = await_idb_request(values_request.unchecked_ref()).await?;
    await_idb_transaction(&tx).await?;

    let keys_array = js_sys::Array::from(&keys_value);
    let values_array = js_sys::Array::from(&values_value);
    if keys_array.length() != values_array.length() {
        return Err(JsValue::from_str(
            "IndexedDB returned mismatched key/value counts for segment snapshot",
        ));
    }

    for index in 0..keys_array.length() {
        let key = keys_array
            .get(index)
            .as_string()
            .ok_or_else(|| JsValue::from_str("IndexedDB key is not a string"))?;
        let value = values_array.get(index);

        if key == format!("{prefix}meta") {
            metadata = Some(
                serde_wasm_bindgen::from_value(value)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?,
            );
        } else if let Some(encoded_node) = key.strip_prefix(&node_prefix) {
            let node_id = crate::engine::decode_component(encoded_node).map_err(to_js_error)?;
            let node_state: crate::NodeState = serde_wasm_bindgen::from_value(value)
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            nodes.insert(node_id, node_state);
        }
    }

    Ok(metadata.map(|metadata| crate::DatabaseSnapshot {
        clock: metadata.clock,
        nodes,
        pending_ops: metadata.pending_ops,
        scope_policies: metadata.scope_policies,
        provisional_transactions: metadata.provisional_transactions,
        next_provisional_transaction_id: metadata.next_provisional_transaction_id,
    }))
}

async fn open_indexed_db(
    database_name: &str,
    store_name: &str,
) -> std::result::Result<web_sys::IdbDatabase, JsValue> {
    let factory = browser_window()?
        .indexed_db()?
        .ok_or_else(|| JsValue::from_str("IndexedDB is unavailable in this browser context"))?;
    let request = factory.open(database_name)?;

    let upgrade_request = request.clone();
    let store_name = store_name.to_owned();
    let on_upgrade = Closure::wrap(Box::new(move |_event: web_sys::IdbVersionChangeEvent| {
        let Ok(result) = upgrade_request.result() else {
            return;
        };
        let Ok(database) = result.dyn_into::<web_sys::IdbDatabase>() else {
            return;
        };
        let _ = database.create_object_store(&store_name);
    }) as Box<dyn FnMut(_)>);
    request.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));

    let result = await_idb_request(request.unchecked_ref()).await?;
    request.set_onupgradeneeded(None);
    drop(on_upgrade);
    result.dyn_into::<web_sys::IdbDatabase>()
}

async fn await_idb_request(request: &web_sys::IdbRequest) -> std::result::Result<JsValue, JsValue> {
    let request_success = request.clone();
    let request_error = request.clone();
    let request_for_setters = request.clone();

    let promise = js_sys::Promise::new(&mut move |resolve, reject| {
        let resolve_fn = resolve.clone();
        let reject_fn = reject.clone();
        let request_success = request_success.clone();
        let request_error = request_error.clone();
        let success = Closure::once(
            move |_event: web_sys::Event| match request_success.result() {
                Ok(value) => {
                    let _ = resolve_fn.call1(&JsValue::NULL, &value);
                }
                Err(error) => {
                    let _ = reject.call1(&JsValue::NULL, &error);
                }
            },
        );

        let error = Closure::once(move |_event: web_sys::Event| {
            let error = request_error
                .error()
                .ok()
                .flatten()
                .map(JsValue::from)
                .unwrap_or_else(|| JsValue::from_str("IndexedDB request failed"));
            let _ = reject_fn.call1(&JsValue::NULL, &error);
        });

        request_for_setters.set_onsuccess(Some(success.as_ref().unchecked_ref()));
        request_for_setters.set_onerror(Some(error.as_ref().unchecked_ref()));
        success.forget();
        error.forget();
    });

    let result = JsFuture::from(promise).await;
    request.set_onsuccess(None);
    request.set_onerror(None);
    result
}

async fn await_idb_transaction(
    transaction: &web_sys::IdbTransaction,
) -> std::result::Result<(), JsValue> {
    let transaction_complete = transaction.clone();
    let transaction_error = transaction.clone();
    let transaction_abort = transaction.clone();
    let transaction_for_setters = transaction.clone();

    let promise = js_sys::Promise::new(&mut move |resolve, reject| {
        let resolve_fn = resolve.clone();
        let reject_error = reject.clone();
        let reject_abort = reject.clone();
        let transaction_error = transaction_error.clone();
        let transaction_abort = transaction_abort.clone();

        let complete = Closure::once(move |_event: web_sys::Event| {
            let _ = resolve_fn.call0(&JsValue::NULL);
        });

        let error = Closure::once(move |_event: web_sys::Event| {
            let error = transaction_error
                .error()
                .map(JsValue::from)
                .unwrap_or_else(|| JsValue::from_str("IndexedDB transaction failed"));
            let _ = reject_error.call1(&JsValue::NULL, &error);
        });

        let abort = Closure::once(move |_event: web_sys::Event| {
            let error = transaction_abort
                .error()
                .map(JsValue::from)
                .unwrap_or_else(|| JsValue::from_str("IndexedDB transaction aborted"));
            let _ = reject_abort.call1(&JsValue::NULL, &error);
        });

        transaction_for_setters.set_oncomplete(Some(complete.as_ref().unchecked_ref()));
        transaction_for_setters.set_onerror(Some(error.as_ref().unchecked_ref()));
        transaction_for_setters.set_onabort(Some(abort.as_ref().unchecked_ref()));
        complete.forget();
        error.forget();
        abort.forget();
    });

    let result = JsFuture::from(promise).await.map(|_| ());
    transaction_complete.set_oncomplete(None);
    transaction_complete.set_onerror(None);
    transaction_complete.set_onabort(None);
    result
}

fn parse_mesh_config(
    room: String,
    retry_interval_ms: Option<i32>,
    options: Option<JsValue>,
) -> std::result::Result<MeshConfig, JsValue> {
    let mut config = match options {
        None => MeshConfig::broadcast(room.clone()),
        Some(value) if value.is_null() || value.is_undefined() => {
            MeshConfig::broadcast(room.clone())
        }
        Some(value) => serde_wasm_bindgen::from_value(value)
            .map_err(|error| JsValue::from_str(&error.to_string()))?,
    };
    config.room = room;
    if let Some(retry_interval_ms) = retry_interval_ms {
        config.retry_interval_ms = retry_interval_ms.max(1) as u64;
    }
    Ok(config)
}

fn mesh_websocket_relay_url(config: &MeshConfig) -> std::result::Result<String, JsValue> {
    if let Some(url) = config.relay_url.clone() {
        return Ok(url);
    }
    match config.relay_endpoint.as_ref() {
        Some(RelayEndpointConfig::WebSocket(endpoint)) => Ok(endpoint.url.clone()),
        Some(RelayEndpointConfig::Moq(_)) => Err(JsValue::from_str(
            "MoQ mesh signaling requires connectMeshWithExternalSignaling",
        )),
        None => Err(JsValue::from_str(
            "relay mesh signaling requires a relayUrl",
        )),
    }
}

fn mesh_external_signaling_metadata(config: &MeshConfig) -> (String, Option<String>) {
    match config.relay_endpoint.as_ref() {
        Some(RelayEndpointConfig::Moq(endpoint)) => ("moq".to_owned(), Some(endpoint.url.clone())),
        Some(RelayEndpointConfig::WebSocket(endpoint)) => {
            ("websocket".to_owned(), Some(endpoint.url.clone()))
        }
        None => ("external".to_owned(), config.relay_url.clone()),
    }
}

fn build_web_rtc_configuration(
    ice_servers: &[IceServerConfig],
) -> std::result::Result<JsValue, JsValue> {
    let config = js_sys::Object::new();
    let servers = js_sys::Array::new();
    for server in ice_servers.iter().cloned() {
        let server_value = js_sys::Object::new();
        let urls = server.urls.into_vec();
        let urls_value = if urls.len() == 1 {
            JsValue::from_str(&urls[0])
        } else {
            let array = js_sys::Array::new();
            for url in urls {
                array.push(&JsValue::from_str(&url));
            }
            array.into()
        };
        js_sys::Reflect::set(&server_value, &JsValue::from_str("urls"), &urls_value)?;
        if let Some(username) = server.username {
            js_sys::Reflect::set(
                &server_value,
                &JsValue::from_str("username"),
                &JsValue::from_str(&username),
            )?;
        }
        if let Some(credential) = server.credential {
            js_sys::Reflect::set(
                &server_value,
                &JsValue::from_str("credential"),
                &JsValue::from_str(&credential),
            )?;
        }
        servers.push(&server_value);
    }
    js_sys::Reflect::set(&config, &JsValue::from_str("iceServers"), &servers.into())?;
    Ok(config.into())
}

fn browser_window() -> std::result::Result<web_sys::Window, JsValue> {
    web_sys::window().ok_or_else(|| JsValue::from_str("browser window is unavailable"))
}

fn js_to_json(value: JsValue) -> std::result::Result<JsonValue, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn parse_wasm_network_hook_callbacks(
    hooks: JsValue,
) -> std::result::Result<WasmNetworkHookCallbacks, JsValue> {
    if !hooks.is_object() {
        return Err(JsValue::from_str(
            "network hooks must be an object with optional callback functions",
        ));
    }
    Ok(WasmNetworkHookCallbacks {
        on_connect: js_function_property(&hooks, "onConnect")?,
        on_join_room: js_function_property(&hooks, "onJoinRoom")?,
        on_pull: js_function_property(&hooks, "onPull")?,
        on_watch: js_function_property(&hooks, "onWatch")?,
        on_serve_result: js_function_property(&hooks, "onServeResult")?,
    })
}

fn js_function_property(
    object: &JsValue,
    key: &str,
) -> std::result::Result<Option<js_sys::Function>, JsValue> {
    let value = js_sys::Reflect::get(object, &JsValue::from_str(key))?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    value
        .dyn_into::<js_sys::Function>()
        .map(Some)
        .map_err(|_| JsValue::from_str(&format!("network hook `{key}` must be a function")))
}

fn call_hook1<T: Serialize>(
    function: &js_sys::Function,
    arg: &T,
) -> std::result::Result<JsValue, String> {
    let arg = serde_wasm_bindgen::to_value(arg).map_err(|error| error.to_string())?;
    function
        .call1(&JsValue::NULL, &arg)
        .map_err(js_hook_error_string)
}

fn call_hook2<A: Serialize, B: Serialize>(
    function: &js_sys::Function,
    arg_a: &A,
    arg_b: &B,
) -> std::result::Result<JsValue, String> {
    let arg_a = serde_wasm_bindgen::to_value(arg_a).map_err(|error| error.to_string())?;
    let arg_b = serde_wasm_bindgen::to_value(arg_b).map_err(|error| error.to_string())?;
    function
        .call2(&JsValue::NULL, &arg_a, &arg_b)
        .map_err(js_hook_error_string)
}

fn parse_void_hook_decision(
    response: std::result::Result<JsValue, String>,
    default_message: &str,
) -> crate::HookDecision<()> {
    match response {
        Ok(value) => match js_hook_json(value) {
            Ok(value) => crate::parse_void_hook_json(value, default_message),
            Err(message) => crate::HookDecision::deny(message),
        },
        Err(message) => crate::HookDecision::deny(message),
    }
}

fn parse_request_hook_decision(
    response: std::result::Result<JsValue, String>,
    default_request: &PullRequestKind,
    default_message: &str,
) -> crate::HookDecision<PullRequestKind> {
    match response {
        Ok(value) => match js_hook_json(value) {
            Ok(value) => crate::parse_request_hook_json(value, default_request, default_message),
            Err(message) => crate::HookDecision::deny(message),
        },
        Err(message) => crate::HookDecision::deny(message),
    }
}

fn parse_result_hook_decision(
    response: std::result::Result<JsValue, String>,
    default_result: RemoteResult,
    default_message: &str,
) -> crate::HookDecision<RemoteResult> {
    match response {
        Ok(value) => match js_hook_json(value) {
            Ok(value) => crate::parse_result_hook_json(value, default_result, default_message),
            Err(message) => crate::HookDecision::deny(message),
        },
        Err(message) => crate::HookDecision::deny(message),
    }
}

fn js_hook_json(value: JsValue) -> std::result::Result<Option<JsonValue>, String> {
    if value.is_null() || value.is_undefined() {
        Ok(None)
    } else {
        js_to_json(value).map(Some).map_err(js_hook_error_string)
    }
}

fn js_hook_error_string(error: JsValue) -> String {
    if let Some(message) = error.as_string() {
        return message;
    }
    if let Ok(message) = js_sys::Reflect::get(&error, &JsValue::from_str("message"))
        && let Some(message) = message.as_string()
    {
        return message;
    }
    js_sys::JSON::stringify(&error)
        .ok()
        .and_then(|value| value.as_string())
        .unwrap_or_else(|| "javascript network hook threw".to_owned())
}

fn js_to_supported_json(value: JsValue) -> std::result::Result<JsonValue, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(JsonValue::Null);
    }
    if js_sys::ArrayBuffer::instanceof(&value) {
        let bytes = js_sys::Uint8Array::new(&value).to_vec();
        return Ok(serde_json::json!({
            "$bytes": crate::BinaryBytes::from(bytes).to_base64(),
        }));
    }
    if js_sys::Uint8Array::instanceof(&value) {
        let bytes = js_sys::Uint8Array::new(&value).to_vec();
        return Ok(serde_json::json!({
            "$bytes": crate::BinaryBytes::from(bytes).to_base64(),
        }));
    }
    if js_sys::Array::is_array(&value) {
        let array = js_sys::Array::from(&value);
        let mut items = Vec::with_capacity(array.length() as usize);
        for index in 0..array.length() {
            items.push(js_to_supported_json(array.get(index))?);
        }
        return Ok(JsonValue::Array(items));
    }
    if value.is_object() {
        let object = js_sys::Object::from(value.clone());
        let keys = js_sys::Object::keys(&object);
        let mut map = serde_json::Map::new();
        for index in 0..keys.length() {
            let key = keys.get(index).as_string().unwrap_or_default();
            let field = js_sys::Reflect::get(&object, &JsValue::from_str(&key))?;
            map.insert(key, js_to_supported_json(field)?);
        }
        return Ok(JsonValue::Object(map));
    }
    js_to_json(value)
}

fn json_to_supported_js(value: &JsonValue) -> std::result::Result<JsValue, JsValue> {
    match value {
        JsonValue::Null => Ok(JsValue::NULL),
        JsonValue::Bool(value) => Ok(JsValue::from_bool(*value)),
        JsonValue::Number(value) => serde_wasm_bindgen::to_value(value)
            .map_err(|error| JsValue::from_str(&error.to_string())),
        JsonValue::String(value) => Ok(JsValue::from_str(value)),
        JsonValue::Array(items) => {
            let array = js_sys::Array::new();
            for item in items {
                array.push(&json_to_supported_js(item)?);
            }
            Ok(array.into())
        }
        JsonValue::Object(object) => {
            if object.len() == 1 {
                if let Some(JsonValue::String(encoded)) = object.get("$bytes") {
                    let bytes = crate::BinaryBytes::from_base64(encoded)
                        .map_err(|error| JsValue::from_str(&error))?;
                    return Ok(js_sys::Uint8Array::from(bytes.as_slice()).into());
                }
            }
            let js_object = js_sys::Object::new();
            for (key, value) in object {
                js_sys::Reflect::set(
                    &js_object,
                    &JsValue::from_str(key),
                    &json_to_supported_js(value)?,
                )?;
            }
            Ok(js_object.into())
        }
    }
}

fn map_entry_to_js(entry: &MapEntry) -> std::result::Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("key"),
        &JsValue::from_str(&entry.key),
    )?;
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("value"),
        &json_to_supported_js(&entry.value)?,
    )?;
    Ok(object.into())
}

fn map_entries_to_js(entries: &[MapEntry]) -> std::result::Result<JsValue, JsValue> {
    let array = js_sys::Array::new();
    for entry in entries {
        array.push(&map_entry_to_js(entry)?);
    }
    Ok(array.into())
}

fn lex_entry_to_js(entry: &LexEntry) -> std::result::Result<JsValue, JsValue> {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("path"),
        &JsValue::from_str(&entry.path),
    )?;
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("key"),
        &JsValue::from_str(&entry.key),
    )?;
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("value"),
        &json_to_supported_js(&entry.value)?,
    )?;
    Ok(object.into())
}

fn lex_entries_to_js(entries: &[LexEntry]) -> std::result::Result<JsValue, JsValue> {
    let array = js_sys::Array::new();
    for entry in entries {
        array.push(&lex_entry_to_js(entry)?);
    }
    Ok(array.into())
}

fn to_js<T>(value: &T) -> std::result::Result<JsValue, JsValue>
where
    T: Serialize,
{
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn to_js_error(error: impl ToString) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn js_error_string(error: JsValue) -> String {
    error
        .as_string()
        .or_else(|| {
            js_sys::Reflect::get(&error, &JsValue::from_str("message"))
                .ok()
                .and_then(|message| message.as_string())
        })
        .unwrap_or_else(|| format!("{error:?}"))
}
