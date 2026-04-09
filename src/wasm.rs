use crate::{
    Chain, ChangeSubscription, DurableStorageBinding, DurableStorageConfig, HybridClock,
    IceServerConfig, LexEntry, LexSpec, MapEntry, MeshConfig, MeshSignal, MeshSignalingMode,
    Operation, PeerRecommendation, Primadb, PullRequest, PullRequestKind,
    PullResponse, PullResponseBody, QuerySpec, RelayClientConfig, RemotePath, RemoteResult,
    RouteBatchItem, RouteEnvelope, RoutePayload, RouteTarget, Router, RouterConfig, Subscription,
    SyncEnvelope, SyncFrame, build_storage_metadata, build_storage_transaction,
    encode_component,
};
#[cfg(feature = "crypto")]
use crate::SecureSyncFrame;
use async_channel::{Sender, bounded};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
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
}

#[wasm_bindgen(js_name = Chain)]
pub struct WasmChain {
    inner: Chain,
}

#[wasm_bindgen(js_name = Subscription)]
pub struct WasmSubscription {
    inner: Option<Subscription>,
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
    subscription: Option<ChangeSubscription>,
}

#[allow(dead_code)]
enum WasmDurableStorageHook {
    Snapshot {
        _hook: WasmIndexedDbPersistence,
    },
    Segment {
        _hook: WasmIndexedDbSegmentPersistence,
    },
}

#[derive(Debug)]
struct WebSocketSyncState {
    db: Primadb,
    router: Router,
    socket: web_sys::WebSocket,
    inflight: BTreeMap<String, OutboundSync>,
    pending_requests: BTreeMap<String, PendingPullRequest>,
    recommendations: BTreeMap<String, PeerRecommendation>,
    next_message_seq: u64,
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
    Snapshot {
        clock: Option<HybridClock>,
        nodes: BTreeMap<String, crate::NodeState>,
        pending_ops: Vec<Operation>,
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
}

struct WebRtcMeshState {
    db: Primadb,
    router: Router,
    room: String,
    peer_id: String,
    signaling: MeshSignalingTransport,
    rtc_configuration: JsValue,
    peers: BTreeMap<String, MeshPeer>,
    inflight: BTreeMap<String, MeshOutbound>,
    next_message_seq: u64,
}

#[wasm_bindgen(js_name = WebRtcMesh)]
pub struct WasmWebRtcMesh {
    state: Rc<RefCell<WebRtcMeshState>>,
    change_subscription: Option<ChangeSubscription>,
    signaling_onmessage: Option<Closure<dyn FnMut(web_sys::MessageEvent)>>,
    relay_onopen: Option<Closure<dyn FnMut(web_sys::Event)>>,
    relay_onclose: Option<Closure<dyn FnMut(web_sys::CloseEvent)>>,
    relay_onerror: Option<Closure<dyn FnMut(web_sys::Event)>>,
    retry_callback: Option<Closure<dyn FnMut()>>,
    retry_interval_id: Option<i32>,
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
        }
    }

    #[wasm_bindgen(js_name = replicaId)]
    pub fn replica_id(&self) -> String {
        self.inner.replica_id()
    }

    pub fn chain(&self, root: String) -> WasmChain {
        WasmChain {
            inner: self.inner.root(root),
        }
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
        self.inner
            .merge_snapshot_json(payload)
            .map_err(to_js_error)
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
                }
            }
            DurableStorageConfig::SnapshotFile { .. } | DurableStorageConfig::SegmentFiles { .. } => {
                return Err(JsValue::from_str(
                    "native durable storage config is not available in the browser",
                ));
            }
        };
        to_js(&binding)
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
            while let Ok(event) = receiver.recv().await {
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
        let snapshot = self.inner.snapshot();
        let metadata = build_storage_metadata(snapshot.clock, snapshot.pending_ops, 1);
        let transaction = build_storage_transaction(0, metadata, snapshot.nodes);
        save_segment_transaction_indexed_db(&database_name, &store_name, &namespace, &transaction)
            .await
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

        let subscription = self.inner.subscribe_changes();
        let receiver = subscription.receiver();
        let db = self.inner.clone();
        let db_name = database_name.clone();
        let store = store_name.clone();
        let namespace_key = namespace.clone();
        spawn_local(async move {
            while let Ok(event) = receiver.recv().await {
                if !event.data_changed && event.pending_ops == 0 {
                    continue;
                }
                let snapshot = db.snapshot();
                let metadata = build_storage_metadata(snapshot.clock, snapshot.pending_ops, 1);
                let transaction = build_storage_transaction(0, metadata, snapshot.nodes);
                let _ = save_segment_transaction_indexed_db(
                    &db_name,
                    &store,
                    &namespace_key,
                    &transaction,
                )
                .await;
            }
        });

        let hook = WasmIndexedDbSegmentPersistence {
            db: self.inner.clone(),
            database_name,
            store_name,
            namespace,
            subscription: Some(subscription),
        };
        hook.flush().await?;
        Ok(hook)
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
        })
    }

    #[wasm_bindgen(js_name = connectRelay)]
    pub fn connect_relay(&self, config: JsValue) -> std::result::Result<WasmWebSocketSync, JsValue> {
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
}

impl WasmPrimadb {
    fn connect_relay_config(
        &self,
        config: RelayClientConfig,
    ) -> std::result::Result<WasmWebSocketSync, JsValue> {
        let url = config.url;
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
            socket: socket.clone(),
            inflight: BTreeMap::new(),
            pending_requests: BTreeMap::new(),
            recommendations: BTreeMap::new(),
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
                borrowed.router.presence(
                    borrowed.db.replica_id(),
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
                )
            };
            if let RoutePayload::Presence { peer } = &mut route.payload {
                peer.metadata
                    .insert("relay_url".to_owned(), relay_url.clone());
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
        }) as Box<dyn FnMut(_)>);
        socket.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        let onerror_state = state.clone();
        let onerror = Closure::wrap(Box::new(move |_event: web_sys::Event| {
            requeue_inflight_state(&onerror_state);
            fail_pending_requests_state(
                &onerror_state,
                "websocket errored while requests were pending",
            );
        }) as Box<dyn FnMut(_)>);
        socket.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        let change_subscription = self.inner.subscribe_changes();
        let receiver = change_subscription.receiver();
        let change_state = state.clone();
        spawn_local(async move {
            while let Ok(event) = receiver.recv().await {
                if event.pending_ops > 0 {
                    let _ = flush_pending_state(&change_state);
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

        Ok(WasmWebSocketSync {
            state,
            change_subscription: Some(change_subscription),
            onmessage: Some(onmessage),
            onopen: Some(onopen),
            onclose: Some(onclose),
            onerror: Some(onerror),
            interval_callback: Some(interval_callback),
            interval_id: Some(interval_id),
        })
    }

    fn connect_mesh_config(&self, config: MeshConfig) -> std::result::Result<WasmWebRtcMesh, JsValue> {
        let room = config.room.clone();
        let rtc_configuration = build_web_rtc_configuration(&config.effective_ice_servers())?;
        let peer_id = format!(
            "mesh:{}:{}",
            self.inner.replica_id(),
            js_sys::Date::now() as u64
        );
        let signaling = match config.signaling {
            MeshSignalingMode::BroadcastChannel => {
                MeshSignalingTransport::BroadcastChannel(web_sys::BroadcastChannel::new(
                    &format!("primadb-mesh-{room}"),
                )?)
            }
            MeshSignalingMode::Relay => {
                let url = config
                    .relay_url
                    .clone()
                    .ok_or_else(|| JsValue::from_str("relay mesh signaling requires a relayUrl"))?;
                let socket = web_sys::WebSocket::new(&url)?;
                socket.set_binary_type(web_sys::BinaryType::Arraybuffer);
                MeshSignalingTransport::Relay {
                    socket,
                    relay_url: url,
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
            signaling,
            rtc_configuration,
            peers: BTreeMap::new(),
            inflight: BTreeMap::new(),
            next_message_seq: 0,
        }));
        let (signaling_onmessage, relay_onopen, relay_onclose, relay_onerror) =
            match state.borrow().signaling.clone() {
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
                (Some(onmessage), None, None, None)
            }
            MeshSignalingTransport::Relay { socket, relay_url } => {
                let onmessage_state = state.clone();
                let onmessage = Closure::wrap(Box::new(move |event: web_sys::MessageEvent| {
                    if let Some(payload) = event.data().as_string() {
                        let _ = handle_mesh_signaling_websocket_message(&onmessage_state, &payload);
                    }
                }) as Box<dyn FnMut(_)>);
                socket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

                let onopen_state = state.clone();
                let onopen = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                    let _ = send_mesh_presence_state(&onopen_state, &relay_url);
                    let _ = announce_mesh_join_state(&onopen_state);
                    let _ = retry_mesh_inflight_state(&onopen_state);
                    let _ = flush_mesh_pending_state(&onopen_state);
                }) as Box<dyn FnMut(_)>);
                socket.set_onopen(Some(onopen.as_ref().unchecked_ref()));

                let onclose = Closure::wrap(Box::new(move |_event: web_sys::CloseEvent| {
                    // Existing peer data channels may remain alive after signaling disconnects.
                }) as Box<dyn FnMut(_)>);
                socket.set_onclose(Some(onclose.as_ref().unchecked_ref()));

                let onerror = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                    // Keep the current mesh alive for already-open data channels.
                }) as Box<dyn FnMut(_)>);
                socket.set_onerror(Some(onerror.as_ref().unchecked_ref()));
                (Some(onmessage), Some(onopen), Some(onclose), Some(onerror))
            }
        };

        let change_subscription = self.inner.subscribe_changes();
        let receiver = change_subscription.receiver();
        let change_state = state.clone();
        spawn_local(async move {
            while let Ok(event) = receiver.recv().await {
                if event.pending_ops > 0 {
                    let _ = flush_mesh_pending_state(&change_state);
                }
            }
        });

        let retry_ms = config.retry_interval_ms.min(i32::MAX as u64) as i32;
        let retry_state = state.clone();
        let retry_callback = Closure::wrap(Box::new(move || {
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

        Ok(WasmWebRtcMesh {
            state,
            change_subscription: Some(change_subscription),
            signaling_onmessage,
            relay_onopen,
            relay_onclose,
            relay_onerror,
            retry_callback: Some(retry_callback),
            retry_interval_id: Some(retry_interval_id),
        })
    }
}

#[cfg(feature = "crypto")]
#[wasm_bindgen(js_name = generateSeaPair)]
pub fn generate_sea_pair() -> std::result::Result<JsValue, JsValue> {
    to_js(&crate::SeaPair::generate())
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
        }
    }

    pub fn path(&self) -> String {
        self.inner.path()
    }

    pub fn put(&self, value: JsValue) -> std::result::Result<(), JsValue> {
        self.inner.put(js_to_json(value)?).map_err(to_js_error)
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = putSigned)]
    pub fn put_signed(
        &self,
        value: JsValue,
        certificate: Option<String>,
    ) -> std::result::Result<(), JsValue> {
        self.inner
            .put_signed(js_to_json(value)?, certificate)
            .map_err(to_js_error)
    }

    pub fn once(&self) -> std::result::Result<JsValue, JsValue> {
        match self.inner.once_json().map_err(to_js_error)? {
            Some(value) => to_js(&value),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn set(&self, value: JsValue) -> std::result::Result<String, JsValue> {
        self.inner.set(js_to_json(value)?).map_err(to_js_error)
    }

    #[cfg(feature = "crypto")]
    #[wasm_bindgen(js_name = setSigned)]
    pub fn set_signed(
        &self,
        value: JsValue,
        certificate: Option<String>,
    ) -> std::result::Result<String, JsValue> {
        self.inner
            .set_signed(js_to_json(value)?, certificate)
            .map_err(to_js_error)
    }

    pub fn remove(&self, value: JsValue) -> std::result::Result<String, JsValue> {
        self.inner.remove(js_to_json(value)?).map_err(to_js_error)
    }

    pub fn unset(&self) -> std::result::Result<(), JsValue> {
        self.inner.unset().map_err(to_js_error)
    }

    pub fn map(&self) -> std::result::Result<JsValue, JsValue> {
        let entries = self.inner.map().map_err(to_js_error)?;
        to_js(&entries)
    }

    pub fn query(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let entries = self.inner.query(spec).map_err(to_js_error)?;
        to_js(&entries)
    }

    pub fn scan(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: LexSpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let entries = self.inner.scan(spec).map_err(to_js_error)?;
        to_js(&entries)
    }

    #[wasm_bindgen(js_name = firstQuery)]
    pub fn first_query(&self, spec: JsValue) -> std::result::Result<JsValue, JsValue> {
        let spec: QuerySpec = serde_wasm_bindgen::from_value(spec)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        match self.inner.first(spec).map_err(to_js_error)? {
            Some(value) => to_js(&value),
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
                    Some(value) => serde_wasm_bindgen::to_value(&value).unwrap_or(JsValue::NULL),
                    None => JsValue::NULL,
                };
                let _ = callback.call1(&JsValue::NULL, &js_value);
            }
        });

        Ok(WasmSubscription {
            inner: Some(subscription),
        })
    }
}

#[wasm_bindgen(js_class = Subscription)]
impl WasmSubscription {
    pub fn cancel(&mut self) {
        self.inner.take();
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
        let snapshot = self.db.snapshot();
        let metadata = build_storage_metadata(snapshot.clock, snapshot.pending_ops, 1);
        let transaction = build_storage_transaction(0, metadata, snapshot.nodes);
        save_segment_transaction_indexed_db(
            &self.database_name,
            &self.store_name,
            &self.namespace,
            &transaction,
        )
        .await
    }

    pub fn close(&mut self) {
        self.subscription.take();
    }
}

impl Drop for WasmIndexedDbSegmentPersistence {
    fn drop(&mut self) {
        self.subscription.take();
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
                Some(value) => to_js(&value),
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
            RemoteResult::Query { entries } => to_js(&entries),
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
            RemoteResult::Lex { entries } => to_js(&entries),
            other => Err(JsValue::from_str(&format!(
                "expected lex result, received {other:?}"
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
        }
    }

    #[wasm_bindgen(js_name = relayUrl)]
    pub fn relay_url(&self) -> Option<String> {
        match &self.state.borrow().signaling {
            MeshSignalingTransport::BroadcastChannel(_) => None,
            MeshSignalingTransport::Relay { relay_url, .. } => Some(relay_url.clone()),
        }
    }

    #[wasm_bindgen(js_name = signalingReadyState)]
    pub fn signaling_ready_state(&self) -> Option<u16> {
        match &self.state.borrow().signaling {
            MeshSignalingTransport::BroadcastChannel(_) => None,
            MeshSignalingTransport::Relay { socket, .. } => Some(socket.ready_state()),
        }
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
        self.relay_onopen.take();
        self.relay_onclose.take();
        self.relay_onerror.take();
        self.retry_callback.take();
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
    handle_route_payload(state, route.from, route.route_id, route.payload)
}

fn handle_route_payload(
    state: &Rc<RefCell<WebSocketSyncState>>,
    from: String,
    route_id: String,
    payload: RoutePayload,
) -> std::result::Result<(), JsValue> {
    let mut pending = vec![payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                store_peer_recommendations_state(
                    state,
                    vec![peer_recommendation_from_presence(&peer)],
                );
            }
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let response = {
                    let borrowed = state.borrow();
                    borrowed.router.snapshot_response(
                        root,
                        borrowed.db.snapshot(),
                        RouteTarget::Peer(from.clone()),
                    )
                };
                send_route_state(state, &response)?;
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state
                    .borrow()
                    .db
                    .load_snapshot(snapshot)
                    .map_err(to_js_error)?;
            }
            RoutePayload::Sync { encoding, payload } => {
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
                let result = db.execute_pull_request(&request).map_err(to_js_error)?;
                let items = db
                    .chunk_remote_result(&request.request_id, result)
                    .into_iter()
                    .map(|response| RouteBatchItem::PullResponse { response })
                    .collect::<Vec<_>>();
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
            RoutePayload::PeerExchange { peers } => {
                store_peer_recommendations_state(state, peers);
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

fn fail_pending_requests_state(state: &Rc<RefCell<WebSocketSyncState>>, message: &str) {
    let pending = {
        let mut borrowed = state.borrow_mut();
        std::mem::take(&mut borrowed.pending_requests)
    };
    for request in pending.into_values() {
        let _ = request.sender.try_send(Err(message.to_owned()));
    }
}

fn accept_pull_response_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
    response: PullResponse,
) -> std::result::Result<(), JsValue> {
    let mut borrowed = state.borrow_mut();
    let Some(request) = borrowed.pending_requests.get_mut(&response.request_id) else {
        return Ok(());
    };

    match &response.result {
        PullResponseBody::Get { value } => {
            request.accumulator = PullAccumulator::Get {
                value: value.clone(),
            };
        }
        PullResponseBody::Map { entries } => {
            if let PullAccumulator::Map { entries: current } = &mut request.accumulator {
                current.extend(entries.clone());
            }
        }
        PullResponseBody::Query { entries } => {
            if let PullAccumulator::Query { entries: current } = &mut request.accumulator {
                current.extend(entries.clone());
            }
        }
        PullResponseBody::Lex { entries } => {
            if let PullAccumulator::Lex { entries: current } = &mut request.accumulator {
                current.extend(entries.clone());
            }
        }
        PullResponseBody::Snapshot {
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
        PullResponseBody::Error { message } => {
            let request = borrowed
                .pending_requests
                .remove(&response.request_id)
                .unwrap();
            let _ = request.sender.try_send(Err(message.clone()));
            return Ok(());
        }
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
            PullRequestKind::Snapshot { .. } => Self::Snapshot {
                clock: None,
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
            },
        }
    }

    fn into_result(self) -> crate::Result<RemoteResult> {
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
                        crate::PrimadbError::Message(
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
        MeshSignalingTransport::Relay { socket, .. } => {
            let (router, room, max_bytes) = {
                let borrowed = state.borrow();
                (
                    borrowed.router.clone(),
                    borrowed.room.clone(),
                    borrowed.db.limits().max_route_payload_bytes,
                )
            };
            if socket.ready_state() != web_sys::WebSocket::OPEN {
                return Err(JsValue::from_str("mesh relay websocket is not connected"));
            }
            let route = router.wrap_signal(
                room,
                serde_json::to_value(signal).map_err(to_js_error)?,
                mesh_signal_target(signal),
            );
            send_websocket_route(&socket, max_bytes, &route)
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

fn send_mesh_presence_state(
    state: &Rc<RefCell<WebRtcMeshState>>,
    relay_url: &str,
) -> std::result::Result<(), JsValue> {
    let (socket, route, max_bytes) = {
        let borrowed = state.borrow();
        let MeshSignalingTransport::Relay { socket, .. } = &borrowed.signaling else {
            return Ok(());
        };
        let mut route = borrowed.router.presence(
            borrowed.db.replica_id(),
            "webrtc-relay",
            vec![
                "signal".to_owned(),
                "webrtc".to_owned(),
                "peer_exchange".to_owned(),
            ],
            vec![format!("mesh:{}", borrowed.room)],
        );
        if let RoutePayload::Presence { peer } = &mut route.payload {
            peer.metadata
                .insert("relay_url".to_owned(), relay_url.to_owned());
            peer.metadata
                .insert("mesh_room".to_owned(), borrowed.room.clone());
            peer.metadata
                .insert("signaling".to_owned(), "relay".to_owned());
        }
        (
            socket.clone(),
            route,
            borrowed.db.limits().max_route_payload_bytes,
        )
    };
    if socket.ready_state() != web_sys::WebSocket::OPEN {
        return Err(JsValue::from_str("mesh relay websocket is not connected"));
    }
    send_websocket_route(&socket, max_bytes, &route)
}

fn handle_mesh_signaling_websocket_message(
    state: &Rc<RefCell<WebRtcMeshState>>,
    payload: &str,
) -> std::result::Result<(), JsValue> {
    let route: RouteEnvelope =
        serde_json::from_str(payload).map_err(|error| JsValue::from_str(&error.to_string()))?;
    let decision = {
        let borrowed = state.borrow();
        borrowed.router.accept(route.clone())
    };
    if !decision.deliver {
        return Ok(());
    }

    let room = state.borrow().room.clone();
    let channel = format!("mesh:{room}");
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { peer } => {
                let in_room = peer
                    .topics
                    .iter()
                    .any(|topic| topic == &channel)
                    || peer
                        .metadata
                        .get("mesh_room")
                        .is_some_and(|candidate| candidate == &room);
                if in_room {
                    handle_mesh_signal_state(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer.peer_id,
                        },
                    )?;
                }
            }
            RoutePayload::Signal { room: signal_room, payload } => {
                if signal_room != room {
                    continue;
                }
                let signal: MeshSignal = serde_json::from_value(payload).map_err(to_js_error)?;
                handle_mesh_signal_state(state, signal)?;
            }
            RoutePayload::PeerExchange { peers } => {
                for recommendation in peers {
                    let peer = recommendation.peer;
                    let in_room = peer
                        .topics
                        .iter()
                        .any(|topic| topic == &channel)
                        || peer
                            .metadata
                            .get("mesh_room")
                            .is_some_and(|candidate| candidate == &room);
                    if !in_room {
                        continue;
                    }
                    handle_mesh_signal_state(
                        state,
                        MeshSignal::Join {
                            room: room.clone(),
                            from: peer.peer_id,
                        },
                    )?;
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
            let (already_open, is_stale) = {
                let borrowed = state.borrow();
                let peer = borrowed.peers.get(&from);
                let already_open = peer
                    .and_then(|peer| peer.channel.as_ref())
                    .is_some_and(mesh_channel_is_open);
                let is_stale = peer.is_some_and(|peer| {
                    !peer
                        .channel
                        .as_ref()
                        .is_some_and(mesh_channel_is_open)
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
    let onopen = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        let _ = flush_mesh_pending_state(&open_state);
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
    let mut pending = vec![route.payload];
    while let Some(payload) = pending.pop() {
        match payload {
            RoutePayload::Presence { .. } => {}
            RoutePayload::Signal { .. } => {}
            RoutePayload::SnapshotRequest { root } => {
                let response = {
                    let borrowed = state.borrow();
                    borrowed.router.snapshot_response(
                        root,
                        borrowed.db.snapshot(),
                        RouteTarget::Peer(remote_peer.to_owned()),
                    )
                };
                send_mesh_route_to_peer(state, remote_peer, &response)?;
            }
            RoutePayload::SnapshotResponse { snapshot, .. } => {
                state
                    .borrow()
                    .db
                    .load_snapshot(snapshot)
                    .map_err(to_js_error)?;
            }
            RoutePayload::Sync { encoding, payload } => {
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
                let items = db
                    .chunk_remote_result(
                        &request.request_id,
                        db.execute_pull_request(&request).map_err(to_js_error)?,
                    )
                    .into_iter()
                    .map(|response| RouteBatchItem::PullResponse { response })
                    .collect::<Vec<_>>();
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
            RoutePayload::PeerExchange { .. } => {}
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

fn segment_namespace_prefix(namespace: &str) -> String {
    format!("segment/{namespace}/")
}

async fn save_segment_transaction_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
    transaction: &crate::StorageTransaction,
) -> std::result::Result<(), JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let tx = db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readwrite)?;
    let store = tx.object_store(store_name)?;
    let _ = await_idb_request(store.clear()?.unchecked_ref()).await?;

    let prefix = segment_namespace_prefix(namespace);
    let metadata_key = format!("{prefix}meta");
    let metadata_value =
        serde_wasm_bindgen::to_value(&transaction.metadata).map_err(|error| JsValue::from_str(&error.to_string()))?;
    let _ = await_idb_request(
        store
            .put_with_key(&metadata_value, &JsValue::from_str(&metadata_key))?
            .unchecked_ref(),
    )
    .await?;

    for (node_id, node_state) in &transaction.nodes {
        let key = format!("{prefix}node/{}", encode_component(node_id));
        let value =
            serde_wasm_bindgen::to_value(node_state).map_err(|error| JsValue::from_str(&error.to_string()))?;
        let _ = await_idb_request(
            store
                .put_with_key(&value, &JsValue::from_str(&key))?
                .unchecked_ref(),
        )
        .await?;
    }

    for (node_id, auth_meta) in &transaction.auth_meta {
        let key = format!("{prefix}auth/{}", encode_component(node_id));
        let value =
            serde_wasm_bindgen::to_value(auth_meta).map_err(|error| JsValue::from_str(&error.to_string()))?;
        let _ = await_idb_request(
            store
                .put_with_key(&value, &JsValue::from_str(&key))?
                .unchecked_ref(),
        )
        .await?;
    }

    for (node_id, manifest) in &transaction.node_indexes {
        let key = format!("{prefix}node_index/{}", encode_component(node_id));
        let value =
            serde_wasm_bindgen::to_value(manifest).map_err(|error| JsValue::from_str(&error.to_string()))?;
        let _ = await_idb_request(
            store
                .put_with_key(&value, &JsValue::from_str(&key))?
                .unchecked_ref(),
        )
        .await?;
    }

    for (key, entry) in &transaction.direct_indexes {
        let full_key = format!("{prefix}index/{key}");
        let value =
            serde_wasm_bindgen::to_value(entry).map_err(|error| JsValue::from_str(&error.to_string()))?;
        let _ = await_idb_request(
            store
                .put_with_key(&value, &JsValue::from_str(&full_key))?
                .unchecked_ref(),
        )
        .await?;
    }

    await_idb_transaction(&tx).await?;
    Ok(())
}

async fn load_segment_snapshot_indexed_db(
    database_name: &str,
    store_name: &str,
    namespace: &str,
) -> std::result::Result<Option<crate::DatabaseSnapshot>, JsValue> {
    let db = open_indexed_db(database_name, store_name).await?;
    let tx = db.transaction_with_str_and_mode(store_name, web_sys::IdbTransactionMode::Readonly)?;
    let store = tx.object_store(store_name)?;
    let request = store.open_cursor()?;
    let prefix = segment_namespace_prefix(namespace);
    let node_prefix = format!("{prefix}node/");
    let mut metadata: Option<crate::StorageMetadata> = None;
    let mut nodes = BTreeMap::new();

    let mut cursor = await_idb_request(request.unchecked_ref()).await?;
    while !cursor.is_null() && !cursor.is_undefined() {
        let current: web_sys::IdbCursorWithValue = cursor.dyn_into()?;
        let key = current
            .key()?
            .as_string()
            .ok_or_else(|| JsValue::from_str("IndexedDB cursor key is not a string"))?;

        if key == format!("{prefix}meta") {
            metadata = Some(
                serde_wasm_bindgen::from_value(current.value()?)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?,
            );
        } else if let Some(encoded_node) = key.strip_prefix(&node_prefix) {
            let node_id = crate::engine::decode_component(encoded_node).map_err(to_js_error)?;
            let node_state: crate::NodeState = serde_wasm_bindgen::from_value(current.value()?)
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            nodes.insert(node_id, node_state);
        }

        current.continue_()?;
        cursor = await_idb_request(request.unchecked_ref()).await?;
    }

    await_idb_transaction(&tx).await?;

    Ok(metadata.map(|metadata| crate::DatabaseSnapshot {
        clock: metadata.clock,
        nodes,
        pending_ops: metadata.pending_ops,
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
