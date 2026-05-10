use primadb::{
    BlobStorageBinding as CoreBlobStorageBinding, BlobStorageConfig, Chain as CoreChain,
    ConnectHookContext, DurableStorageBinding as CoreDurableStorageBinding, DurableStorageConfig,
    HookDecision, Identity, LexSpec, MeshConfig, NativeWebRtcMesh as CoreWebRtcMesh,
    NativeRelayServer as CoreRelayServer,
    NativeWebSocketSync as CoreWebSocketSync, NetworkHooks, Operation,
    PasswordKeyDerivationOptions, Primadb as CorePrimadb, PublicIdentity, PullRequestKind,
    QuerySpec, RecordBatch, RecordScan, RelayClientConfig, RelayServerConfig, RemotePath,
    RemoteInterestPolicy, RemoteResult as CoreRemoteResult, RemoteWatchMessage as CoreRemoteWatchMessage,
    RemoteWatchSubscription as CoreRemoteWatch, RecordWatchSubscription as CoreRecordWatchSubscription,
    RoomHookContext, Scope as CoreScope, ScopePolicy, SecretBoxKey, ServeRequestContext,
    ServeResultContext, ScriptExecutionOptions, Subscription as CoreSubscription,
    TransactionOptions, TransactionStep, TraversalSubscription as CoreTraversalSubscription,
    TraversalSpec, UserGrant,
    VectorCollectionConfig, VectorSearchSpec,
    VectorWatchSubscription as CoreVectorWatchSubscription,
    derive_password_key as core_derive_password_key, parse_request_hook_json,
    parse_result_hook_json, parse_void_hook_json,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict};
use pythonize::{depythonize, pythonize};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value as JsonValue, json};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::runtime::{Builder, Runtime};

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("primadb-python")
            .build()
            .expect("failed to build tokio runtime for primadb-python")
    })
}

fn to_py_err(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn closed_error(kind: &str) -> PyErr {
    PyRuntimeError::new_err(format!("{kind} is closed"))
}

fn to_py<T: Serialize>(py: Python<'_>, value: T) -> PyResult<Py<PyAny>> {
    pythonize(py, &value)
        .map(|value| value.unbind())
        .map_err(to_py_err)
}

fn from_py<T: DeserializeOwned>(value: &Bound<'_, PyAny>) -> PyResult<T> {
    depythonize(value).map_err(to_py_err)
}

fn remote_policy(value: Option<&Bound<'_, PyAny>>) -> PyResult<RemoteInterestPolicy> {
    match value {
        Some(value) if !value.is_none() => from_py(value),
        _ => Ok(RemoteInterestPolicy::default()),
    }
}

fn binding_to_json(binding: CoreDurableStorageBinding) -> JsonValue {
    let mut value = json!({
        "backend": binding.backend,
        "incremental": binding.incremental,
        "loadedExisting": binding.loaded_existing,
        "autoPersist": binding.auto_persist,
    });
    if let JsonValue::Object(object) = &mut value {
        if let Some(durability) = binding.durability {
            object.insert("durability".to_owned(), serde_json::to_value(durability).unwrap_or(JsonValue::Null));
        }
        if let Some(lock_mode) = binding.lock_mode {
            object.insert("lockMode".to_owned(), serde_json::to_value(lock_mode).unwrap_or(JsonValue::Null));
        }
    }
    value
}

fn blob_binding_to_json(binding: CoreBlobStorageBinding) -> JsonValue {
    let mut value = json!({
        "backend": binding.backend,
        "contentAddressed": binding.content_addressed,
    });
    if let JsonValue::Object(object) = &mut value
        && let Some(durability) = binding.durability
    {
        object.insert(
            "durability".to_owned(),
            serde_json::to_value(durability).unwrap_or(JsonValue::Null),
        );
    }
    value
}

fn remote_result_to_json_value(value: CoreRemoteResult) -> JsonValue {
    match value {
        CoreRemoteResult::Get { value } => value.unwrap_or(JsonValue::Null),
        CoreRemoteResult::Map { entries }
        | CoreRemoteResult::Query { entries } => serde_json::to_value(entries).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Lex { entries } => serde_json::to_value(entries).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Records { result } => serde_json::to_value(result).unwrap_or(JsonValue::Null),
        CoreRemoteResult::VectorSearch { result } => serde_json::to_value(result).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Node { node } => serde_json::to_value(node).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Snapshot { snapshot } => serde_json::to_value(snapshot).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Transaction { report } => serde_json::to_value(report).unwrap_or(JsonValue::Null),
    }
}

fn watch_message_to_json(message: CoreRemoteWatchMessage) -> JsonValue {
    let kind = match &message.result {
        CoreRemoteResult::Get { .. } => "get",
        CoreRemoteResult::Map { .. } => "map",
        CoreRemoteResult::Query { .. } => "query",
        CoreRemoteResult::Lex { .. } => "lex",
        CoreRemoteResult::Records { .. } => "records",
        CoreRemoteResult::VectorSearch { .. } => "vector_search",
        CoreRemoteResult::Node { .. } => "node",
        CoreRemoteResult::Snapshot { .. } => "snapshot",
        CoreRemoteResult::Transaction { .. } => "transaction",
    };
    json!({
        "done": false,
        "initial": message.initial,
        "kind": kind,
        "value": remote_result_to_json_value(message.result),
        "error": JsonValue::Null,
    })
}

struct PythonNetworkHookCallbacks {
    on_connect: Option<Py<PyAny>>,
    on_join_room: Option<Py<PyAny>>,
    on_pull: Option<Py<PyAny>>,
    on_watch: Option<Py<PyAny>>,
    on_serve_result: Option<Py<PyAny>>,
}

struct PythonNetworkHooks {
    callbacks: PythonNetworkHookCallbacks,
}

impl NetworkHooks for PythonNetworkHooks {
    fn on_connect(&self, context: &ConnectHookContext) -> HookDecision<()> {
        match &self.callbacks.on_connect {
            Some(callback) => match call_python_hook1(callback, context) {
                Ok(response) => parse_void_hook_json(response, "connection denied by network hook"),
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(()),
        }
    }

    fn on_join_room(&self, context: &RoomHookContext) -> HookDecision<()> {
        match &self.callbacks.on_join_room {
            Some(callback) => match call_python_hook1(callback, context) {
                Ok(response) => parse_void_hook_json(response, "room denied by network hook"),
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(()),
        }
    }

    fn on_pull(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
        match &self.callbacks.on_pull {
            Some(callback) => match call_python_hook1(callback, context) {
                Ok(response) => {
                    parse_request_hook_json(response, &context.request, "pull denied by network hook")
                }
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(context.request.clone()),
        }
    }

    fn on_watch(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
        match &self.callbacks.on_watch {
            Some(callback) => match call_python_hook1(callback, context) {
                Ok(response) => {
                    parse_request_hook_json(response, &context.request, "watch denied by network hook")
                }
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(context.request.clone()),
        }
    }

    fn on_serve_result(
        &self,
        context: &ServeResultContext,
        result: CoreRemoteResult,
    ) -> HookDecision<CoreRemoteResult> {
        match &self.callbacks.on_serve_result {
            Some(callback) => match call_python_hook2(callback, context, &result) {
                Ok(response) => parse_result_hook_json(response, result, "served result denied by network hook"),
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(result),
        }
    }
}

fn resolve_python_hook(
    hooks: &Bound<'_, PyAny>,
    snake_key: &str,
    camel_key: &str,
) -> PyResult<Option<Py<PyAny>>> {
    if hooks.is_none() {
        return Ok(None);
    }
    if let Ok(dict) = hooks.cast::<PyDict>() {
        if let Some(value) = dict.get_item(snake_key)? {
            return python_hook_value(value, snake_key);
        }
        if let Some(value) = dict.get_item(camel_key)? {
            return python_hook_value(value, camel_key);
        }
    }
    if hooks.hasattr(snake_key)? {
        let value = hooks.getattr(snake_key)?;
        return python_hook_value(value, snake_key);
    }
    if hooks.hasattr(camel_key)? {
        let value = hooks.getattr(camel_key)?;
        return python_hook_value(value, camel_key);
    }
    Ok(None)
}

fn python_hook_value(value: Bound<'_, PyAny>, key: &str) -> PyResult<Option<Py<PyAny>>> {
    if value.is_none() {
        return Ok(None);
    }
    if !value.is_callable() {
        return Err(PyRuntimeError::new_err(format!(
            "network hook `{key}` must be callable"
        )));
    }
    Ok(Some(value.unbind()))
}

fn call_python_hook1<T: Serialize>(
    callback: &Py<PyAny>,
    arg: &T,
) -> std::result::Result<Option<JsonValue>, String> {
    Python::attach(|py| {
        let arg = pythonize(py, arg).map_err(|error| error.to_string())?;
        let result = callback
            .bind(py)
            .call1((arg,))
            .map_err(|error| error.to_string())?;
        python_hook_response_to_json(result)
    })
}

fn call_python_hook2<A: Serialize, B: Serialize>(
    callback: &Py<PyAny>,
    arg_a: &A,
    arg_b: &B,
) -> std::result::Result<Option<JsonValue>, String> {
    Python::attach(|py| {
        let arg_a = pythonize(py, arg_a).map_err(|error| error.to_string())?;
        let arg_b = pythonize(py, arg_b).map_err(|error| error.to_string())?;
        let result = callback
            .bind(py)
            .call1((arg_a, arg_b))
            .map_err(|error| error.to_string())?;
        python_hook_response_to_json(result)
    })
}

fn python_hook_response_to_json(
    value: Bound<'_, PyAny>,
) -> std::result::Result<Option<JsonValue>, String> {
    if value.is_none() {
        return Ok(None);
    }
    depythonize::<JsonValue>(&value)
        .map(Some)
        .map_err(|error| error.to_string())
}

#[pyclass(module = "primadb._native")]
struct Primadb {
    inner: CorePrimadb,
}

#[pyclass(module = "primadb._native")]
struct Chain {
    inner: CoreChain,
}

#[pyclass(module = "primadb._native")]
struct Scope {
    inner: CoreScope,
}

#[pyclass(module = "primadb._native")]
struct Subscription {
    inner: Arc<Mutex<Option<CoreSubscription>>>,
}

#[pyclass(module = "primadb._native")]
struct TraversalSubscription {
    inner: Arc<Mutex<Option<CoreTraversalSubscription>>>,
}

#[pyclass(module = "primadb._native")]
struct RecordWatchSubscription {
    inner: Arc<Mutex<Option<CoreRecordWatchSubscription>>>,
}

#[pyclass(module = "primadb._native")]
struct VectorWatchSubscription {
    inner: Arc<Mutex<Option<CoreVectorWatchSubscription>>>,
}

#[pyclass(module = "primadb._native")]
struct WebSocketSync {
    inner: Arc<Mutex<Option<CoreWebSocketSync>>>,
}

#[pyclass(module = "primadb._native")]
struct RelayServer {
    inner: Arc<Mutex<Option<CoreRelayServer>>>,
}

#[pyclass(module = "primadb._native")]
struct RemoteWatch {
    inner: Arc<Mutex<Option<CoreRemoteWatch>>>,
}

#[pyclass(module = "primadb._native")]
struct WebRtcMesh {
    inner: Arc<Mutex<Option<CoreWebRtcMesh>>>,
}

#[pymethods]
impl Primadb {
    #[new]
    #[pyo3(signature = (replica_id=None))]
    fn new(replica_id: Option<String>) -> Self {
        let inner = replica_id
            .map(CorePrimadb::with_replica_id)
            .unwrap_or_default();
        Self { inner }
    }

    fn replica_id(&self) -> String {
        self.inner.replica_id()
    }

    fn chain(&self, root: String) -> Chain {
        Chain {
            inner: self.inner.root(root),
        }
    }

    fn scope(&self, root: String) -> Scope {
        Scope {
            inner: self.inner.scope(root),
        }
    }

    fn transaction(&self, py: Python<'_>, steps: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let steps: Vec<TransactionStep> = from_py(steps)?;
        to_py(
            py,
            self.inner
                .apply_transaction_steps(steps)
                .map_err(to_py_err)?,
        )
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.snapshot())
    }

    #[pyo3(signature = (root=None))]
    fn snapshot_for_root(&self, py: Python<'_>, root: Option<String>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.snapshot_for_root(root.as_deref()))
    }

    fn node_state(&self, py: Python<'_>, id: String) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.node_state(&id).map_err(to_py_err)?)
    }

    fn apply_node_state(&self, node: &Bound<'_, PyAny>) -> PyResult<bool> {
        let node: primadb::NodeState = from_py(node)?;
        self.inner.apply_node_state(node).map_err(to_py_err)
    }

    fn export_snapshot_json(&self) -> PyResult<String> {
        self.inner.export_snapshot_json().map_err(to_py_err)
    }

    fn import_snapshot_json(&self, payload: String) -> PyResult<()> {
        self.inner.import_snapshot_json(&payload).map_err(to_py_err)
    }

    fn merge_snapshot_json(&self, payload: String) -> PyResult<()> {
        self.inner.merge_snapshot_json(&payload).map_err(to_py_err)
    }

    fn pending_operations(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.pending_operations())
    }

    fn pending_envelope(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.sync_envelope())
    }

    fn export_pending_operations_json(&self) -> PyResult<String> {
        self.inner
            .export_pending_operations_json()
            .map_err(to_py_err)
    }

    fn drain_pending_operations(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.drain_pending_operations().map_err(to_py_err)?)
    }

    fn drain_pending_envelope(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.drain_sync_envelope().map_err(to_py_err)?)
    }

    fn drain_pending_envelope_json(&self) -> PyResult<String> {
        self.inner
            .drain_pending_envelope_json()
            .map_err(to_py_err)
    }

    fn apply_operations(&self, operations: &Bound<'_, PyAny>) -> PyResult<u32> {
        let operations: Vec<Operation> = from_py(operations)?;
        let applied = self.inner.apply_operations(operations).map_err(to_py_err)?;
        u32::try_from(applied).map_err(to_py_err)
    }

    fn apply_envelope(&self, envelope: &Bound<'_, PyAny>) -> PyResult<u32> {
        let envelope = from_py(envelope)?;
        let applied = self.inner.apply_sync_envelope(envelope).map_err(to_py_err)?;
        u32::try_from(applied).map_err(to_py_err)
    }

    fn apply_operations_json(&self, payload: String) -> PyResult<u32> {
        let applied = self
            .inner
            .apply_operations_json(&payload)
            .map_err(to_py_err)?;
        u32::try_from(applied).map_err(to_py_err)
    }

    fn open_durable_storage(
        &self,
        py: Python<'_>,
        config: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let config: DurableStorageConfig = from_py(config)?;
        let binding = self
            .inner
            .open_durable_storage(config)
            .map_err(to_py_err)?;
        to_py(py, binding_to_json(binding))
    }

    fn open_blob_storage(&self, py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let config: BlobStorageConfig = from_py(config)?;
        let binding = self.inner.open_blob_storage(config).map_err(to_py_err)?;
        to_py(py, blob_binding_to_json(binding))
    }

    fn close_durable_storage(&self) {
        self.inner.close_durable_storage();
    }

    fn sync_storage(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.sync_storage().map_err(to_py_err)?)
    }

    fn storage_recovery_report(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.storage_recovery_report())
    }

    fn put_record(&self, key: String, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let value: JsonValue = from_py(value)?;
        self.inner.put_record_json(key, value).map_err(to_py_err)
    }

    fn put_record_bytes(&self, key: String, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = value
            .extract::<Vec<u8>>()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        self.inner.put_record_bytes(key, bytes).map_err(to_py_err)
    }

    #[pyo3(signature = (key, value, media_type=None))]
    fn put_record_blob(
        &self,
        py: Python<'_>,
        key: String,
        value: &Bound<'_, PyAny>,
        media_type: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let bytes = value
            .extract::<Vec<u8>>()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let reference = self
            .inner
            .put_record_blob(key, bytes, media_type.as_deref())
            .map_err(to_py_err)?;
        to_py(py, reference)
    }

    fn get_record(&self, py: Python<'_>, key: String) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.get_record(&key).map_err(to_py_err)?)
    }

    fn scan_records(&self, py: Python<'_>, scan: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let scan: RecordScan = from_py(scan)?;
        to_py(py, self.inner.scan_records(scan).map_err(to_py_err)?)
    }

    fn watch_records(&self, scan: &Bound<'_, PyAny>) -> PyResult<RecordWatchSubscription> {
        let scan: RecordScan = from_py(scan)?;
        let subscription = self.inner.watch_records(scan).map_err(to_py_err)?;
        Ok(RecordWatchSubscription {
            inner: Arc::new(Mutex::new(Some(subscription))),
        })
    }

    fn create_vector_collection(
        &self,
        name: String,
        config: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let config: VectorCollectionConfig = from_py(config)?;
        self.inner
            .create_vector_collection(name, config)
            .map_err(to_py_err)
    }

    fn put_vector(
        &self,
        collection: String,
        id: String,
        vector: Vec<f32>,
        metadata: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let metadata = metadata.map(from_py::<JsonValue>).transpose()?;
        self.inner
            .put_vector(collection, id, vector, metadata)
            .map_err(to_py_err)
    }

    fn delete_vector(&self, collection: String, id: String) -> PyResult<()> {
        self.inner
            .delete_vector(collection, id)
            .map_err(to_py_err)
    }

    fn get_vector(&self, py: Python<'_>, collection: String, id: String) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.get_vector(collection, id).map_err(to_py_err)?)
    }

    fn search_vectors(
        &self,
        py: Python<'_>,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let spec: VectorSearchSpec = from_py(spec)?;
        to_py(
            py,
            self.inner
                .search_vectors(collection, query, spec)
                .map_err(to_py_err)?,
        )
    }

    fn watch_vector_search(
        &self,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<VectorWatchSubscription> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let subscription = self
            .inner
            .watch_vector_search(collection, query, spec)
            .map_err(to_py_err)?;
        Ok(VectorWatchSubscription {
            inner: Arc::new(Mutex::new(Some(subscription))),
        })
    }

    fn apply_record_batch(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let batch: RecordBatch = from_py(batch)?;
        to_py(
            py,
            self.inner.apply_record_batch(batch).map_err(to_py_err)?,
        )
    }

    fn delete_record(&self, key: String) -> PyResult<()> {
        self.inner.delete_record(key).map_err(to_py_err)
    }

    fn attach_node_script(
        &self,
        path: &Bound<'_, PyAny>,
        script: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let path: RemotePath = from_py(path)?;
        let script = from_py(script)?;
        self.inner
            .attach_node_script(path, script)
            .map_err(to_py_err)
    }

    fn remove_node_script(&self, path: &Bound<'_, PyAny>, script_id: String) -> PyResult<()> {
        let path: RemotePath = from_py(path)?;
        self.inner
            .remove_node_script(&path, &script_id)
            .map_err(to_py_err)
    }

    fn node_scripts(&self, py: Python<'_>, path: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        to_py(py, self.inner.node_scripts(&path).map_err(to_py_err)?)
    }

    fn execute_node_scripts(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let options = options
            .map(from_py::<ScriptExecutionOptions>)
            .transpose()?
            .unwrap_or_default();
        to_py(
            py,
            self.inner
                .execute_node_scripts(path, options)
                .map_err(to_py_err)?,
        )
    }

    fn register_user(
        &self,
        alias: String,
        public_key: String,
        grants: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let public_identity = PublicIdentity::from_base64(&public_key).map_err(to_py_err)?;
        let grants: Vec<UserGrant> = from_py(grants)?;
        self.inner
            .register_user(alias, public_identity, grants)
            .map_err(to_py_err)
    }

    fn authenticate_local_user(
        &self,
        alias: String,
        secret_key: String,
        grants: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let identity = Identity::from_secret_key_base64(&secret_key).map_err(to_py_err)?;
        let grants: Vec<UserGrant> = from_py(grants)?;
        self.inner
            .authenticate_local_user(alias, identity, grants)
            .map_err(to_py_err)
    }

    fn set_require_signed_sync(&self, required: bool) {
        self.inner.set_require_signed_sync(required);
    }

    fn set_snapshot_encryption_key(&self, key_base64: String) -> PyResult<()> {
        let key = SecretBoxKey::from_base64(&key_base64).map_err(to_py_err)?;
        self.inner.set_snapshot_encryption_key(key);
        Ok(())
    }

    fn set_transport_encryption_key(&self, key_base64: String) -> PyResult<()> {
        let key = SecretBoxKey::from_base64(&key_base64).map_err(to_py_err)?;
        self.inner.set_transport_encryption_key(key);
        Ok(())
    }

    fn connect_relay(&self, py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<WebSocketSync> {
        let config: RelayClientConfig = from_py(config)?;
        let sync = py.detach(|| runtime().block_on(self.inner.connect_relay(config)))
            .map_err(to_py_err)?;
        Ok(WebSocketSync {
            inner: Arc::new(Mutex::new(Some(sync))),
        })
    }

    fn connect_mesh(&self, py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<WebRtcMesh> {
        let config: MeshConfig = from_py(config)?;
        let mesh = py.detach(|| runtime().block_on(self.inner.connect_mesh(config)))
            .map_err(to_py_err)?;
        Ok(WebRtcMesh {
            inner: Arc::new(Mutex::new(Some(mesh))),
        })
    }

    fn set_network_hooks(&self, hooks: &Bound<'_, PyAny>) -> PyResult<()> {
        if hooks.is_none() {
            self.inner.clear_network_hooks();
            return Ok(());
        }
        let callbacks = PythonNetworkHookCallbacks {
            on_connect: resolve_python_hook(hooks, "on_connect", "onConnect")?,
            on_join_room: resolve_python_hook(hooks, "on_join_room", "onJoinRoom")?,
            on_pull: resolve_python_hook(hooks, "on_pull", "onPull")?,
            on_watch: resolve_python_hook(hooks, "on_watch", "onWatch")?,
            on_serve_result: resolve_python_hook(hooks, "on_serve_result", "onServeResult")?,
        };
        self.inner
            .set_network_hooks(Arc::new(PythonNetworkHooks { callbacks }));
        Ok(())
    }

    fn clear_network_hooks(&self) {
        self.inner.clear_network_hooks();
    }
}

#[pymethods]
impl Scope {
    fn root(&self) -> String {
        self.inner.root().to_owned()
    }

    fn configure(&self, policy: &Bound<'_, PyAny>) -> PyResult<()> {
        let policy: ScopePolicy = from_py(policy)?;
        self.inner.configure(policy).map_err(to_py_err)
    }

    fn policy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.policy())
    }

    fn proposals(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.proposals())
    }

    #[pyo3(signature = (steps, options=None))]
    fn transaction(
        &self,
        py: Python<'_>,
        steps: &Bound<'_, PyAny>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let steps: Vec<TransactionStep> = from_py(steps)?;
        let options = match options {
            Some(value) => from_py(value)?,
            None => TransactionOptions::default(),
        };
        to_py(
            py,
            self.inner
                .transaction_steps(steps, options)
                .map_err(to_py_err)?,
        )
    }
}

#[pymethods]
impl Chain {
    fn field(&self, key: String) -> Chain {
        Chain {
            inner: self.inner.field(key),
        }
    }

    fn path(&self) -> String {
        self.inner.path()
    }

    fn put(&self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let value: JsonValue = from_py(value)?;
        self.inner.put(value).map_err(to_py_err)
    }

    fn put_bytes(&self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = value
            .extract::<Vec<u8>>()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        self.inner.put_bytes(bytes).map_err(to_py_err)
    }

    #[pyo3(signature = (value, certificate=None))]
    fn put_signed(&self, value: &Bound<'_, PyAny>, certificate: Option<String>) -> PyResult<()> {
        let value: JsonValue = from_py(value)?;
        self.inner
            .put_signed(value, certificate)
            .map_err(to_py_err)
    }

    fn once(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.once_json().map_err(to_py_err)?)
    }

    fn once_bytes(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .once_bytes()
            .map(|value| value.map(|bytes| PyBytes::new(py, &bytes).unbind()))
            .map_err(to_py_err)
    }

    fn unset(&self) -> PyResult<()> {
        self.inner.unset().map_err(to_py_err)
    }

    fn set(&self, value: &Bound<'_, PyAny>) -> PyResult<String> {
        let value: JsonValue = from_py(value)?;
        self.inner.set(value).map_err(to_py_err)
    }

    #[pyo3(signature = (value, certificate=None))]
    fn set_signed(&self, value: &Bound<'_, PyAny>, certificate: Option<String>) -> PyResult<String> {
        let value: JsonValue = from_py(value)?;
        self.inner
            .set_signed(value, certificate)
            .map_err(to_py_err)
    }

    fn remove(&self, value: &Bound<'_, PyAny>) -> PyResult<String> {
        let value: JsonValue = from_py(value)?;
        self.inner.remove(value).map_err(to_py_err)
    }

    #[pyo3(signature = (value, media_type=None))]
    fn put_blob(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        media_type: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let bytes = value
            .extract::<Vec<u8>>()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let reference = self
            .inner
            .put_blob(bytes, media_type.as_deref())
            .map_err(to_py_err)?;
        to_py(py, reference)
    }

    fn blob_ref(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.once_blob_ref().map_err(to_py_err)?)
    }

    fn get_blob(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        self.inner
            .get_blob()
            .map(|value| {
                value.map(|blob| PyBytes::new(py, &blob.data.into_inner()).unbind())
            })
            .map_err(to_py_err)
    }

    fn map(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.map().map_err(to_py_err)?)
    }

    fn query(&self, py: Python<'_>, spec: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let spec: QuerySpec = from_py(spec)?;
        to_py(py, self.inner.query(spec).map_err(to_py_err)?)
    }

    fn first_query(&self, py: Python<'_>, spec: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let spec: QuerySpec = from_py(spec)?;
        to_py(py, self.inner.first(spec).map_err(to_py_err)?)
    }

    fn scan(&self, py: Python<'_>, spec: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let spec: LexSpec = from_py(spec)?;
        to_py(py, self.inner.scan(spec).map_err(to_py_err)?)
    }

    fn traverse(&self, py: Python<'_>, spec: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let spec: TraversalSpec = from_py(spec)?;
        to_py(py, self.inner.traverse(spec).map_err(to_py_err)?)
    }

    fn subscribe(&self) -> PyResult<Subscription> {
        let subscription = self.inner.subscribe().map_err(to_py_err)?;
        Ok(Subscription {
            inner: Arc::new(Mutex::new(Some(subscription))),
        })
    }

    fn watch_traverse(&self, spec: &Bound<'_, PyAny>) -> PyResult<TraversalSubscription> {
        let spec: TraversalSpec = from_py(spec)?;
        let subscription = self.inner.watch_traverse(spec).map_err(to_py_err)?;
        Ok(TraversalSubscription {
            inner: Arc::new(Mutex::new(Some(subscription))),
        })
    }
}

#[pymethods]
impl Subscription {
    fn next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let subscription = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("subscription"))?
        };

        let message = py.detach(|| runtime().block_on(subscription.recv()));
        let response = match message {
            Some(value) => {
                self.inner.lock().unwrap().replace(subscription);
                json!({
                    "done": false,
                    "value": value,
                })
            }
            None => json!({
                "done": true,
                "value": JsonValue::Null,
            }),
        };

        to_py(py, response)
    }

    fn try_next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.inner.lock().unwrap();
        let Some(subscription) = guard.as_ref() else {
            return to_py(
                py,
                json!({
                    "done": true,
                    "value": JsonValue::Null,
                }),
            );
        };

        let message = match subscription.try_recv() {
            Some(value) => json!({
                "done": false,
                "value": value,
            }),
            None => json!({
                "done": false,
                "value": JsonValue::Null,
            }),
        };
        to_py(py, message)
    }

    fn close(&self) {
        let _ = self.inner.lock().unwrap().take();
    }
}

#[pymethods]
impl TraversalSubscription {
    fn next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let subscription = {
            let mut guard = self.inner.lock().unwrap();
            guard
                .take()
                .ok_or_else(|| closed_error("traversal subscription"))?
        };

        let message = py.detach(|| runtime().block_on(subscription.recv()));
        let response = match message {
            Some(value) => {
                self.inner.lock().unwrap().replace(subscription);
                json!({
                    "done": false,
                    "value": value,
                })
            }
            None => json!({
                "done": true,
                "value": JsonValue::Null,
            }),
        };

        to_py(py, response)
    }

    fn try_next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.inner.lock().unwrap();
        let Some(subscription) = guard.as_ref() else {
            return to_py(
                py,
                json!({
                    "done": true,
                    "value": JsonValue::Null,
                }),
            );
        };

        let message = match subscription.try_recv() {
            Some(value) => json!({
                "done": false,
                "value": value,
            }),
            None => json!({
                "done": false,
                "value": JsonValue::Null,
            }),
        };
        to_py(py, message)
    }

    fn close(&self) {
        let _ = self.inner.lock().unwrap().take();
    }
}

#[pymethods]
impl RecordWatchSubscription {
    fn next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let subscription = {
            let mut guard = self.inner.lock().unwrap();
            guard
                .take()
                .ok_or_else(|| closed_error("record watch subscription"))?
        };

        let message = py.detach(|| runtime().block_on(subscription.recv()));
        let response = match message {
            Some(value) => {
                self.inner.lock().unwrap().replace(subscription);
                json!({
                    "done": false,
                    "value": value,
                })
            }
            None => json!({
                "done": true,
                "value": JsonValue::Null,
            }),
        };

        to_py(py, response)
    }

    fn try_next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.inner.lock().unwrap();
        let Some(subscription) = guard.as_ref() else {
            return to_py(
                py,
                json!({
                    "done": true,
                    "value": JsonValue::Null,
                }),
            );
        };

        let message = match subscription.try_recv() {
            Some(value) => json!({
                "done": false,
                "value": value,
            }),
            None => json!({
                "done": false,
                "value": JsonValue::Null,
            }),
        };
        to_py(py, message)
    }

    fn close(&self) {
        let _ = self.inner.lock().unwrap().take();
    }
}

#[pymethods]
impl VectorWatchSubscription {
    fn next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let subscription = {
            let mut guard = self.inner.lock().unwrap();
            guard
                .take()
                .ok_or_else(|| closed_error("vector watch subscription"))?
        };

        let message = py.detach(|| runtime().block_on(subscription.recv()));
        let response = match message {
            Some(value) => {
                self.inner.lock().unwrap().replace(subscription);
                json!({
                    "done": false,
                    "value": value,
                })
            }
            None => json!({
                "done": true,
                "value": JsonValue::Null,
            }),
        };

        to_py(py, response)
    }

    fn try_next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.inner.lock().unwrap();
        let Some(subscription) = guard.as_ref() else {
            return to_py(
                py,
                json!({
                    "done": true,
                    "value": JsonValue::Null,
                }),
            );
        };

        let message = match subscription.try_recv() {
            Some(value) => json!({
                "done": false,
                "value": value,
            }),
            None => json!({
                "done": false,
                "value": JsonValue::Null,
            }),
        };
        to_py(py, message)
    }

    fn close(&self) {
        let _ = self.inner.lock().unwrap().take();
    }
}

#[pymethods]
impl RelayServer {
    #[staticmethod]
    fn listen(py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<RelayServer> {
        let config: RelayServerConfig = from_py(config)?;
        let server = py
            .detach(|| runtime().block_on(CoreRelayServer::bind_with_config(config)))
            .map_err(to_py_err)?;
        Ok(RelayServer {
            inner: Arc::new(Mutex::new(Some(server))),
        })
    }

    fn bind_addr(&self) -> PyResult<String> {
        let guard = self.inner.lock().unwrap();
        let server = guard.as_ref().ok_or_else(|| closed_error("relay server"))?;
        Ok(server.bind_addr().to_string())
    }

    fn url(&self) -> PyResult<String> {
        let guard = self.inner.lock().unwrap();
        let server = guard.as_ref().ok_or_else(|| closed_error("relay server"))?;
        Ok(server.url())
    }

    fn client_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|server| server.client_count() as u32)
            .unwrap_or(0)
    }

    fn peer_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|server| server.peer_count() as u32)
            .unwrap_or(0)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let server = self.inner.lock().unwrap().take();
        if let Some(server) = server {
            py.detach(|| runtime().block_on(server.close()));
        }
        Ok(())
    }
}

#[pymethods]
impl RemoteWatch {
    fn next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let watch = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("remote watch"))?
        };

        let message = py.detach(|| runtime().block_on(watch.recv()));
        let response = match message {
            Some(Ok(value)) => {
                self.inner.lock().unwrap().replace(watch);
                watch_message_to_json(value)
            }
            Some(Err(message)) => json!({
                "done": false,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": message,
            }),
            None => json!({
                "done": true,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": JsonValue::Null,
            }),
        };
        to_py(py, response)
    }

    fn try_next(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let guard = self.inner.lock().unwrap();
        let Some(watch) = guard.as_ref() else {
            return to_py(
                py,
                json!({
                    "done": true,
                    "initial": false,
                    "kind": JsonValue::Null,
                    "value": JsonValue::Null,
                    "error": JsonValue::Null,
                }),
            );
        };

        let message = match watch.try_recv() {
            Some(Ok(value)) => watch_message_to_json(value),
            Some(Err(message)) => json!({
                "done": false,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": message,
            }),
            None => json!({
                "done": false,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": JsonValue::Null,
            }),
        };
        to_py(py, message)
    }

    fn close(&self) {
        if let Some(watch) = self.inner.lock().unwrap().take() {
            watch.close();
        }
    }
}

#[pymethods]
impl WebSocketSync {
    fn is_connected(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(CoreWebSocketSync::is_connected)
    }

    fn pending_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|sync| sync.pending_count() as u32)
            .unwrap_or(0)
    }

    fn inflight_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|sync| sync.inflight_count() as u32)
            .unwrap_or(0)
    }

    fn known_peer_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|sync| sync.known_peer_count() as u32)
            .unwrap_or(0)
    }

    fn recommended_peers(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let peers = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(CoreWebSocketSync::recommended_peers)
            .unwrap_or_default();
        to_py(py, peers)
    }

    #[pyo3(signature = (path, policy=None))]
    fn get(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_get_with_policy(path, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (path, spec, policy=None))]
    fn query(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let spec: QuerySpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_query_with_policy(path, spec, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (path, spec, policy=None))]
    fn lex(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let spec: LexSpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_lex_with_policy(path, spec, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (scan, policy=None))]
    fn records(
        &self,
        py: Python<'_>,
        scan: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let scan: RecordScan = from_py(scan)?;
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_records_with_policy(scan, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (collection, query, spec, policy=None))]
    fn vector_search(
        &self,
        py: Python<'_>,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result =
            py.detach(|| runtime().block_on(sync.remote_vector_search_with_policy(collection, query, spec, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (id, policy=None))]
    fn node(
        &self,
        py: Python<'_>,
        id: String,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_node_with_policy(id, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (root=None, policy=None))]
    fn snapshot(
        &self,
        py: Python<'_>,
        root: Option<String>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let policy = remote_policy(policy)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_snapshot_with_policy(root, policy)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn remote_get(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_get(peer_id, path)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn remote_query(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let spec: QuerySpec = from_py(spec)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_query(peer_id, path, spec)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn remote_lex(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let path: RemotePath = from_py(path)?;
        let spec: LexSpec = from_py(spec)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_lex(peer_id, path, spec)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn remote_records(
        &self,
        py: Python<'_>,
        peer_id: String,
        scan: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let scan: RecordScan = from_py(scan)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_records(peer_id, scan)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn remote_vector_search(
        &self,
        py: Python<'_>,
        peer_id: String,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result =
            py.detach(|| runtime().block_on(sync.remote_vector_search(peer_id, collection, query, spec)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn remote_node(
        &self,
        py: Python<'_>,
        peer_id: String,
        id: String,
    ) -> PyResult<Py<PyAny>> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_node(peer_id, id)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (peer_id, root=None))]
    fn remote_snapshot(
        &self,
        py: Python<'_>,
        peer_id: String,
        root: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.remote_snapshot(peer_id, root)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    #[pyo3(signature = (peer_id, scope, steps, options=None))]
    fn remote_transaction(
        &self,
        py: Python<'_>,
        peer_id: String,
        scope: String,
        steps: &Bound<'_, PyAny>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let steps: Vec<TransactionStep> = from_py(steps)?;
        let options = match options {
            Some(value) => from_py(value)?,
            None => TransactionOptions::default(),
        };
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result =
            py.detach(|| runtime().block_on(sync.remote_transaction(peer_id, scope, steps, options)));
        self.inner.lock().unwrap().replace(sync);
        to_py(py, result.map_err(to_py_err)?)
    }

    fn watch_remote_get(&self, peer_id: String, path: &Bound<'_, PyAny>) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_get(peer_id, path)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn watch_remote_map(&self, peer_id: String, path: &Bound<'_, PyAny>) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_map(peer_id, path)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn watch_remote_query(
        &self,
        peer_id: String,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: QuerySpec = from_py(spec)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_query(peer_id, path, spec)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn watch_remote_lex(
        &self,
        peer_id: String,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: LexSpec = from_py(spec)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_lex(peer_id, path, spec)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn watch_remote_records(
        &self,
        peer_id: String,
        scan: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let scan: RecordScan = from_py(scan)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_records(peer_id, scan)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn watch_remote_vector_search(
        &self,
        peer_id: String,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_vector_search(peer_id, collection, query, spec)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn watch_remote_node(&self, peer_id: String, id: String) -> PyResult<RemoteWatch> {
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_node(peer_id, id)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (peer_id, root=None))]
    fn watch_remote_snapshot(&self, peer_id: String, root: Option<String>) -> PyResult<RemoteWatch> {
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_snapshot(peer_id, root)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (path, policy=None))]
    fn watch_get(
        &self,
        path: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_get_with_policy(path, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (path, policy=None))]
    fn watch_map(
        &self,
        path: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_map_with_policy(path, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (path, spec, policy=None))]
    fn watch_query(
        &self,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: QuerySpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_query_with_policy(path, spec, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (path, spec, policy=None))]
    fn watch_lex(
        &self,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: LexSpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_lex_with_policy(path, spec, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (scan, policy=None))]
    fn watch_records(
        &self,
        scan: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let scan: RecordScan = from_py(scan)?;
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_records_with_policy(scan, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (collection, query, spec, policy=None))]
    fn watch_vector_search(
        &self,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_vector_search_with_policy(collection, query, spec, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (id, policy=None))]
    fn watch_node(
        &self,
        id: String,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_node_with_policy(id, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[pyo3(signature = (root=None, policy=None))]
    fn watch_snapshot(
        &self,
        root: Option<String>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let policy = remote_policy(policy)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_snapshot_with_policy(root, policy)
            .map_err(to_py_err)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    fn flush_pending(&self, py: Python<'_>) -> PyResult<u32> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.flush_pending()));
        self.inner.lock().unwrap().replace(sync);
        u32::try_from(result.map_err(to_py_err)?).map_err(to_py_err)
    }

    fn retry_inflight(&self, py: Python<'_>) -> PyResult<u32> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = py.detach(|| runtime().block_on(sync.retry_inflight()));
        self.inner.lock().unwrap().replace(sync);
        u32::try_from(result.map_err(to_py_err)?).map_err(to_py_err)
    }

    fn close(&self) {
        if let Some(mut sync) = self.inner.lock().unwrap().take() {
            sync.close();
        }
    }
}

#[pymethods]
impl WebRtcMesh {
    fn peer_id(&self) -> String {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|mesh| mesh.peer_id())
            .unwrap_or_default()
    }

    fn signaling_mode(&self) -> String {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|mesh| mesh.signaling_mode().to_owned())
            .unwrap_or_default()
    }

    fn relay_url(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|mesh| mesh.relay_url().to_owned())
    }

    fn relay_connected(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(CoreWebRtcMesh::relay_connected)
    }

    fn peer_count(&self, py: Python<'_>) -> PyResult<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.peer_count()));
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result).map_err(to_py_err)
    }

    fn open_peer_count(&self, py: Python<'_>) -> PyResult<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.open_peer_count()));
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result).map_err(to_py_err)
    }

    fn inflight_count(&self, py: Python<'_>) -> PyResult<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.inflight_count()));
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result).map_err(to_py_err)
    }

    fn recommended_peers(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let peers = py.detach(|| runtime().block_on(mesh.recommended_peers()));
        self.inner.lock().unwrap().replace(mesh);
        to_py(py, peers)
    }

    fn watch_remote_get(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_get(peer_id, path)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn watch_remote_map(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_map(peer_id, path)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn watch_remote_query(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: QuerySpec = from_py(spec)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_query(peer_id, path, spec)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn watch_remote_lex(
        &self,
        py: Python<'_>,
        peer_id: String,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: LexSpec = from_py(spec)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_lex(peer_id, path, spec)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn watch_remote_records(
        &self,
        py: Python<'_>,
        peer_id: String,
        scan: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let scan: RecordScan = from_py(scan)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_records(peer_id, scan)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn watch_remote_vector_search(
        &self,
        py: Python<'_>,
        peer_id: String,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<RemoteWatch> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result =
            py.detach(|| runtime().block_on(mesh.watch_vector_search(peer_id, collection, query, spec)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn watch_remote_node(
        &self,
        py: Python<'_>,
        peer_id: String,
        id: String,
    ) -> PyResult<RemoteWatch> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_node(peer_id, id)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (peer_id, root=None))]
    fn watch_remote_snapshot(
        &self,
        py: Python<'_>,
        peer_id: String,
        root: Option<String>,
    ) -> PyResult<RemoteWatch> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_snapshot(peer_id, root)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (path, policy=None))]
    fn watch_get(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_get_with_policy(path, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (path, policy=None))]
    fn watch_map(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_map_with_policy(path, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (path, spec, policy=None))]
    fn watch_query(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: QuerySpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_query_with_policy(path, spec, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (path, spec, policy=None))]
    fn watch_lex(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let path: RemotePath = from_py(path)?;
        let spec: LexSpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_lex_with_policy(path, spec, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (scan, policy=None))]
    fn watch_records(
        &self,
        py: Python<'_>,
        scan: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let scan: RecordScan = from_py(scan)?;
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_records_with_policy(scan, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (collection, query, spec, policy=None))]
    fn watch_vector_search(
        &self,
        py: Python<'_>,
        collection: String,
        query: Vec<f32>,
        spec: &Bound<'_, PyAny>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let spec: VectorSearchSpec = from_py(spec)?;
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result =
            py.detach(|| runtime().block_on(mesh.watch_vector_search_with_policy(collection, query, spec, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (id, policy=None))]
    fn watch_node(
        &self,
        py: Python<'_>,
        id: String,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_node_with_policy(id, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    #[pyo3(signature = (root=None, policy=None))]
    fn watch_snapshot(
        &self,
        py: Python<'_>,
        root: Option<String>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RemoteWatch> {
        let policy = remote_policy(policy)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.watch_snapshot_with_policy(root, policy)));
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result.map_err(to_py_err)?))),
        })
    }

    fn flush_pending(&self, py: Python<'_>) -> PyResult<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.flush_pending()));
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result.map_err(to_py_err)?).map_err(to_py_err)
    }

    fn retry_inflight(&self, py: Python<'_>) -> PyResult<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = py.detach(|| runtime().block_on(mesh.retry_inflight()));
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result.map_err(to_py_err)?).map_err(to_py_err)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let mesh = self.inner.lock().unwrap().take();
        if let Some(mut mesh) = mesh {
            py.detach(|| runtime().block_on(mesh.close()));
        }
        Ok(())
    }
}

#[pyfunction]
fn generate_identity(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let identity = Identity::generate();
    to_py(
        py,
        json!({
            "publicKey": identity.public_key_base64(),
            "secretKey": identity.secret_key_base64(),
        }),
    )
}

#[pyfunction(signature = (password, options=None))]
fn derive_password_key(
    py: Python<'_>,
    password: String,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let options = match options {
        Some(options) if !options.is_none() => from_py::<PasswordKeyDerivationOptions>(options)?,
        _ => PasswordKeyDerivationOptions::default(),
    };
    let derived = core_derive_password_key(password, options).map_err(to_py_err)?;
    to_py(py, derived)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Primadb>()?;
    module.add_class::<Chain>()?;
    module.add_class::<Scope>()?;
    module.add_class::<Subscription>()?;
    module.add_class::<TraversalSubscription>()?;
    module.add_class::<RecordWatchSubscription>()?;
    module.add_class::<VectorWatchSubscription>()?;
    module.add_class::<RelayServer>()?;
    module.add_class::<RemoteWatch>()?;
    module.add_class::<WebSocketSync>()?;
    module.add_class::<WebRtcMesh>()?;
    module.add_function(wrap_pyfunction!(derive_password_key, module)?)?;
    module.add_function(wrap_pyfunction!(generate_identity, module)?)?;
    Ok(())
}
