use primadb::{
    BlobStorageBinding as CoreBlobStorageBinding, BlobStorageConfig, Chain as CoreChain,
    DurableStorageBinding as CoreDurableStorageBinding, DurableStorageConfig, LexSpec, MeshConfig,
    NativeWebRtcMesh as CoreWebRtcMesh,
    NativeWebSocketSync as CoreWebSocketSync, Operation, Primadb as CorePrimadb, QuerySpec,
    RelayClientConfig, RemotePath, RemoteResult as CoreRemoteResult,
    RemoteWatchMessage as CoreRemoteWatchMessage, RemoteWatchSubscription as CoreRemoteWatch,
    Subscription as CoreSubscription,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};
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

fn binding_to_json(binding: CoreDurableStorageBinding) -> JsonValue {
    json!({
        "backend": binding.backend,
        "incremental": binding.incremental,
        "loadedExisting": binding.loaded_existing,
        "autoPersist": binding.auto_persist,
    })
}

fn blob_binding_to_json(binding: CoreBlobStorageBinding) -> JsonValue {
    json!({
        "backend": binding.backend,
        "contentAddressed": binding.content_addressed,
    })
}

fn remote_result_to_json_value(value: CoreRemoteResult) -> JsonValue {
    match value {
        CoreRemoteResult::Get { value } => value.unwrap_or(JsonValue::Null),
        CoreRemoteResult::Map { entries }
        | CoreRemoteResult::Query { entries } => serde_json::to_value(entries).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Lex { entries } => serde_json::to_value(entries).unwrap_or(JsonValue::Null),
        CoreRemoteResult::Snapshot { snapshot } => serde_json::to_value(snapshot).unwrap_or(JsonValue::Null),
    }
}

fn watch_message_to_json(message: CoreRemoteWatchMessage) -> JsonValue {
    let kind = match &message.result {
        CoreRemoteResult::Get { .. } => "get",
        CoreRemoteResult::Map { .. } => "map",
        CoreRemoteResult::Query { .. } => "query",
        CoreRemoteResult::Lex { .. } => "lex",
        CoreRemoteResult::Snapshot { .. } => "snapshot",
    };
    json!({
        "done": false,
        "initial": message.initial,
        "kind": kind,
        "value": remote_result_to_json_value(message.result),
        "error": JsonValue::Null,
    })
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
struct Subscription {
    inner: Arc<Mutex<Option<CoreSubscription>>>,
}

#[pyclass(module = "primadb._native")]
struct WebSocketSync {
    inner: Arc<Mutex<Option<CoreWebSocketSync>>>,
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

    fn snapshot(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.snapshot())
    }

    #[pyo3(signature = (root=None))]
    fn snapshot_for_root(&self, py: Python<'_>, root: Option<String>) -> PyResult<Py<PyAny>> {
        to_py(py, self.inner.snapshot_for_root(root.as_deref()))
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

    fn subscribe(&self) -> PyResult<Subscription> {
        let subscription = self.inner.subscribe().map_err(to_py_err)?;
        Ok(Subscription {
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

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Primadb>()?;
    module.add_class::<Chain>()?;
    module.add_class::<Subscription>()?;
    module.add_class::<RemoteWatch>()?;
    module.add_class::<WebSocketSync>()?;
    module.add_class::<WebRtcMesh>()?;
    Ok(())
}
