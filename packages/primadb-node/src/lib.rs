use futures::executor::block_on;
use napi::bindgen_prelude::{Buffer, Error, Result, Status};
use napi::threadsafe_function::{ThreadSafeCallContext, ThreadsafeFunction};
use napi::{Env, JsFunction, JsObject, JsUnknown, ValueType};
use napi_derive::napi;
use primadb::{
    BlobStorageBinding as CoreBlobStorageBinding, BlobStorageConfig, Chain as CoreChain,
    ConnectHookContext, DurableStorageBinding as CoreDurableStorageBinding, DurableStorageConfig,
    HookDecision, LexSpec, MeshConfig, NativeWebRtcMesh as CoreWebRtcMesh,
    NativeRelayServer as CoreRelayServer,
    NativeWebSocketSync as CoreWebSocketSync, NetworkHooks, Operation, Primadb as CorePrimadb,
    PullRequestKind, QuerySpec, RelayClientConfig, RelayServerConfig, RemotePath,
    RemoteResult as CoreRemoteResult, RoomHookContext, ServeRequestContext, ServeResultContext,
    RemoteWatchMessage as CoreRemoteWatchMessage, RemoteWatchSubscription as CoreRemoteWatch,
    Subscription as CoreSubscription, parse_request_hook_json, parse_result_hook_json,
    parse_void_hook_json,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::sync::{Arc, Mutex};

fn to_napi_error(error: impl ToString) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn closed_error(kind: &str) -> Error {
    Error::new(Status::GenericFailure, format!("{kind} is closed"))
}

fn to_json<T: Serialize>(value: T) -> Result<JsonValue> {
    serde_json::to_value(value).map_err(to_napi_error)
}

fn from_json<T: DeserializeOwned>(value: JsonValue) -> Result<T> {
    serde_json::from_value(value).map_err(to_napi_error)
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

type ConnectHookTsfn = ThreadsafeFunction<ConnectHookContext>;
type RoomHookTsfn = ThreadsafeFunction<RoomHookContext>;
type RequestHookTsfn = ThreadsafeFunction<ServeRequestContext>;
type ResultHookTsfn = ThreadsafeFunction<NodeServeResultPayload>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NodeServeResultPayload {
    context: ServeResultContext,
    result: CoreRemoteResult,
}

struct NodeNetworkHooks {
    on_connect: Option<ConnectHookTsfn>,
    on_join_room: Option<RoomHookTsfn>,
    on_pull: Option<RequestHookTsfn>,
    on_watch: Option<RequestHookTsfn>,
    on_serve_result: Option<ResultHookTsfn>,
}

impl NetworkHooks for NodeNetworkHooks {
    fn on_connect(&self, context: &ConnectHookContext) -> HookDecision<()> {
        match &self.on_connect {
            Some(function) => match call_node_hook(function, context.clone()) {
                Ok(response) => parse_void_hook_json(Some(response), "connection denied by network hook"),
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(()),
        }
    }

    fn on_join_room(&self, context: &RoomHookContext) -> HookDecision<()> {
        match &self.on_join_room {
            Some(function) => match call_node_hook(function, context.clone()) {
                Ok(response) => parse_void_hook_json(Some(response), "room denied by network hook"),
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(()),
        }
    }

    fn on_pull(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
        match &self.on_pull {
            Some(function) => match call_node_hook(function, context.clone()) {
                Ok(response) => {
                    parse_request_hook_json(Some(response), &context.request, "pull denied by network hook")
                }
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(context.request.clone()),
        }
    }

    fn on_watch(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
        match &self.on_watch {
            Some(function) => match call_node_hook(function, context.clone()) {
                Ok(response) => {
                    parse_request_hook_json(Some(response), &context.request, "watch denied by network hook")
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
        match &self.on_serve_result {
            Some(function) => match call_node_hook(
                function,
                NodeServeResultPayload {
                    context: context.clone(),
                    result: result.clone(),
                },
            ) {
                Ok(response) => {
                    parse_result_hook_json(Some(response), result, "served result denied by network hook")
                }
                Err(message) => HookDecision::deny(message),
            },
            None => HookDecision::allow(result),
        }
    }
}

fn optional_hook_function(hooks: &JsObject, key: &str) -> Result<Option<JsFunction>> {
    if !hooks.has_named_property(key)? {
        return Ok(None);
    }
    let value: JsUnknown = hooks.get_named_property_unchecked(key)?;
    match value.get_type()? {
        ValueType::Undefined | ValueType::Null => Ok(None),
        ValueType::Function => Ok(Some(unsafe { value.cast::<JsFunction>() })),
        _ => Err(Error::new(
            Status::InvalidArg,
            format!("network hook `{key}` must be a function"),
        )),
    }
}

fn normalize_hook_function(env: &Env, function: JsFunction, key: &str) -> Result<JsFunction> {
    let function_ref = env.create_reference(function)?;
    let key = key.to_owned();
    env.create_function_from_closure(&format!("primadb_{key}_hook"), move |ctx| {
        let function: JsFunction = ctx.env.get_reference_value_unchecked(&function_ref)?;
        let mut args = (0..ctx.length)
            .map(|index| ctx.get::<JsUnknown>(index))
            .collect::<Result<Vec<_>>>()?;
        if let Some(error) = args.first() {
            match error.get_type()? {
                ValueType::Null | ValueType::Undefined => {}
                _ => {
                    return Err(Error::new(
                        Status::GenericFailure,
                        format!("network hook `{key}` invocation failed before callback execution"),
                    ));
                }
            }
            args.remove(0);
        }
        let result = function.call(None, &args)?;
        if result.get_type()? == ValueType::Undefined {
            Ok(ctx.env.get_null()?.into_unknown())
        } else {
            Ok(result)
        }
    })
}

fn build_unary_hook_tsfn<T>(env: &Env, hooks: &JsObject, key: &str) -> Result<Option<ThreadsafeFunction<T>>>
where
    T: Serialize + Send + 'static,
{
    let Some(function) = optional_hook_function(hooks, key)? else {
        return Ok(None);
    };
    let function = normalize_hook_function(env, function, key)?;
    function
        .create_threadsafe_function::<T, JsUnknown, _, napi::threadsafe_function::ErrorStrategy::CalleeHandled>(
            0,
            |ctx: ThreadSafeCallContext<T>| Ok(vec![ctx.env.to_js_value(&ctx.value)?]),
        )
        .map(Some)
}

fn build_result_hook_tsfn(env: &Env, hooks: &JsObject, key: &str) -> Result<Option<ResultHookTsfn>> {
    let Some(function) = optional_hook_function(hooks, key)? else {
        return Ok(None);
    };
    let function = normalize_hook_function(env, function, key)?;
    function
        .create_threadsafe_function::<
            NodeServeResultPayload,
            JsUnknown,
            _,
            napi::threadsafe_function::ErrorStrategy::CalleeHandled,
        >(0, |ctx: ThreadSafeCallContext<NodeServeResultPayload>| {
            Ok(vec![
                ctx.env.to_js_value(&ctx.value.context)?,
                ctx.env.to_js_value(&ctx.value.result)?,
            ])
        })
        .map(Some)
}

fn call_node_hook<T>(function: &ThreadsafeFunction<T>, payload: T) -> std::result::Result<JsonValue, String>
where
    T: 'static,
{
    block_on(function.call_async::<JsonValue>(Ok(payload))).map_err(|error| error.to_string())
}

#[napi]
pub struct Primadb {
    inner: CorePrimadb,
}

#[napi]
pub struct Chain {
    inner: CoreChain,
}

#[napi]
pub struct Subscription {
    inner: Arc<Mutex<Option<CoreSubscription>>>,
}

#[napi]
pub struct WebSocketSync {
    inner: Arc<Mutex<Option<CoreWebSocketSync>>>,
}

#[napi]
pub struct RelayServer {
    inner: Arc<Mutex<Option<CoreRelayServer>>>,
}

#[napi]
pub struct RemoteWatch {
    inner: Arc<Mutex<Option<CoreRemoteWatch>>>,
}

#[napi]
pub struct WebRtcMesh {
    inner: Arc<Mutex<Option<CoreWebRtcMesh>>>,
}

#[napi]
impl Primadb {
    #[napi(constructor)]
    pub fn new(replica_id: Option<String>) -> Self {
        let inner = replica_id
            .map(CorePrimadb::with_replica_id)
            .unwrap_or_default();
        Self { inner }
    }

    #[napi(js_name = "replicaId")]
    pub fn replica_id(&self) -> String {
        self.inner.replica_id()
    }

    #[napi]
    pub fn chain(&self, root: String) -> Chain {
        Chain {
            inner: self.inner.root(root),
        }
    }

    #[napi]
    pub fn snapshot(&self) -> Result<JsonValue> {
        to_json(self.inner.snapshot())
    }

    #[napi(js_name = "snapshotForRoot")]
    pub fn snapshot_for_root(&self, root: Option<String>) -> Result<JsonValue> {
        to_json(self.inner.snapshot_for_root(root.as_deref()))
    }

    #[napi(js_name = "exportSnapshotJson")]
    pub fn export_snapshot_json(&self) -> Result<String> {
        self.inner.export_snapshot_json().map_err(to_napi_error)
    }

    #[napi(js_name = "importSnapshotJson")]
    pub fn import_snapshot_json(&self, payload: String) -> Result<()> {
        self.inner.import_snapshot_json(&payload).map_err(to_napi_error)
    }

    #[napi(js_name = "mergeSnapshotJson")]
    pub fn merge_snapshot_json(&self, payload: String) -> Result<()> {
        self.inner.merge_snapshot_json(&payload).map_err(to_napi_error)
    }

    #[napi(js_name = "pendingOperations")]
    pub fn pending_operations(&self) -> Result<JsonValue> {
        to_json(self.inner.pending_operations())
    }

    #[napi(js_name = "pendingEnvelope")]
    pub fn pending_envelope(&self) -> Result<JsonValue> {
        to_json(self.inner.sync_envelope())
    }

    #[napi(js_name = "exportPendingOperationsJson")]
    pub fn export_pending_operations_json(&self) -> Result<String> {
        self.inner
            .export_pending_operations_json()
            .map_err(to_napi_error)
    }

    #[napi(js_name = "drainPendingOperations")]
    pub fn drain_pending_operations(&self) -> Result<JsonValue> {
        to_json(self.inner.drain_pending_operations().map_err(to_napi_error)?)
    }

    #[napi(js_name = "drainPendingEnvelope")]
    pub fn drain_pending_envelope(&self) -> Result<JsonValue> {
        to_json(self.inner.drain_sync_envelope().map_err(to_napi_error)?)
    }

    #[napi(js_name = "drainPendingEnvelopeJson")]
    pub fn drain_pending_envelope_json(&self) -> Result<String> {
        self.inner
            .drain_pending_envelope_json()
            .map_err(to_napi_error)
    }

    #[napi(js_name = "applyOperations")]
    pub fn apply_operations(&self, operations: JsonValue) -> Result<u32> {
        let operations: Vec<Operation> = from_json(operations)?;
        let applied = self.inner.apply_operations(operations).map_err(to_napi_error)?;
        u32::try_from(applied).map_err(to_napi_error)
    }

    #[napi(js_name = "applyEnvelope")]
    pub fn apply_envelope(&self, envelope: JsonValue) -> Result<u32> {
        let envelope = from_json(envelope)?;
        let applied = self.inner.apply_sync_envelope(envelope).map_err(to_napi_error)?;
        u32::try_from(applied).map_err(to_napi_error)
    }

    #[napi(js_name = "applyOperationsJson")]
    pub fn apply_operations_json(&self, payload: String) -> Result<u32> {
        let applied = self
            .inner
            .apply_operations_json(&payload)
            .map_err(to_napi_error)?;
        u32::try_from(applied).map_err(to_napi_error)
    }

    #[napi(js_name = "openDurableStorage")]
    pub fn open_durable_storage(&self, config: JsonValue) -> Result<JsonValue> {
        let config: DurableStorageConfig = from_json(config)?;
        let binding = self
            .inner
            .open_durable_storage(config)
            .map_err(to_napi_error)?;
        Ok(binding_to_json(binding))
    }

    #[napi(js_name = "openBlobStorage")]
    pub fn open_blob_storage(&self, config: JsonValue) -> Result<JsonValue> {
        let config: BlobStorageConfig = from_json(config)?;
        let binding = self.inner.open_blob_storage(config).map_err(to_napi_error)?;
        Ok(blob_binding_to_json(binding))
    }

    #[napi(js_name = "connectRelay")]
    pub async fn connect_relay(&self, config: JsonValue) -> Result<WebSocketSync> {
        let config: RelayClientConfig = from_json(config)?;
        let sync = self.inner.connect_relay(config).await.map_err(to_napi_error)?;
        Ok(WebSocketSync {
            inner: Arc::new(Mutex::new(Some(sync))),
        })
    }

    #[napi(js_name = "connectMesh")]
    pub async fn connect_mesh(&self, config: JsonValue) -> Result<WebRtcMesh> {
        let config: MeshConfig = from_json(config)?;
        let mesh = self.inner.connect_mesh(config).await.map_err(to_napi_error)?;
        Ok(WebRtcMesh {
            inner: Arc::new(Mutex::new(Some(mesh))),
        })
    }

    #[napi(js_name = "setNetworkHooks")]
    pub fn set_network_hooks(&self, env: Env, hooks: JsObject) -> Result<()> {
        let hooks = NodeNetworkHooks {
            on_connect: build_unary_hook_tsfn(&env, &hooks, "onConnect")?,
            on_join_room: build_unary_hook_tsfn(&env, &hooks, "onJoinRoom")?,
            on_pull: build_unary_hook_tsfn(&env, &hooks, "onPull")?,
            on_watch: build_unary_hook_tsfn(&env, &hooks, "onWatch")?,
            on_serve_result: build_result_hook_tsfn(&env, &hooks, "onServeResult")?,
        };
        self.inner.set_network_hooks(Arc::new(hooks));
        Ok(())
    }

    #[napi(js_name = "clearNetworkHooks")]
    pub fn clear_network_hooks(&self) {
        self.inner.clear_network_hooks();
    }
}

#[napi]
impl Chain {
    #[napi]
    pub fn field(&self, key: String) -> Chain {
        Chain {
            inner: self.inner.field(key),
        }
    }

    #[napi]
    pub fn path(&self) -> String {
        self.inner.path()
    }

    #[napi]
    pub fn put(&self, value: JsonValue) -> Result<()> {
        self.inner.put(value).map_err(to_napi_error)
    }

    #[napi(js_name = "putBytes")]
    pub fn put_bytes(&self, bytes: Buffer) -> Result<()> {
        self.inner.put_bytes(bytes.to_vec()).map_err(to_napi_error)
    }

    #[napi(js_name = "putSigned")]
    pub fn put_signed(&self, value: JsonValue, certificate: Option<String>) -> Result<()> {
        self.inner
            .put_signed(value, certificate)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn once(&self) -> Result<JsonValue> {
        Ok(self.inner.once_json().map_err(to_napi_error)?.unwrap_or(JsonValue::Null))
    }

    #[napi(js_name = "onceBytes")]
    pub fn once_bytes(&self) -> Result<Option<Buffer>> {
        self.inner
            .once_bytes()
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn unset(&self) -> Result<()> {
        self.inner.unset().map_err(to_napi_error)
    }

    #[napi]
    pub fn set(&self, value: JsonValue) -> Result<String> {
        self.inner.set(value).map_err(to_napi_error)
    }

    #[napi(js_name = "setSigned")]
    pub fn set_signed(&self, value: JsonValue, certificate: Option<String>) -> Result<String> {
        self.inner
            .set_signed(value, certificate)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn remove(&self, value: JsonValue) -> Result<String> {
        self.inner.remove(value).map_err(to_napi_error)
    }

    #[napi(js_name = "putBlob")]
    pub fn put_blob(&self, bytes: Buffer, media_type: Option<String>) -> Result<JsonValue> {
        to_json(
            self.inner
                .put_blob(bytes.to_vec(), media_type.as_deref())
                .map_err(to_napi_error)?,
        )
    }

    #[napi(js_name = "blobRef")]
    pub fn blob_ref(&self) -> Result<JsonValue> {
        to_json(self.inner.once_blob_ref().map_err(to_napi_error)?)
    }

    #[napi(js_name = "getBlob")]
    pub fn get_blob(&self) -> Result<Option<Buffer>> {
        self.inner
            .get_blob()
            .map(|value| value.map(|blob| Buffer::from(blob.data.into_inner())))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn map(&self) -> Result<JsonValue> {
        to_json(self.inner.map().map_err(to_napi_error)?)
    }

    #[napi]
    pub fn query(&self, spec: JsonValue) -> Result<JsonValue> {
        let spec: QuerySpec = from_json(spec)?;
        to_json(self.inner.query(spec).map_err(to_napi_error)?)
    }

    #[napi(js_name = "firstQuery")]
    pub fn first_query(&self, spec: JsonValue) -> Result<JsonValue> {
        let spec: QuerySpec = from_json(spec)?;
        Ok(self.inner.first(spec).map_err(to_napi_error)?.map_or(JsonValue::Null, |entry| {
            serde_json::to_value(entry).unwrap_or(JsonValue::Null)
        }))
    }

    #[napi]
    pub fn scan(&self, spec: JsonValue) -> Result<JsonValue> {
        let spec: LexSpec = from_json(spec)?;
        to_json(self.inner.scan(spec).map_err(to_napi_error)?)
    }

    #[napi]
    pub fn subscribe(&self) -> Result<Subscription> {
        let subscription = self.inner.subscribe().map_err(to_napi_error)?;
        Ok(Subscription {
            inner: Arc::new(Mutex::new(Some(subscription))),
        })
    }
}

#[napi]
impl Subscription {
    #[napi]
    pub async fn next(&self) -> Result<JsonValue> {
        let subscription = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("subscription"))?
        };

        let message = match subscription.recv().await {
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

        Ok(message)
    }

    #[napi(js_name = "tryNext")]
    pub fn try_next(&self) -> Result<JsonValue> {
        let guard = self.inner.lock().unwrap();
        let Some(subscription) = guard.as_ref() else {
            return Ok(json!({
                "done": true,
                "value": JsonValue::Null,
            }));
        };
        match subscription.try_recv() {
            Some(value) => Ok(json!({
                "done": false,
                "value": value,
            })),
            None => Ok(json!({
                "done": false,
                "value": JsonValue::Null,
            })),
        }
    }

    #[napi]
    pub fn close(&self) {
        let _ = self.inner.lock().unwrap().take();
    }
}

#[napi]
impl RelayServer {
    #[napi(factory, js_name = "listen")]
    pub async fn listen(config: JsonValue) -> Result<RelayServer> {
        let config: RelayServerConfig = from_json(config)?;
        let server = CoreRelayServer::bind_with_config(config)
            .await
            .map_err(to_napi_error)?;
        Ok(RelayServer {
            inner: Arc::new(Mutex::new(Some(server))),
        })
    }

    #[napi(js_name = "bindAddr")]
    pub fn bind_addr(&self) -> Result<String> {
        let guard = self.inner.lock().unwrap();
        let server = guard.as_ref().ok_or_else(|| closed_error("relay server"))?;
        Ok(server.bind_addr().to_string())
    }

    #[napi]
    pub fn url(&self) -> Result<String> {
        let guard = self.inner.lock().unwrap();
        let server = guard.as_ref().ok_or_else(|| closed_error("relay server"))?;
        Ok(server.url())
    }

    #[napi(js_name = "clientCount")]
    pub fn client_count(&self) -> Result<u32> {
        let guard = self.inner.lock().unwrap();
        let server = guard.as_ref().ok_or_else(|| closed_error("relay server"))?;
        u32::try_from(server.client_count()).map_err(to_napi_error)
    }

    #[napi(js_name = "peerCount")]
    pub fn peer_count(&self) -> Result<u32> {
        let guard = self.inner.lock().unwrap();
        let server = guard.as_ref().ok_or_else(|| closed_error("relay server"))?;
        u32::try_from(server.peer_count()).map_err(to_napi_error)
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        let server = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("relay server"))?
        };
        server.close().await;
        Ok(())
    }
}

#[napi]
impl RemoteWatch {
    #[napi]
    pub async fn next(&self) -> Result<JsonValue> {
        let watch = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("remote watch"))?
        };

        let message = match watch.recv().await {
            Some(Ok(value)) => {
                self.inner.lock().unwrap().replace(watch);
                watch_message_to_json(value)
            }
            Some(Err(message)) => {
                json!({
                    "done": false,
                    "initial": false,
                    "kind": JsonValue::Null,
                    "value": JsonValue::Null,
                    "error": message,
                })
            }
            None => json!({
                "done": true,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": JsonValue::Null,
            }),
        };

        Ok(message)
    }

    #[napi(js_name = "tryNext")]
    pub fn try_next(&self) -> Result<JsonValue> {
        let guard = self.inner.lock().unwrap();
        let Some(watch) = guard.as_ref() else {
            return Ok(json!({
                "done": true,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": JsonValue::Null,
            }));
        };
        match watch.try_recv() {
            Some(Ok(value)) => Ok(watch_message_to_json(value)),
            Some(Err(message)) => Ok(json!({
                "done": false,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": message,
            })),
            None => Ok(json!({
                "done": false,
                "initial": false,
                "kind": JsonValue::Null,
                "value": JsonValue::Null,
                "error": JsonValue::Null,
            })),
        }
    }

    #[napi]
    pub fn close(&self) {
        if let Some(watch) = self.inner.lock().unwrap().take() {
            watch.close();
        }
    }
}

#[napi]
impl WebSocketSync {
    #[napi(js_name = "isConnected")]
    pub fn is_connected(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(CoreWebSocketSync::is_connected)
    }

    #[napi(js_name = "pendingCount")]
    pub fn pending_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|sync| sync.pending_count() as u32)
            .unwrap_or(0)
    }

    #[napi(js_name = "inflightCount")]
    pub fn inflight_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|sync| sync.inflight_count() as u32)
            .unwrap_or(0)
    }

    #[napi(js_name = "knownPeerCount")]
    pub fn known_peer_count(&self) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|sync| sync.known_peer_count() as u32)
            .unwrap_or(0)
    }

    #[napi(js_name = "recommendedPeers")]
    pub fn recommended_peers(&self) -> Result<JsonValue> {
        let peers = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(CoreWebSocketSync::recommended_peers)
            .unwrap_or_default();
        to_json(peers)
    }

    #[napi(js_name = "remoteGet")]
    pub async fn remote_get(&self, peer_id: String, path: JsonValue) -> Result<JsonValue> {
        let path: RemotePath = from_json(path)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = sync.remote_get(peer_id, path).await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(sync);
        Ok(result?.unwrap_or(JsonValue::Null))
    }

    #[napi(js_name = "remoteQuery")]
    pub async fn remote_query(
        &self,
        peer_id: String,
        path: JsonValue,
        spec: JsonValue,
    ) -> Result<JsonValue> {
        let path: RemotePath = from_json(path)?;
        let spec: QuerySpec = from_json(spec)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = sync
            .remote_query(peer_id, path, spec)
            .await
            .map_err(to_napi_error);
        self.inner.lock().unwrap().replace(sync);
        to_json(result?)
    }

    #[napi(js_name = "remoteLex")]
    pub async fn remote_lex(
        &self,
        peer_id: String,
        path: JsonValue,
        spec: JsonValue,
    ) -> Result<JsonValue> {
        let path: RemotePath = from_json(path)?;
        let spec: LexSpec = from_json(spec)?;
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = sync
            .remote_lex(peer_id, path, spec)
            .await
            .map_err(to_napi_error);
        self.inner.lock().unwrap().replace(sync);
        to_json(result?)
    }

    #[napi(js_name = "remoteSnapshot")]
    pub async fn remote_snapshot(&self, peer_id: String, root: Option<String>) -> Result<JsonValue> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = sync
            .remote_snapshot(peer_id, root)
            .await
            .map_err(to_napi_error);
        self.inner.lock().unwrap().replace(sync);
        to_json(result?)
    }

    #[napi(js_name = "watchRemoteGet")]
    pub fn watch_remote_get(&self, peer_id: String, path: JsonValue) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_get(peer_id, path)
            .map_err(to_napi_error)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[napi(js_name = "watchRemoteMap")]
    pub fn watch_remote_map(&self, peer_id: String, path: JsonValue) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_map(peer_id, path)
            .map_err(to_napi_error)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[napi(js_name = "watchRemoteQuery")]
    pub fn watch_remote_query(
        &self,
        peer_id: String,
        path: JsonValue,
        spec: JsonValue,
    ) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let spec: QuerySpec = from_json(spec)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_query(peer_id, path, spec)
            .map_err(to_napi_error)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[napi(js_name = "watchRemoteLex")]
    pub fn watch_remote_lex(
        &self,
        peer_id: String,
        path: JsonValue,
        spec: JsonValue,
    ) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let spec: LexSpec = from_json(spec)?;
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_lex(peer_id, path, spec)
            .map_err(to_napi_error)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[napi(js_name = "watchRemoteSnapshot")]
    pub fn watch_remote_snapshot(&self, peer_id: String, root: Option<String>) -> Result<RemoteWatch> {
        let watch = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| closed_error("websocket sync"))?
            .watch_snapshot(peer_id, root)
            .map_err(to_napi_error)?;
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(watch))),
        })
    }

    #[napi(js_name = "flushPending")]
    pub async fn flush_pending(&self) -> Result<u32> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = sync.flush_pending().await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(sync);
        u32::try_from(result?).map_err(to_napi_error)
    }

    #[napi(js_name = "retryInflight")]
    pub async fn retry_inflight(&self) -> Result<u32> {
        let sync = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("websocket sync"))?
        };
        let result = sync.retry_inflight().await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(sync);
        u32::try_from(result?).map_err(to_napi_error)
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        if let Some(mut sync) = self.inner.lock().unwrap().take() {
            sync.close();
        }
        Ok(())
    }
}

#[napi]
impl WebRtcMesh {
    #[napi(js_name = "peerId")]
    pub fn peer_id(&self) -> String {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|mesh| mesh.peer_id())
            .unwrap_or_default()
    }

    #[napi(js_name = "signalingMode")]
    pub fn signaling_mode(&self) -> String {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|mesh| mesh.signaling_mode().to_owned())
            .unwrap_or_default()
    }

    #[napi(js_name = "relayUrl")]
    pub fn relay_url(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|mesh| mesh.relay_url().to_owned())
    }

    #[napi(js_name = "relayConnected")]
    pub fn relay_connected(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(CoreWebRtcMesh::relay_connected)
    }

    #[napi(js_name = "peerCount")]
    pub async fn peer_count(&self) -> Result<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.peer_count().await;
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result).map_err(to_napi_error)
    }

    #[napi(js_name = "openPeerCount")]
    pub async fn open_peer_count(&self) -> Result<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.open_peer_count().await;
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result).map_err(to_napi_error)
    }

    #[napi(js_name = "inflightCount")]
    pub async fn inflight_count(&self) -> Result<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.inflight_count().await;
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result).map_err(to_napi_error)
    }

    #[napi(js_name = "recommendedPeers")]
    pub async fn recommended_peers(&self) -> Result<JsonValue> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let peers = mesh.recommended_peers().await;
        self.inner.lock().unwrap().replace(mesh);
        to_json(peers)
    }

    #[napi(js_name = "watchRemoteGet")]
    pub async fn watch_remote_get(&self, peer_id: String, path: JsonValue) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.watch_get(peer_id, path).await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result?))),
        })
    }

    #[napi(js_name = "watchRemoteMap")]
    pub async fn watch_remote_map(&self, peer_id: String, path: JsonValue) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.watch_map(peer_id, path).await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result?))),
        })
    }

    #[napi(js_name = "watchRemoteQuery")]
    pub async fn watch_remote_query(
        &self,
        peer_id: String,
        path: JsonValue,
        spec: JsonValue,
    ) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let spec: QuerySpec = from_json(spec)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh
            .watch_query(peer_id, path, spec)
            .await
            .map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result?))),
        })
    }

    #[napi(js_name = "watchRemoteLex")]
    pub async fn watch_remote_lex(
        &self,
        peer_id: String,
        path: JsonValue,
        spec: JsonValue,
    ) -> Result<RemoteWatch> {
        let path: RemotePath = from_json(path)?;
        let spec: LexSpec = from_json(spec)?;
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.watch_lex(peer_id, path, spec).await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result?))),
        })
    }

    #[napi(js_name = "watchRemoteSnapshot")]
    pub async fn watch_remote_snapshot(
        &self,
        peer_id: String,
        root: Option<String>,
    ) -> Result<RemoteWatch> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.watch_snapshot(peer_id, root).await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        Ok(RemoteWatch {
            inner: Arc::new(Mutex::new(Some(result?))),
        })
    }

    #[napi(js_name = "flushPending")]
    pub async fn flush_pending(&self) -> Result<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.flush_pending().await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result?).map_err(to_napi_error)
    }

    #[napi(js_name = "retryInflight")]
    pub async fn retry_inflight(&self) -> Result<u32> {
        let mesh = {
            let mut guard = self.inner.lock().unwrap();
            guard.take().ok_or_else(|| closed_error("webrtc mesh"))?
        };
        let result = mesh.retry_inflight().await.map_err(to_napi_error);
        self.inner.lock().unwrap().replace(mesh);
        u32::try_from(result?).map_err(to_napi_error)
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        let mesh = self.inner.lock().unwrap().take();
        if let Some(mut mesh) = mesh {
            mesh.close().await;
        }
        Ok(())
    }
}
