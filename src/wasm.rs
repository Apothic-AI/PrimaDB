use crate::{
    Chain, ChangeSubscription, Operation, Primadb, QuerySpec, Subscription, SyncEnvelope, SyncFrame,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};

#[wasm_bindgen(js_name = Primadb)]
pub struct WasmPrimadb {
    inner: Primadb,
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

#[derive(Debug)]
struct WebSocketSyncState {
    db: Primadb,
    socket: web_sys::WebSocket,
    inflight: BTreeMap<String, SyncEnvelope>,
    next_message_seq: u64,
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

#[wasm_bindgen(js_class = Primadb)]
impl WasmPrimadb {
    #[wasm_bindgen(constructor)]
    pub fn new(replica_id: Option<String>) -> Self {
        let inner = replica_id.map(Primadb::with_replica_id).unwrap_or_default();
        Self { inner }
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

    #[wasm_bindgen(js_name = saveIndexedDb)]
    pub async fn save_indexed_db(
        &self,
        database_name: String,
        store_name: String,
        key: String,
    ) -> std::result::Result<(), JsValue> {
        let payload = self.inner.export_snapshot_json().map_err(to_js_error)?;
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
                    .import_snapshot_json(&payload)
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
                if let Ok(payload) = db.export_snapshot_json() {
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

    #[wasm_bindgen(js_name = connectWebSocket)]
    pub fn connect_web_socket(
        &self,
        url: String,
        retry_interval_ms: Option<i32>,
    ) -> std::result::Result<WasmWebSocketSync, JsValue> {
        let socket = web_sys::WebSocket::new(&url)?;
        socket.set_binary_type(web_sys::BinaryType::Arraybuffer);

        let state = Rc::new(RefCell::new(WebSocketSyncState {
            db: self.inner.clone(),
            socket: socket.clone(),
            inflight: BTreeMap::new(),
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
        let onopen = Closure::wrap(Box::new(move |_event: web_sys::Event| {
            let _ = retry_inflight_state(&onopen_state);
            let _ = flush_pending_state(&onopen_state);
        }) as Box<dyn FnMut(_)>);
        socket.set_onopen(Some(onopen.as_ref().unchecked_ref()));

        let onclose_state = state.clone();
        let onclose = Closure::wrap(Box::new(move |_event: web_sys::CloseEvent| {
            requeue_inflight_state(&onclose_state);
        }) as Box<dyn FnMut(_)>);
        socket.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        let onerror_state = state.clone();
        let onerror = Closure::wrap(Box::new(move |_event: web_sys::Event| {
            requeue_inflight_state(&onerror_state);
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

        let interval_ms = retry_interval_ms.unwrap_or(2_000);
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

    pub fn once(&self) -> std::result::Result<JsValue, JsValue> {
        match self.inner.once_json().map_err(to_js_error)? {
            Some(value) => to_js(&value),
            None => Ok(JsValue::NULL),
        }
    }

    pub fn set(&self, value: JsValue) -> std::result::Result<String, JsValue> {
        self.inner.set(js_to_json(value)?).map_err(to_js_error)
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
        let payload = self.db.export_snapshot_json().map_err(to_js_error)?;
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
    }
}

impl Drop for WasmWebSocketSync {
    fn drop(&mut self) {
        self.teardown();
    }
}

fn handle_websocket_message(
    state: &Rc<RefCell<WebSocketSyncState>>,
    payload: &str,
) -> std::result::Result<(), JsValue> {
    let frame: SyncFrame =
        serde_json::from_str(payload).map_err(|error| JsValue::from_str(&error.to_string()))?;
    match frame {
        SyncFrame::Sync {
            from,
            message_id,
            ops,
        } => {
            let (db, socket) = {
                let state = state.borrow();
                (state.db.clone(), state.socket.clone())
            };
            let applied = db
                .apply_sync_envelope(SyncEnvelope { from, ops })
                .map_err(to_js_error)?;
            if socket.ready_state() == web_sys::WebSocket::OPEN {
                let ack = SyncFrame::Ack {
                    from: db.replica_id(),
                    message_id,
                    applied,
                };
                let payload = serde_json::to_string(&ack).map_err(to_js_error)?;
                socket.send_with_str(&payload)?;
            }
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

    let envelope = db.drain_sync_envelope().map_err(to_js_error)?;
    let count = envelope.ops.len();
    if count == 0 {
        return Ok(0);
    }

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
    let payload = serde_json::to_string(&frame).map_err(to_js_error)?;
    if let Err(error) = socket.send_with_str(&payload) {
        let _ = db.requeue_pending_operations(envelope.ops);
        return Err(error);
    }

    state.borrow_mut().inflight.insert(message_id, envelope);
    Ok(count)
}

fn retry_inflight_state(
    state: &Rc<RefCell<WebSocketSyncState>>,
) -> std::result::Result<usize, JsValue> {
    let (socket, frames) = {
        let state = state.borrow();
        if state.socket.ready_state() != web_sys::WebSocket::OPEN {
            return Ok(0);
        }
        let frames = state
            .inflight
            .iter()
            .map(|(message_id, envelope)| SyncFrame::Sync {
                from: envelope.from.clone(),
                message_id: message_id.clone(),
                ops: envelope.ops.clone(),
            })
            .collect::<Vec<_>>();
        (state.socket.clone(), frames)
    };

    for frame in &frames {
        let payload = serde_json::to_string(frame).map_err(to_js_error)?;
        socket.send_with_str(&payload)?;
    }

    Ok(frames.len())
}

fn requeue_inflight_state(state: &Rc<RefCell<WebSocketSyncState>>) {
    let (db, inflight) = {
        let mut state = state.borrow_mut();
        let inflight = std::mem::take(&mut state.inflight);
        (state.db.clone(), inflight)
    };
    for envelope in inflight.into_values() {
        let _ = db.requeue_pending_operations(envelope.ops);
    }
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
    serde_wasm_bindgen::to_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn to_js_error(error: impl ToString) -> JsValue {
    JsValue::from_str(&error.to_string())
}
