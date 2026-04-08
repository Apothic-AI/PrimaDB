use crate::{
    ChangeSubscription, Primadb, PrimadbError, Result, RouteEnvelope, RoutePayload, RouteTarget,
    Router, RouterConfig, SyncEnvelope, SyncFrame,
};
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
struct NativeWebSocketSyncState {
    db: Primadb,
    router: Router,
    connected: AtomicBool,
    next_message_seq: AtomicU64,
    inflight: Mutex<BTreeMap<String, OutboundSync>>,
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
        let (socket, _) = connect_async(url.as_ref())
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
            outbound,
        });

        let writer_state = state.clone();
        let writer_task = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                if writer.send(message).await.is_err() {
                    writer_state.connected.store(false, Ordering::SeqCst);
                    requeue_inflight_state(&writer_state);
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

        send_route(
            &state,
            &state.router.presence(
                db.replica_id(),
                "websocket",
                vec![
                    "sync".to_owned(),
                    "ack".to_owned(),
                    "routing".to_owned(),
                    "snapshot".to_owned(),
                ],
                vec!["primadb-sync".to_owned()],
            ),
        )?;

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
    }
}

impl Drop for NativeWebSocketSync {
    fn drop(&mut self) {
        self.teardown();
    }
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

    match route.payload {
        RoutePayload::Presence { .. } => Ok(()),
        RoutePayload::Signal { .. } => Ok(()),
        RoutePayload::SnapshotRequest { root } => {
            let response = state.router.snapshot_response(
                root,
                state.db.snapshot(),
                RouteTarget::Peer(route.from),
            );
            send_route(state, &response)
        }
        RoutePayload::SnapshotResponse { snapshot, .. } => state.db.load_snapshot(snapshot),
        RoutePayload::Sync { encoding, payload } => {
            let frame = decode_sync_payload(&state.db, &encoding, payload)?;
            handle_sync_frame(state, frame, Some(route.from)).await
        }
    }
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
