use crate::{ChangeSubscription, Primadb, PrimadbError, Result, SyncEnvelope, SyncFrame};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
struct NativeWebSocketSyncState {
    db: Primadb,
    connected: AtomicBool,
    next_message_seq: AtomicU64,
    inflight: Mutex<BTreeMap<String, SyncEnvelope>>,
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

        let state = Arc::new(NativeWebSocketSyncState {
            db: db.clone(),
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
    let frame: SyncFrame =
        serde_json::from_str(payload).map_err(|error| PrimadbError::Message(error.to_string()))?;
    match frame {
        SyncFrame::Sync {
            from,
            message_id,
            ops,
        } => {
            let applied = state.db.apply_sync_envelope(SyncEnvelope { from, ops })?;
            send_frame(
                state,
                &SyncFrame::Ack {
                    from: state.db.replica_id(),
                    message_id,
                    applied,
                },
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

    let envelope = state.db.drain_sync_envelope()?;
    let count = envelope.ops.len();
    if count == 0 {
        return Ok(0);
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
    if let Err(error) = send_frame(state, &frame) {
        let _ = state.db.requeue_pending_operations(envelope.ops);
        return Err(error);
    }

    state.inflight.lock().unwrap().insert(message_id, envelope);
    Ok(count)
}

async fn retry_inflight_state(state: &Arc<NativeWebSocketSyncState>) -> Result<usize> {
    if !state.connected.load(Ordering::SeqCst) {
        return Ok(0);
    }

    let frames = {
        let inflight = state.inflight.lock().unwrap();
        inflight
            .iter()
            .map(|(message_id, envelope)| SyncFrame::Sync {
                from: envelope.from.clone(),
                message_id: message_id.clone(),
                ops: envelope.ops.clone(),
            })
            .collect::<Vec<_>>()
    };

    for frame in &frames {
        send_frame(state, frame)?;
    }

    Ok(frames.len())
}

fn send_frame(state: &Arc<NativeWebSocketSyncState>, frame: &SyncFrame) -> Result<()> {
    if !state.connected.load(Ordering::SeqCst) {
        return Err(PrimadbError::Message(
            "native websocket is not connected".to_owned(),
        ));
    }
    let payload = serde_json::to_string(frame)?;
    state
        .outbound
        .send(Message::Text(payload.into()))
        .map_err(|error| PrimadbError::Message(error.to_string()))
}

fn requeue_inflight_state(state: &Arc<NativeWebSocketSyncState>) {
    let inflight = std::mem::take(&mut *state.inflight.lock().unwrap());
    for envelope in inflight.into_values() {
        let _ = state.db.requeue_pending_operations(envelope.ops);
    }
}

#[cfg(test)]
mod tests {
    use super::NativeWebSocketSync;
    use crate::Primadb;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::broadcast;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(Clone, Debug)]
    struct RelayMessage {
        sender: u64,
        payload: String,
    }

    #[tokio::test]
    async fn native_websocket_sync_replicates_between_replicas() {
        let addr = spawn_test_relay().await;
        let left = Primadb::with_replica_id("left");
        let right = Primadb::with_replica_id("right");

        let mut left_sync =
            NativeWebSocketSync::connect(left.clone(), format!("ws://{addr}"), Duration::from_millis(100))
                .await
                .unwrap();
        let mut right_sync =
            NativeWebSocketSync::connect(right.clone(), format!("ws://{addr}"), Duration::from_millis(100))
                .await
                .unwrap();

        left.root("docs")
            .field("hello")
            .put(json!({"value": "world"}))
            .unwrap();

        tokio::time::sleep(Duration::from_millis(400)).await;
        let snapshot = right.root("docs").field("hello").once_json().unwrap().unwrap();
        assert_eq!(snapshot["value"], "world");

        left_sync.close();
        right_sync.close();
    }

    async fn spawn_test_relay() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, _) = broadcast::channel::<RelayMessage>(128);

        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let websocket = accept_async(stream).await.unwrap();
                    let (mut writer, mut reader) = websocket.split();
                    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
                    let mut rx = tx.subscribe();

                    let write_task = tokio::spawn(async move {
                        while let Ok(message) = rx.recv().await {
                            if message.sender == client_id {
                                continue;
                            }
                            if writer
                                .send(Message::Text(message.payload.into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });

                    while let Some(message) = reader.next().await {
                        match message.unwrap() {
                            Message::Text(payload) => {
                                let _ = tx.send(RelayMessage {
                                    sender: client_id,
                                    payload: payload.to_string(),
                                });
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }

                    write_task.abort();
                });
            }
        });

        addr
    }
}
