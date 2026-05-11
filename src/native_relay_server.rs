use crate::error::{PrimadbError, Result};
use crate::{RouteEnvelope, RouteRelayCore};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::RelayServerConfig;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
enum RelayMessage {
    Text(String),
    Close,
}

struct RelayState {
    core: RouteRelayCore<UnboundedSender<RelayMessage>>,
}

#[derive(Default)]
struct RelayMetrics {
    client_count: AtomicUsize,
    peer_count: AtomicUsize,
}

pub struct NativeRelayServer {
    local_addr: SocketAddr,
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    shutdown: watch::Sender<bool>,
    accept_task: StdMutex<Option<JoinHandle<()>>>,
}

impl NativeRelayServer {
    pub async fn bind(bind: impl Into<String>) -> Result<Self> {
        Self::bind_with_config(RelayServerConfig::new(bind)).await
    }

    pub async fn bind_with_config(config: RelayServerConfig) -> Result<Self> {
        let bind_addr = config.bind.parse::<SocketAddr>().map_err(|error| {
            PrimadbError::Message(format!(
                "invalid relay bind address `{}`: {error}",
                config.bind
            ))
        })?;
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let state = Arc::new(Mutex::new(RelayState {
            core: RouteRelayCore::new("relay", 8_192),
        }));
        let metrics = Arc::new(RelayMetrics::default());
        let (shutdown, shutdown_rx) = watch::channel(false);

        eprintln!("primadb DAM relay listening on ws://{local_addr}");

        let accept_state = state.clone();
        let accept_metrics = metrics.clone();
        let accept_task = tokio::spawn(async move {
            let _ = accept_loop(listener, accept_state, accept_metrics, shutdown_rx).await;
        });

        Ok(Self {
            local_addr,
            state,
            metrics,
            shutdown,
            accept_task: StdMutex::new(Some(accept_task)),
        })
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn url(&self) -> String {
        socket_addr_to_local_ws_url(self.local_addr)
    }

    pub fn client_count(&self) -> usize {
        self.metrics.client_count.load(Ordering::Relaxed)
    }

    pub fn peer_count(&self) -> usize {
        self.metrics.peer_count.load(Ordering::Relaxed)
    }

    pub async fn close(&self) {
        let _ = self.shutdown.send(true);
        let senders = {
            let state = self.state.lock().await;
            state.core.session_handles_except(u64::MAX)
        };
        for sender in senders {
            let _ = sender.send(RelayMessage::Close);
        }

        let handle = self.accept_task.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

impl Drop for NativeRelayServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(handle) = self.accept_task.lock().unwrap().take() {
            handle.abort();
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            accept_result = listener.accept() => {
                let (stream, peer_addr) = accept_result?;
                let state = state.clone();
                let metrics = metrics.clone();
                let connection_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, peer_addr, state, metrics, connection_shutdown).await {
                        eprintln!("relay connection error from {peer_addr}: {error}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let websocket = accept_async(stream)
        .await
        .map_err(|error| PrimadbError::Message(format!("websocket accept failed: {error}")))?;
    let (mut writer, mut reader) = websocket.split();
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let (sender, mut outbound_rx) = unbounded_channel::<RelayMessage>();

    {
        let mut state = state.lock().await;
        state.core.insert_session(client_id, sender);
        metrics
            .client_count
            .store(state.core.session_count(), Ordering::Relaxed);
    }

    eprintln!("client {client_id} connected from {peer_addr}");
    let mut terminal_error = None;

    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    let _ = writer.send(Message::Close(None)).await;
                    break;
                }
            }
            maybe_outbound = outbound_rx.recv() => {
                match maybe_outbound {
                    Some(RelayMessage::Text(payload)) => {
                        if writer.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(RelayMessage::Close) => {
                        let _ = writer.send(Message::Close(None)).await;
                        break;
                    }
                    None => break,
                }
            }
            maybe_message = reader.next() => {
                match maybe_message {
                    Some(Ok(Message::Text(payload))) => {
                        handle_text_message(state.clone(), metrics.clone(), client_id, payload.to_string()).await?;
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => {
                        terminal_error = Some(PrimadbError::Message(format!(
                            "websocket receive failed: {error}"
                        )));
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    disconnect_client(state, metrics, client_id).await?;
    eprintln!("client {client_id} disconnected");
    if let Some(error) = terminal_error {
        return Err(error);
    }
    Ok(())
}

async fn handle_text_message(
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    client_id: u64,
    payload: String,
) -> Result<()> {
    match serde_json::from_str::<RouteEnvelope>(&payload) {
        Ok(route) => forward_route(state, metrics, client_id, route).await,
        Err(_) => {
            let recipients = {
                let state = state.lock().await;
                state.core.session_handles_except(client_id)
            };
            for recipient in recipients {
                let _ = recipient.send(RelayMessage::Text(payload.clone()));
            }
            Ok(())
        }
    }
}

async fn forward_route(
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    client_id: u64,
    mut route: RouteEnvelope,
) -> Result<()> {
    let forward = {
        let mut state = state.lock().await;
        let forward = state.core.forward_route(client_id, route)?;
        metrics
            .peer_count
            .store(state.core.peer_count(), Ordering::Relaxed);
        forward
    };

    if let Some(bootstrap) = forward.bootstrap {
        send_to_client(&state, client_id, serde_json::to_string(&bootstrap)?).await?;
    }

    if let Some(route) = forward.route {
        let encoded = serde_json::to_string(&route)?;
        for recipient in forward.recipients {
            let _ = recipient.send(RelayMessage::Text(encoded.clone()));
        }
    }
    Ok(())
}

async fn disconnect_client(
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    client_id: u64,
) -> Result<()> {
    let forward = {
        let mut state = state.lock().await;
        let forward = state.core.disconnect_session(client_id);
        metrics
            .client_count
            .store(state.core.session_count(), Ordering::Relaxed);
        metrics
            .peer_count
            .store(state.core.peer_count(), Ordering::Relaxed);
        forward
    };

    if let Some(forward) = forward
        && let Some(route) = forward.route
    {
        let payload = serde_json::to_string(&route)?;
        for recipient in forward.recipients {
            let _ = recipient.send(RelayMessage::Text(payload.clone()));
        }
    }
    Ok(())
}

async fn send_to_client(
    state: &Arc<Mutex<RelayState>>,
    client_id: u64,
    payload: String,
) -> Result<()> {
    let sender = {
        let state = state.lock().await;
        state.core.session_handle(client_id)
    };
    if let Some(sender) = sender {
        let _ = sender.send(RelayMessage::Text(payload));
    }
    Ok(())
}

fn socket_addr_to_local_ws_url(addr: SocketAddr) -> String {
    match addr {
        SocketAddr::V4(addr) => {
            let host = if addr.ip().is_unspecified() {
                "127.0.0.1".to_owned()
            } else {
                addr.ip().to_string()
            };
            format!("ws://{host}:{}", addr.port())
        }
        SocketAddr::V6(addr) => {
            let host = if addr.ip().is_unspecified() {
                "::1".to_owned()
            } else {
                addr.ip().to_string()
            };
            format!("ws://[{host}]:{}", addr.port())
        }
    }
}
