use crate::error::{PrimadbError, Result};
use crate::{
    PeerPresence, PeerRecommendation, RouteBatchItem, RouteEnvelope, RoutePayload, RouteTarget,
    stable_content_hash,
};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::RelayServerConfig;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_ROUTE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
enum RelayMessage {
    Text(String),
    Close,
}

#[derive(Clone, Debug)]
struct PresenceRecord {
    peer: PeerPresence,
    route: RouteEnvelope,
}

#[derive(Clone)]
struct ClientHandle {
    sender: UnboundedSender<RelayMessage>,
    presence: Option<PresenceRecord>,
}

#[derive(Default)]
struct RelayState {
    clients: BTreeMap<u64, ClientHandle>,
    peer_index: BTreeMap<String, u64>,
    seen_routes: BTreeMap<String, u64>,
    seen_content: BTreeMap<String, u64>,
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
        let state = Arc::new(Mutex::new(RelayState::default()));
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
            state
                .clients
                .values()
                .map(|handle| handle.sender.clone())
                .collect::<Vec<_>>()
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
        state.clients.insert(
            client_id,
            ClientHandle {
                sender,
                presence: None,
            },
        );
        metrics
            .client_count
            .store(state.clients.len(), Ordering::Relaxed);
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
                state
                    .clients
                    .iter()
                    .filter_map(|(candidate_id, handle)| {
                        if *candidate_id == client_id {
                            None
                        } else {
                            Some(handle.sender.clone())
                        }
                    })
                    .collect::<Vec<_>>()
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
    let (bootstrap, recipients, encoded) = {
        let mut state = state.lock().await;

        if route.seen_by.iter().any(|peer_id| peer_id == "relay")
            || state.seen_routes.contains_key(&route.route_id)
            || dedupe_key(&route)
                .as_ref()
                .is_some_and(|key| state.seen_content.contains_key(key))
        {
            return Ok(());
        }

        let seen_at = now_millis();
        state.seen_routes.insert(route.route_id.clone(), seen_at);
        trim_seen_cache(&mut state.seen_routes, 8_192);
        if let Some(key) = dedupe_key(&route) {
            state.seen_content.insert(key, seen_at);
            trim_seen_cache(&mut state.seen_content, 8_192);
        }

        if !route.seen_by.iter().any(|peer_id| peer_id == "relay") {
            route.seen_by.push("relay".to_owned());
        }
        if route.content_hash.is_none() {
            route.content_hash = stable_content_hash(&route.payload);
        }

        let mut bootstrap = None;
        if let RoutePayload::Presence { peer } = &route.payload {
            let existing = state
                .clients
                .iter()
                .filter_map(|(candidate_id, handle)| {
                    if *candidate_id == client_id {
                        None
                    } else {
                        handle
                            .presence
                            .as_ref()
                            .map(|presence| presence.route.clone())
                    }
                })
                .collect::<Vec<_>>();
            let recommendations = collect_peer_recommendations(&state, Some(client_id));
            bootstrap = build_bootstrap_route(
                route.channel.clone(),
                peer.peer_id.clone(),
                existing,
                recommendations,
            );

            let previous_peer_id = state
                .clients
                .get(&client_id)
                .and_then(|handle| handle.presence.as_ref())
                .map(|presence| presence.peer.peer_id.clone());
            if let Some(previous_peer_id) = previous_peer_id {
                state.peer_index.remove(&previous_peer_id);
            }
            state.peer_index.insert(peer.peer_id.clone(), client_id);
            if let Some(handle) = state.clients.get_mut(&client_id) {
                handle.presence = Some(PresenceRecord {
                    peer: peer.clone(),
                    route: route.clone(),
                });
            }
            metrics
                .peer_count
                .store(state.peer_index.len(), Ordering::Relaxed);
        }

        let recipients = collect_route_recipients(&state, client_id, &route);
        let encoded = serde_json::to_string(&route)?;
        (bootstrap, recipients, encoded)
    };

    if let Some(bootstrap) = bootstrap {
        send_to_client(&state, client_id, serde_json::to_string(&bootstrap)?).await?;
    }

    for recipient in recipients {
        let _ = recipient.send(RelayMessage::Text(encoded.clone()));
    }
    Ok(())
}

fn collect_route_recipients(
    state: &RelayState,
    sender_id: u64,
    route: &RouteEnvelope,
) -> Vec<UnboundedSender<RelayMessage>> {
    match &route.target {
        RouteTarget::Peer(peer_id) => state
            .peer_index
            .get(peer_id)
            .and_then(|client_id| {
                if *client_id == sender_id {
                    None
                } else {
                    state
                        .clients
                        .get(client_id)
                        .map(|handle| handle.sender.clone())
                }
            })
            .into_iter()
            .collect(),
        RouteTarget::Broadcast => state
            .clients
            .iter()
            .filter_map(|(candidate_id, handle)| {
                if *candidate_id == sender_id {
                    return None;
                }
                match &handle.presence {
                    Some(presence) if presence.route.channel == route.channel => {
                        Some(handle.sender.clone())
                    }
                    Some(_) => None,
                    None => Some(handle.sender.clone()),
                }
            })
            .collect(),
        RouteTarget::Topic(topic) => state
            .clients
            .iter()
            .filter_map(|(candidate_id, handle)| {
                if *candidate_id == sender_id {
                    return None;
                }
                let Some(presence) = &handle.presence else {
                    return None;
                };
                if presence
                    .peer
                    .topics
                    .iter()
                    .any(|candidate| candidate == topic)
                    || presence.route.channel == *topic
                {
                    Some(handle.sender.clone())
                } else {
                    None
                }
            })
            .collect(),
    }
}

fn collect_peer_recommendations(
    state: &RelayState,
    exclude_client_id: Option<u64>,
) -> Vec<PeerRecommendation> {
    state
        .clients
        .iter()
        .filter_map(|(client_id, handle)| {
            if exclude_client_id == Some(*client_id) {
                return None;
            }
            let presence = handle.presence.as_ref()?;
            let relay_urls = presence
                .peer
                .metadata
                .get("relay_url")
                .into_iter()
                .flat_map(|value| value.split(','))
                .map(str::trim)
                .filter(|candidate| !candidate.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            Some(PeerRecommendation {
                peer: presence.peer.clone(),
                relay_urls,
                score: recommendation_score(&presence.peer),
                discovered_at_millis: now_millis(),
            })
        })
        .collect()
}

fn build_bootstrap_route(
    channel: String,
    target_peer_id: String,
    existing: Vec<RouteEnvelope>,
    recommendations: Vec<PeerRecommendation>,
) -> Option<RouteEnvelope> {
    let mut items = existing
        .into_iter()
        .filter_map(|route| match route.payload {
            RoutePayload::Presence { peer } => Some(RouteBatchItem::Presence { peer }),
            _ => None,
        })
        .collect::<Vec<_>>();

    if !recommendations.is_empty() {
        items.push(RouteBatchItem::PeerExchange {
            peers: recommendations,
        });
    }

    if items.is_empty() {
        return None;
    }

    let payload = RoutePayload::Batch { items };
    Some(RouteEnvelope {
        route_id: format!(
            "relay/bootstrap/{:x}",
            NEXT_ROUTE_ID.fetch_add(1, Ordering::Relaxed)
        ),
        from: "relay".to_owned(),
        channel,
        target: RouteTarget::Peer(target_peer_id),
        ttl: 1,
        hops: 0,
        issued_at_millis: now_millis(),
        reply_to: None,
        content_hash: stable_content_hash(&payload),
        seen_by: vec!["relay".to_owned()],
        payload,
    })
}

async fn disconnect_client(
    state: Arc<Mutex<RelayState>>,
    metrics: Arc<RelayMetrics>,
    client_id: u64,
) -> Result<()> {
    let (offline_route, recipients) = {
        let mut state = state.lock().await;
        let Some(handle) = state.clients.remove(&client_id) else {
            return Ok(());
        };
        metrics
            .client_count
            .store(state.clients.len(), Ordering::Relaxed);
        let Some(presence) = handle.presence else {
            return Ok(());
        };

        state.peer_index.remove(&presence.peer.peer_id);
        metrics
            .peer_count
            .store(state.peer_index.len(), Ordering::Relaxed);

        let mut offline_peer = presence.peer.clone();
        offline_peer
            .metadata
            .insert("state".to_owned(), "offline".to_owned());
        let payload = RoutePayload::Presence { peer: offline_peer };
        let offline_route = RouteEnvelope {
            route_id: format!("relay/{:x}", NEXT_ROUTE_ID.fetch_add(1, Ordering::Relaxed)),
            from: "relay".to_owned(),
            channel: presence.route.channel.clone(),
            target: RouteTarget::Broadcast,
            ttl: 1,
            hops: 0,
            issued_at_millis: now_millis(),
            reply_to: None,
            content_hash: stable_content_hash(&payload),
            seen_by: vec!["relay".to_owned()],
            payload,
        };
        let recipients = collect_route_recipients(&state, client_id, &offline_route);
        (offline_route, recipients)
    };

    let payload = serde_json::to_string(&offline_route)?;
    for recipient in recipients {
        let _ = recipient.send(RelayMessage::Text(payload.clone()));
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
        state
            .clients
            .get(&client_id)
            .map(|handle| handle.sender.clone())
    };
    if let Some(sender) = sender {
        let _ = sender.send(RelayMessage::Text(payload));
    }
    Ok(())
}

fn recommendation_score(peer: &PeerPresence) -> u16 {
    let capability_bonus = peer.capabilities.len().min(8) as u16 * 10;
    let topic_bonus = peer.topics.len().min(8) as u16 * 5;
    50 + capability_bonus + topic_bonus
}

fn dedupe_key(route: &RouteEnvelope) -> Option<String> {
    route.content_hash.as_ref().map(|content_hash| {
        format!(
            "{}:{}:{content_hash}",
            route.from,
            route.reply_to.as_deref().unwrap_or_default()
        )
    })
}

fn trim_seen_cache(cache: &mut BTreeMap<String, u64>, max: usize) {
    while cache.len() > max {
        let Some(oldest) = cache.keys().next().cloned() else {
            break;
        };
        cache.remove(&oldest);
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
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
