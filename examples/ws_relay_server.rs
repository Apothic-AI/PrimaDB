use futures_util::{SinkExt, StreamExt};
use primadb::{
    PeerPresence, PeerRecommendation, RouteBatchItem, RouteEnvelope, RoutePayload, RouteTarget,
    stable_content_hash,
};
use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_ROUTE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct RelayMessage {
    payload: String,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9010".to_owned())
        .parse::<SocketAddr>()?;

    let listener = TcpListener::bind(addr).await?;
    let state = Arc::new(Mutex::new(RelayState::default()));

    eprintln!("primadb DAM relay listening on ws://{addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, peer_addr, state).await {
                eprintln!("relay connection error from {peer_addr}: {error}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<Mutex<RelayState>>,
) -> anyhow::Result<()> {
    let websocket = accept_async(stream).await?;
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
    }

    eprintln!("client {client_id} connected from {peer_addr}");

    let write_task = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
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
        match message? {
            Message::Text(payload) => {
                handle_text_message(state.clone(), client_id, payload.to_string()).await?;
            }
            Message::Binary(_) => {}
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    disconnect_client(state, client_id).await?;
    write_task.abort();
    eprintln!("client {client_id} disconnected");
    Ok(())
}

async fn handle_text_message(
    state: Arc<Mutex<RelayState>>,
    client_id: u64,
    payload: String,
) -> anyhow::Result<()> {
    match serde_json::from_str::<RouteEnvelope>(&payload) {
        Ok(route) => forward_route(state, client_id, route).await,
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
                let _ = recipient.send(RelayMessage {
                    payload: payload.clone(),
                });
            }
            Ok(())
        }
    }
}

async fn forward_route(
    state: Arc<Mutex<RelayState>>,
    client_id: u64,
    mut route: RouteEnvelope,
) -> anyhow::Result<()> {
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
        }

        let recipients = collect_route_recipients(&state, client_id, &route);
        let encoded = serde_json::to_string(&route)?;
        (bootstrap, recipients, encoded)
    };

    if let Some(bootstrap) = bootstrap {
        send_to_client(&state, client_id, serde_json::to_string(&bootstrap)?).await?;
    }

    for recipient in recipients {
        let _ = recipient.send(RelayMessage {
            payload: encoded.clone(),
        });
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

async fn disconnect_client(state: Arc<Mutex<RelayState>>, client_id: u64) -> anyhow::Result<()> {
    let (offline_route, recipients) = {
        let mut state = state.lock().await;
        let Some(handle) = state.clients.remove(&client_id) else {
            return Ok(());
        };
        let Some(presence) = handle.presence else {
            return Ok(());
        };

        state.peer_index.remove(&presence.peer.peer_id);

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
        let _ = recipient.send(RelayMessage {
            payload: payload.clone(),
        });
    }
    Ok(())
}

async fn send_to_client(
    state: &Arc<Mutex<RelayState>>,
    client_id: u64,
    payload: String,
) -> anyhow::Result<()> {
    let sender = {
        let state = state.lock().await;
        state
            .clients
            .get(&client_id)
            .map(|handle| handle.sender.clone())
    };
    if let Some(sender) = sender {
        let _ = sender.send(RelayMessage { payload });
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
