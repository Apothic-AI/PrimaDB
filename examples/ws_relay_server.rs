use futures_util::{SinkExt, StreamExt};
use primadb::{PeerPresence, RouteEnvelope, RoutePayload, RouteTarget};
use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::Mutex;
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
    route: RouteEnvelope,
) -> anyhow::Result<()> {
    let encoded = serde_json::to_string(&route)?;
    let mut bootstrap_routes = Vec::new();
    let recipients = {
        let mut state = state.lock().await;

        if let RoutePayload::Presence { peer } = &route.payload {
            let existing = state
                .clients
                .iter()
                .filter_map(|(candidate_id, handle)| {
                    if *candidate_id == client_id {
                        None
                    } else {
                        handle.presence.as_ref().map(|presence| presence.route.clone())
                    }
                })
                .collect::<Vec<_>>();
            bootstrap_routes = existing;

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

        collect_route_recipients(&state, client_id, &route)
    };

    for bootstrap in bootstrap_routes {
        if let Ok(payload) = serde_json::to_string(&bootstrap) {
            let _ = send_to_client(&state, client_id, payload).await;
        }
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
                    state.clients.get(client_id).map(|handle| handle.sender.clone())
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
                if presence.peer.topics.iter().any(|candidate| candidate == topic)
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
        let offline_route = RouteEnvelope {
            route_id: format!(
                "relay/{:x}",
                NEXT_ROUTE_ID.fetch_add(1, Ordering::Relaxed)
            ),
            from: "relay".to_owned(),
            channel: presence.route.channel.clone(),
            target: RouteTarget::Broadcast,
            ttl: 1,
            hops: 0,
            issued_at_millis: now_millis(),
            payload: RoutePayload::Presence { peer: offline_peer },
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
        state.clients.get(&client_id).map(|handle| handle.sender.clone())
    };
    if let Some(sender) = sender {
        let _ = sender.send(RelayMessage { payload });
    }
    Ok(())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
