use futures_util::{SinkExt, StreamExt};
use std::env;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct RelayMessage {
    sender: u64,
    payload: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9010".to_owned())
        .parse::<SocketAddr>()?;

    let listener = TcpListener::bind(addr).await?;
    let (tx, _) = broadcast::channel::<RelayMessage>(512);

    eprintln!("primadb relay listening on ws://{addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, peer_addr, tx).await {
                eprintln!("relay connection error from {peer_addr}: {error}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    tx: broadcast::Sender<RelayMessage>,
) -> anyhow::Result<()> {
    let websocket = accept_async(stream).await?;
    let (mut writer, mut reader) = websocket.split();
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut rx = tx.subscribe();

    eprintln!("client {client_id} connected from {peer_addr}");

    let write_task = tokio::spawn(async move {
        while let Ok(message) = rx.recv().await {
            if message.sender == client_id {
                continue;
            }
            if writer.send(Message::Text(message.payload.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(message) = reader.next().await {
        match message? {
            Message::Text(payload) => {
                let _ = tx.send(RelayMessage {
                    sender: client_id,
                    payload: payload.to_string(),
                });
            }
            Message::Binary(_) => {}
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    write_task.abort();
    eprintln!("client {client_id} disconnected");
    Ok(())
}
