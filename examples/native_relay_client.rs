#[cfg(not(feature = "native-websocket"))]
fn main() {
    eprintln!(
        "Run with: cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010"
    );
}

#[cfg(feature = "native-websocket")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use primadb::{NativeWebSocketSync, Primadb};
    use serde_json::json;
    use std::env;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let url = env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://127.0.0.1:9010".to_owned());
    let replica = env::args().nth(2).unwrap_or_else(|| {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("native-{suffix}")
    });

    let db = Primadb::with_replica_id(replica.clone());
    let mut sync = NativeWebSocketSync::connect(db.clone(), &url, Duration::from_secs(2)).await?;
    let notes = db.root("boards").field("shared").field("notes");

    let subscription = notes.subscribe()?;
    tokio::spawn(async move {
        while let Some(snapshot) = subscription.recv().await {
            if let Some(snapshot) = snapshot {
                eprintln!(
                    "replica update:\n{}",
                    serde_json::to_string_pretty(&snapshot).unwrap_or_default()
                );
            }
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    notes.set(json!({
        "title": format!("Hello from {replica}"),
        "body": "This note was written from the native relay client example.",
        "done": false,
        "archived": false,
        "created_at": now,
        "updated_at": now,
    }))?;
    sync.flush_pending().await?;

    eprintln!("connected to {url} as {replica}. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;
    sync.close();
    Ok(())
}
