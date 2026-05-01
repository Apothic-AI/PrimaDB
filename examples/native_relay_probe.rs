#[cfg(not(feature = "native-websocket"))]
fn main() {
    eprintln!(
        "Run with: cargo run --features native-websocket --example native_relay_probe -- --relay ws://127.0.0.1:9010 --action status"
    );
}

#[cfg(feature = "native-websocket")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use primadb::{Primadb, RelayClientConfig};
    use serde_json::json;
    use std::env;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let options = Options::parse(env::args().skip(1).collect())?;
    let db = Primadb::with_replica_id(options.replica.clone());
    let mut relay = db
        .connect_relay(RelayClientConfig {
            url: options.relay.clone(),
            retry_interval_ms: 1_500,
            session_auth: Default::default(),
        })
        .await?;
    let notes = db.root("boards").field(&options.board).field("notes");

    wait_for_known_peers(&relay, options.expected_peers, options.timeout_ms).await?;

    match options.action.as_str() {
        "status" => {}
        "write-note" => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            notes.set(json!({
                "title": options.title.clone().unwrap_or_else(|| format!("relay note {}", options.replica)),
                "body": options.body.clone().unwrap_or_else(|| "native relay probe".to_owned()),
                "done": false,
                "archived": false,
                "created_at": now,
                "updated_at": now,
            }))?;
            relay.flush_pending().await?;
        }
        "wait-note" => {
            let title = options
                .title
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--title is required for wait-note"))?;
            wait_for_note(&notes, &title, options.timeout_ms).await?;
        }
        "wait-remote-note" => {
            let title = options
                .title
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--title is required for wait-remote-note"))?;
            wait_for_remote_note(&relay, &options.board, &title, options.timeout_ms).await?;
        }
        other => return Err(anyhow::anyhow!("unsupported action `{other}`")),
    }

    let output = json!({
        "relay": options.relay,
        "board": options.board,
        "replica": options.replica,
        "action": options.action,
        "connected": relay.is_connected(),
        "knownPeerCount": relay.known_peer_count(),
        "inflightCount": relay.inflight_count(),
        "title": options.title,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);

    if options.hold_ms > 0 {
        tokio::time::sleep(Duration::from_millis(options.hold_ms)).await;
    }

    relay.close();
    Ok(())
}

#[cfg(feature = "native-websocket")]
struct Options {
    relay: String,
    board: String,
    replica: String,
    action: String,
    title: Option<String>,
    body: Option<String>,
    timeout_ms: u64,
    hold_ms: u64,
    expected_peers: usize,
}

#[cfg(feature = "native-websocket")]
impl Options {
    fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut relay = "ws://127.0.0.1:9010".to_owned();
        let mut board = "shared".to_owned();
        let mut replica = format!("native-relay-{}", std::process::id());
        let mut action = "status".to_owned();
        let mut title = None;
        let mut body = None;
        let mut timeout_ms = 20_000;
        let mut hold_ms = 1_000;
        let mut expected_peers = 1usize;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--relay" => {
                    relay = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --relay"))?
                }
                "--board" => {
                    board = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --board"))?
                }
                "--replica" => {
                    replica = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --replica"))?
                }
                "--action" => {
                    action = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --action"))?
                }
                "--title" => {
                    title = Some(
                        iter.next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --title"))?,
                    )
                }
                "--body" => {
                    body = Some(
                        iter.next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --body"))?,
                    )
                }
                "--timeout-ms" => {
                    timeout_ms = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --timeout-ms"))?
                        .parse()?
                }
                "--hold-ms" => {
                    hold_ms = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --hold-ms"))?
                        .parse()?
                }
                "--expected-peers" => {
                    expected_peers = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --expected-peers"))?
                        .parse()?
                }
                other => return Err(anyhow::anyhow!("unknown argument `{other}`")),
            }
        }

        Ok(Self {
            relay,
            board,
            replica,
            action,
            title,
            body,
            timeout_ms,
            hold_ms,
            expected_peers,
        })
    }
}

#[cfg(feature = "native-websocket")]
async fn wait_for_known_peers(
    relay: &primadb::NativeWebSocketSync,
    expected: usize,
    timeout_ms: u64,
) -> anyhow::Result<()> {
    use std::time::{Duration, Instant};
    if expected == 0 {
        return Ok(());
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if relay.known_peer_count() >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timed out waiting for {expected} relay peer(s)"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(feature = "native-websocket")]
async fn wait_for_note(notes: &primadb::Chain, title: &str, timeout_ms: u64) -> anyhow::Result<()> {
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let matches = notes.find().where_eq("title", title)?.run()?;
        if !matches.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!("timed out waiting for note `{title}`"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(feature = "native-websocket")]
async fn wait_for_remote_note(
    relay: &primadb::NativeWebSocketSync,
    board: &str,
    title: &str,
    timeout_ms: u64,
) -> anyhow::Result<()> {
    use primadb::{QueryFilter, QuerySpec, RemotePath};
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let path = RemotePath::new("boards", vec![board.to_owned(), "notes".to_owned()]);
    let spec = QuerySpec {
        filters: vec![QueryFilter::Eq {
            path: "title".to_owned(),
            value: serde_json::Value::String(title.to_owned()),
        }],
        limit: Some(1),
        ..Default::default()
    };
    loop {
        for recommendation in relay.recommended_peers() {
            if let Ok(entries) = relay
                .remote_query(
                    recommendation.peer.peer_id.clone(),
                    path.clone(),
                    spec.clone(),
                )
                .await
            {
                if !entries.is_empty() {
                    return Ok(());
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timed out waiting for remote note `{title}`"
            ));
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}
