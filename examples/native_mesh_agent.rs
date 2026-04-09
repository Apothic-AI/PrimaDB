#[cfg(not(feature = "native-webrtc"))]
fn main() {
    eprintln!(
        "Run with: cargo run --features native-webrtc --example native_mesh_agent -- --relay ws://127.0.0.1:9010 --room demo --action live"
    );
}

#[cfg(feature = "native-webrtc")]
use std::time::{Duration, Instant};

#[cfg(feature = "native-webrtc")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use primadb::{DurableStorageConfig, MeshConfig, Primadb};
    use serde_json::json;
    use std::env;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    let options = Options::parse(env::args().skip(1).collect())?;
    let db = Primadb::with_replica_id(options.replica.clone());

    let storage = if let Some(directory) = options.storage_dir.clone() {
        fs::create_dir_all(&directory)?;
        Some(db.open_durable_storage(DurableStorageConfig::SegmentFiles {
            directory,
            journal_retention: 8,
        })?)
    } else {
        None
    };

    if options.action == "verify-stored" {
        let titles = collect_titles(&db, &options.room)?;
        let missing: Vec<_> = options
            .expected_titles
            .iter()
            .filter(|title| !titles.iter().any(|candidate| candidate == *title))
            .cloned()
            .collect();
        anyhow::ensure!(
            missing.is_empty(),
            "stored data missing titles: {}",
            missing.join(", ")
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "action": options.action,
                "replica": options.replica,
                "storage": storage,
                "storedTitles": titles,
                "rust_native_storage_confirmed": true,
            }))?
        );
        return Ok(());
    }

    let mut mesh = db
        .connect_mesh(MeshConfig::relay(&options.room, &options.relay))
        .await?;

    wait_for_open_peers(&mesh, options.min_peers, options.timeout_ms).await?;

    if let Some(title) = options.write_title.as_deref() {
        if options.write_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(options.write_delay_ms)).await;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        db.root("boards").field(&options.room).field("notes").set(json!({
            "title": title,
            "body": options.write_body,
            "done": false,
            "archived": false,
            "created_at": now,
            "updated_at": now,
        }))?;
        mesh.flush_pending().await?;
    }

    let titles = wait_for_titles(&db, &options.room, &options.expected_titles, options.timeout_ms).await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "action": options.action,
            "replica": options.replica,
            "storage": storage,
            "relay": options.relay,
            "room": options.room,
            "peerId": mesh.peer_id(),
            "signaling": mesh.signaling_mode(),
            "relayConnected": mesh.relay_connected(),
            "openPeerCount": mesh.open_peer_count().await,
            "titles": titles,
            "rust_native_mesh_agent_confirmed": true,
        }))?
    );

    if options.hold_ms > 0 {
        tokio::time::sleep(Duration::from_millis(options.hold_ms)).await;
    }
    mesh.close().await;
    Ok(())
}

#[cfg(feature = "native-webrtc")]
struct Options {
    action: String,
    relay: String,
    room: String,
    replica: String,
    storage_dir: Option<String>,
    write_title: Option<String>,
    write_body: String,
    expected_titles: Vec<String>,
    min_peers: usize,
    timeout_ms: u64,
    hold_ms: u64,
    write_delay_ms: u64,
}

#[cfg(feature = "native-webrtc")]
impl Options {
    fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut action = "live".to_owned();
        let mut relay = "ws://127.0.0.1:9010".to_owned();
        let mut room = "primadb-mesh-agent".to_owned();
        let mut replica = format!("rust-mesh-agent-{}", std::process::id());
        let mut storage_dir = None;
        let mut write_title = None;
        let mut write_body = "rust mesh agent".to_owned();
        let mut expected_titles = Vec::new();
        let mut min_peers = 1usize;
        let mut timeout_ms = 60_000u64;
        let mut hold_ms = 2_000u64;
        let mut write_delay_ms = 0u64;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--action" => action = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --action"))?,
                "--relay" => relay = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --relay"))?,
                "--room" => room = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --room"))?,
                "--replica" => replica = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --replica"))?,
                "--storage-dir" => storage_dir = Some(iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --storage-dir"))?),
                "--write-title" => write_title = Some(iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --write-title"))?),
                "--write-body" => write_body = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --write-body"))?,
                "--expect-titles" => {
                    expected_titles = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --expect-titles"))?
                        .split(',')
                        .filter(|value| !value.trim().is_empty())
                        .map(|value| value.trim().to_owned())
                        .collect();
                }
                "--min-peers" => min_peers = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --min-peers"))?.parse()?,
                "--timeout-ms" => timeout_ms = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --timeout-ms"))?.parse()?,
                "--hold-ms" => hold_ms = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --hold-ms"))?.parse()?,
                "--write-delay-ms" => write_delay_ms = iter.next().ok_or_else(|| anyhow::anyhow!("missing value for --write-delay-ms"))?.parse()?,
                other => return Err(anyhow::anyhow!("unknown argument `{other}`")),
            }
        }

        Ok(Self {
            action,
            relay,
            room,
            replica,
            storage_dir,
            write_title,
            write_body,
            expected_titles,
            min_peers,
            timeout_ms,
            hold_ms,
            write_delay_ms,
        })
    }
}

#[cfg(feature = "native-webrtc")]
fn collect_titles(db: &primadb::Primadb, room: &str) -> anyhow::Result<Vec<String>> {
    let entries = db
        .root("boards")
        .field(room)
        .field("notes")
        .query(primadb::QuerySpec {
            order: Some(primadb::QueryOrder {
                path: "updated_at".to_owned(),
                direction: primadb::QueryDirection::Asc,
            }),
            limit: Some(1_000),
            ..Default::default()
        })?;
    Ok(entries
        .into_iter()
        .filter_map(|entry| entry.value.get("title").and_then(|value| value.as_str()).map(str::to_owned))
        .collect())
}

#[cfg(feature = "native-webrtc")]
async fn wait_for_open_peers(
    mesh: &primadb::NativeWebRtcMesh,
    expected: usize,
    timeout_ms: u64,
) -> anyhow::Result<()> {
    if expected == 0 {
        return Ok(());
    }
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if mesh.open_peer_count().await >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "timed out waiting for {expected} open mesh peer(s)"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(feature = "native-webrtc")]
async fn wait_for_titles(
    db: &primadb::Primadb,
    room: &str,
    expected_titles: &[String],
    timeout_ms: u64,
) -> anyhow::Result<Vec<String>> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let current = collect_titles(db, room)?;
        if expected_titles
            .iter()
            .all(|title| current.iter().any(|candidate| candidate == title))
        {
            return Ok(current);
        }
        if Instant::now() >= deadline {
            let missing: Vec<_> = expected_titles
                .iter()
                .filter(|title| !current.iter().any(|candidate| candidate == *title))
                .cloned()
                .collect();
            return Err(anyhow::anyhow!(
                "timed out waiting for expected mesh titles; missing: {}; current: {}",
                missing.join(", "),
                current.join(", ")
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
