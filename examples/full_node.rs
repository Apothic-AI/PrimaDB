#[cfg(not(feature = "native-webrtc"))]
fn main() {
    eprintln!(
        "Run with: cargo run --features native-webrtc --example full_node -- --relay-bind 127.0.0.1:9010 --room demo"
    );
}

#[cfg(feature = "native-webrtc")]
use std::path::PathBuf;

#[cfg(feature = "native-webrtc")]
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "native-webrtc")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use primadb::{
        DurableStorageConfig, MeshConfig, NativeRelayServer, Primadb, QueryDirection, QueryOrder,
        QuerySpec,
    };
    use serde_json::json;

    let options = Options::parse(std::env::args().skip(1).collect())?;
    let relay = NativeRelayServer::bind(options.relay_bind.to_string()).await?;
    let relay_bind = relay.bind_addr();
    let relay_url = options.relay_url.clone().unwrap_or_else(|| relay.url());

    let db = Primadb::with_replica_id(options.replica.clone());
    std::fs::create_dir_all(&options.storage_dir)?;
    let storage = db.open_durable_storage(DurableStorageConfig::SegmentFiles {
        directory: options.storage_dir.clone(),
        journal_retention: 8,
    })?;

    let mut mesh_config = MeshConfig::relay(&options.room, &relay_url);
    mesh_config.ice_servers = if options.ice_servers.is_empty() {
        default_example_ice_servers()
    } else {
        options.ice_servers.clone()
    };
    let mut mesh = db.connect_mesh(mesh_config).await?;
    let notes = db.root("full_nodes").field(&options.room).field("notes");

    if let Some(message) = &options.message {
        let now = now_millis();
        notes.set(json!({
            "author": &options.replica,
            "title": options.title.clone().unwrap_or_else(|| format!("{} {}", options.replica, now)),
            "body": message,
            "role": "full-node",
            "updated_at": now,
        }))?;
        mesh.flush_pending().await?;
    }

    let mut previous = String::new();
    let deadline = options
        .duration_ms
        .map(|value| Instant::now() + Duration::from_millis(value));

    loop {
        let snapshot = json!({
            "role": "full-node",
            "replica": &options.replica,
            "room": &options.room,
            "relayBind": relay_bind.to_string(),
            "relayUrl": &relay_url,
            "relayClients": relay.client_count(),
            "relayPeers": relay.peer_count(),
            "storage": {
                "backend": &storage.backend,
                "incremental": storage.incremental,
                "loadedExisting": storage.loaded_existing,
                "autoPersist": storage.auto_persist,
                "directory": &options.storage_dir,
            },
            "peerId": mesh.peer_id(),
            "signaling": mesh.signaling_mode(),
            "relayConnected": mesh.relay_connected(),
            "peers": mesh.peer_count().await,
            "openPeers": mesh.open_peer_count().await,
            "inflight": mesh.inflight_count().await,
            "notes": notes.query(QuerySpec {
                order: Some(QueryOrder {
                    path: "updated_at".to_owned(),
                    direction: QueryDirection::Desc,
                }),
                limit: Some(5),
                ..Default::default()
            })?,
        });
        let encoded = serde_json::to_string_pretty(&snapshot)?;
        if encoded != previous {
            println!("{encoded}");
            previous = encoded;
        }

        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(1_000)) => {}
        }
    }

    mesh.close().await;
    relay.close().await;
    Ok(())
}

#[cfg(feature = "native-webrtc")]
struct Options {
    relay_bind: std::net::SocketAddr,
    relay_url: Option<String>,
    room: String,
    replica: String,
    storage_dir: String,
    ice_servers: Vec<primadb::IceServerConfig>,
    title: Option<String>,
    message: Option<String>,
    duration_ms: Option<u64>,
}

#[cfg(feature = "native-webrtc")]
impl Options {
    fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut relay_bind = "127.0.0.1:9010".parse()?;
        let mut relay_url = None;
        let mut room = "primadb-full-node".to_owned();
        let mut replica = format!("full-node-{}", std::process::id());
        let mut storage_dir = default_storage_dir(&replica);
        let mut ice_servers = Vec::new();
        let mut title = None;
        let mut message = None;
        let mut duration_ms = None;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--relay-bind" => {
                    relay_bind = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --relay-bind"))?
                        .parse()?
                }
                "--room" => {
                    room = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --room"))?
                }
                "--relay-url" => {
                    relay_url = Some(
                        iter.next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --relay-url"))?,
                    )
                }
                "--replica" => {
                    replica = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --replica"))?;
                    storage_dir = default_storage_dir(&replica);
                }
                "--storage-dir" => {
                    storage_dir = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --storage-dir"))?
                }
                "--ice-server" => {
                    ice_servers.push(parse_ice_server_spec(
                        &iter
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --ice-server"))?,
                    )?);
                }
                "--title" => {
                    title = Some(
                        iter.next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --title"))?,
                    )
                }
                "--message" => {
                    message = Some(
                        iter.next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --message"))?,
                    )
                }
                "--duration-ms" => {
                    duration_ms = Some(
                        iter.next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --duration-ms"))?
                            .parse()?,
                    )
                }
                other => return Err(anyhow::anyhow!("unknown argument `{other}`")),
            }
        }

        Ok(Self {
            relay_bind,
            relay_url,
            room,
            replica,
            storage_dir,
            ice_servers,
            title,
            message,
            duration_ms,
        })
    }
}

#[cfg(feature = "native-webrtc")]
fn default_storage_dir(replica: &str) -> String {
    PathBuf::from("examples")
        .join(".data")
        .join("full-node")
        .join(replica)
        .display()
        .to_string()
}

#[cfg(feature = "native-webrtc")]
fn parse_ice_server_spec(spec: &str) -> anyhow::Result<primadb::IceServerConfig> {
    let trimmed = spec.trim();
    if trimmed.starts_with('{') {
        return Ok(serde_json::from_str(trimmed)?);
    }
    if trimmed.starts_with("stun:") || trimmed.starts_with("turn:") || trimmed.starts_with("turns:")
    {
        return Ok(primadb::IceServerConfig {
            urls: primadb::IceServerUrls::One(trimmed.to_owned()),
            username: None,
            credential: None,
        });
    }
    Err(anyhow::anyhow!(
        "invalid --ice-server value `{trimmed}`; use a STUN/TURN URL or JSON object"
    ))
}

#[cfg(feature = "native-webrtc")]
fn default_example_ice_servers() -> Vec<primadb::IceServerConfig> {
    vec![primadb::IceServerConfig {
        urls: primadb::IceServerUrls::One("stun:stun.cloudflare.com:3478".to_owned()),
        username: None,
        credential: None,
    }]
}

#[cfg(feature = "native-webrtc")]
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
