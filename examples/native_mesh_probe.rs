#[cfg(not(feature = "native-webrtc"))]
fn main() {
    eprintln!(
        "Run with: cargo run --features native-webrtc --example native_mesh_probe -- --relay ws://127.0.0.1:9010 --room demo --action status"
    );
}

#[cfg(feature = "native-webrtc")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use primadb::{MeshConfig, Primadb};
    use serde_json::json;
    use std::env;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let options = Options::parse(env::args().skip(1).collect())?;
    let db = Primadb::with_replica_id(options.replica.clone());
    let mut mesh_config = MeshConfig::relay(&options.room, &options.relay);
    mesh_config.ice_servers = if options.ice_servers.is_empty() {
        default_example_ice_servers()
    } else {
        options.ice_servers.clone()
    };
    let mut mesh = db.connect_mesh(mesh_config).await?;
    let notes = db.root("boards").field(&options.room).field("notes");

    wait_for_open_peers(&mesh, options.expected_peers, options.timeout_ms).await?;

    match options.action.as_str() {
        "status" => {}
        "write-note" => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            notes.set(json!({
                "title": options.title.clone().unwrap_or_else(|| format!("native note {}", options.replica)),
                "body": options.body.clone().unwrap_or_else(|| "native mesh probe".to_owned()),
                "done": false,
                "archived": false,
                "created_at": now,
                "updated_at": now,
            }))?;
            mesh.flush_pending().await?;
        }
        "wait-note" => {
            let title = options
                .title
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--title is required for wait-note"))?;
            wait_for_note(&notes, &title, options.timeout_ms).await?;
        }
        other => return Err(anyhow::anyhow!("unsupported action `{other}`")),
    }

    let output = json!({
        "relay": options.relay,
        "room": options.room,
        "replica": options.replica,
        "action": options.action,
        "peerId": mesh.peer_id(),
        "signaling": mesh.signaling_mode(),
        "relayConnected": mesh.relay_connected(),
        "peerCount": mesh.peer_count().await,
        "openPeerCount": mesh.open_peer_count().await,
        "inflightCount": mesh.inflight_count().await,
        "title": options.title,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);

    if options.hold_ms > 0 {
        tokio::time::sleep(Duration::from_millis(options.hold_ms)).await;
    }

    mesh.close().await;
    Ok(())
}

#[cfg(feature = "native-webrtc")]
struct Options {
    relay: String,
    room: String,
    replica: String,
    ice_servers: Vec<primadb::IceServerConfig>,
    action: String,
    title: Option<String>,
    body: Option<String>,
    timeout_ms: u64,
    hold_ms: u64,
    expected_peers: usize,
}

#[cfg(feature = "native-webrtc")]
impl Options {
    fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut relay = "ws://127.0.0.1:9010".to_owned();
        let mut room = "primadb-native-mesh".to_owned();
        let mut replica = format!("native-mesh-{}", std::process::id());
        let mut ice_servers = Vec::new();
        let mut action = "status".to_owned();
        let mut title = None;
        let mut body = None;
        let mut timeout_ms = 30_000;
        let mut hold_ms = 1_500;
        let mut expected_peers = 1usize;

        let mut iter = args.into_iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--relay" => {
                    relay = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --relay"))?
                }
                "--room" => {
                    room = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --room"))?
                }
                "--replica" => {
                    replica = iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --replica"))?
                }
                "--ice-server" => ice_servers.push(parse_ice_server_spec(
                    &iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("missing value for --ice-server"))?,
                )?),
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
            room,
            replica,
            ice_servers,
            action,
            title,
            body,
            timeout_ms,
            hold_ms,
            expected_peers,
        })
    }
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
async fn wait_for_open_peers(
    mesh: &primadb::NativeWebRtcMesh,
    expected: usize,
    timeout_ms: u64,
) -> anyhow::Result<()> {
    use std::time::{Duration, Instant};
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
