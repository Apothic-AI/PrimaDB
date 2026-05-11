#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use futures_util::{SinkExt, StreamExt};
#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use primadb::{
    MoqRelayClientConfig, NativeRelayServer, RelayServerConfig, Result, RouteEnvelope,
    RoutePayload, RouteTarget, Router, RouterConfig,
};
#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use std::fs;
#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use std::path::PathBuf;
#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use tokio_tungstenite::connect_async;
#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
use tokio_tungstenite::tungstenite::Message;

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
fn load_dotenv() {
    let path = PathBuf::from(".env");
    let Ok(contents) = fs::read_to_string(path) else {
        return;
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if std::env::var_os(key).is_none() {
            unsafe {
                std::env::set_var(key, value.trim_matches(&['"', '\''][..]));
            }
        }
    }
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
fn normalize_relay_url(value: &str) -> String {
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else {
        format!("https://{value}")
    }
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
fn relay_candidates() -> Vec<(String, String)> {
    if let Ok(relay) = std::env::var("MOQ_RELAY").or_else(|_| std::env::var("PRIMADB_MOQ_RELAY")) {
        let draft = std::env::var("MOQ_RELAY_DRAFT").unwrap_or_else(|_| "selected".to_owned());
        return vec![(draft, normalize_relay_url(&relay))];
    }
    let mut candidates = Vec::new();
    if let Ok(relay) = std::env::var("MOQ_DRAFT14_RELAY") {
        candidates.push(("draft_14".to_owned(), normalize_relay_url(&relay)));
    }
    if let Ok(relay) = std::env::var("MOQ_DRAFT07_RELAY") {
        candidates.push(("draft_07".to_owned(), normalize_relay_url(&relay)));
    }
    candidates
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
fn signal_route(router: &Router, label: &str) -> RouteEnvelope {
    router.wrap_signal(
        "moq-gateway-interop",
        serde_json::json!({
            "kind": "interop_probe",
            "fromLabel": label,
        }),
        RouteTarget::Broadcast,
    )
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
fn is_signal_from(route: &RouteEnvelope, label: &str) -> bool {
    matches!(
        &route.payload,
        RoutePayload::Signal { payload, .. }
            if payload.get("fromLabel").and_then(|value| value.as_str()) == Some(label)
    )
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
async fn probe_candidate(draft: &str, url: &str) -> Result<serde_json::Value> {
    let token = format!("{}-{}", now_millis(), std::process::id());
    let channel = format!("primadb-live-gateway-{token}");
    let gateway_path = format!("primadb/live/gateway/{token}/gateway");
    let moq_peer_path = format!("primadb/live/gateway/{token}/moq-peer");

    let mut gateway_moq = MoqRelayClientConfig::new(url, &gateway_path);
    gateway_moq.channel = channel.clone();
    gateway_moq.subscribe = vec![moq_peer_path.clone()];
    gateway_moq.retry_interval_ms = 500;
    let server = NativeRelayServer::bind_with_config(RelayServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        moq: Some(gateway_moq),
    })
    .await?;

    let mut moq_config = MoqRelayClientConfig::new(url, &moq_peer_path);
    moq_config.channel = channel.clone();
    moq_config.subscribe = vec![gateway_path.clone()];
    moq_config.retry_interval_ms = 500;
    let mut moq_peer = primadb::NativeMoqRouteClient::connect(moq_config).await?;

    let (socket, _) = connect_async(server.url())
        .await
        .map_err(|error| primadb::PrimadbError::Message(error.to_string()))?;
    let (mut ws_writer, mut ws_reader) = socket.split();
    let ws_router = Router::new(RouterConfig {
        peer_id: format!("ws-peer:{token}"),
        default_channel: channel.clone(),
        default_ttl: 6,
        max_seen_routes: 4096,
    });
    let moq_router = Router::new(RouterConfig {
        peer_id: format!("moq-peer:{token}"),
        default_channel: channel.clone(),
        default_ttl: 6,
        max_seen_routes: 4096,
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    let mut ws_got_moq = None;
    let mut moq_got_ws = None;
    while tokio::time::Instant::now() < deadline {
        let ws_route = signal_route(&ws_router, "ws-peer");
        ws_writer
            .send(Message::Text(serde_json::to_string(&ws_route)?.into()))
            .await
            .map_err(|error| primadb::PrimadbError::Message(error.to_string()))?;
        let _ = moq_peer.send_route(signal_route(&moq_router, "moq-peer"));

        while let Some(route) = moq_peer.try_recv_route() {
            if is_signal_from(&route, "ws-peer") {
                moq_got_ws = Some(route.route_id);
            }
        }

        tokio::select! {
            maybe_message = ws_reader.next() => {
                if let Some(Ok(Message::Text(payload))) = maybe_message
                    && let Ok(route) = serde_json::from_str::<RouteEnvelope>(&payload)
                    && is_signal_from(&route, "moq-peer")
                {
                    ws_got_moq = Some(route.route_id);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }

        if ws_got_moq.is_some() && moq_got_ws.is_some() {
            break;
        }
    }

    let _ = ws_writer.close().await;
    moq_peer.close().await;
    server.close().await;

    Ok(serde_json::json!({
        "draft": draft,
        "url": url,
        "channel": channel,
        "webSocketPeerGotMoqPeer": ws_got_moq,
        "moqPeerGotWebSocketPeer": moq_got_ws,
        "ok": ws_got_moq.is_some() && moq_got_ws.is_some(),
    }))
}

#[cfg(all(feature = "native-websocket", feature = "native-moq"))]
#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv();
    let candidates = relay_candidates();
    if candidates.is_empty() {
        println!(
            "Skipping live native gateway MoQ/WebSocket smoke: MOQ_DRAFT14_RELAY/MOQ_DRAFT07_RELAY are not set."
        );
        return Ok(());
    }
    let mut results = Vec::new();
    for (draft, url) in candidates {
        match probe_candidate(&draft, &url).await {
            Ok(result) => results.push(result),
            Err(error) => results.push(serde_json::json!({
                "draft": draft,
                "url": url,
                "ok": false,
                "error": error.to_string(),
            })),
        }
    }
    println!("{}", serde_json::to_string_pretty(&results).unwrap());
    if !results
        .iter()
        .any(|result| result.get("ok").and_then(|value| value.as_bool()) == Some(true))
    {
        return Err(primadb::PrimadbError::Message(
            "all live native gateway MoQ/WebSocket probes failed".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(not(all(feature = "native-websocket", feature = "native-moq")))]
fn main() {
    eprintln!("native_gateway_moq_ws_live_probe requires --features 'native-websocket native-moq'");
}
