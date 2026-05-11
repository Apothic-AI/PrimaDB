#[cfg(feature = "native-moq")]
use primadb::{
    MoqRelayClientConfig, Result, RouteEnvelope, RoutePayload, RouteTarget, Router, RouterConfig,
};
#[cfg(feature = "native-moq")]
use std::fs;
#[cfg(feature = "native-moq")]
use std::path::PathBuf;
#[cfg(feature = "native-moq")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "native-moq")]
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

#[cfg(feature = "native-moq")]
fn normalize_relay_url(value: &str) -> String {
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else {
        format!("https://{value}")
    }
}

#[cfg(feature = "native-moq")]
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

#[cfg(feature = "native-moq")]
fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(feature = "native-moq")]
fn route_from(router: &Router, draft: &str, from_label: &str) -> RouteEnvelope {
    router.wrap_signal(
        "moq-live-interop",
        serde_json::json!({
            "kind": "interop_probe",
            "draft": draft,
            "fromLabel": from_label,
        }),
        RouteTarget::Broadcast,
    )
}

#[cfg(feature = "native-moq")]
async fn probe_candidate(draft: &str, url: &str) -> Result<serde_json::Value> {
    let token = format!("{}-{}", now_millis(), std::process::id(),);
    let channel = format!("primadb-live-native-{token}");
    let path_a = format!("primadb/live/native/{token}/a");
    let path_b = format!("primadb/live/native/{token}/b");
    let mut config_a = MoqRelayClientConfig::new(url, &path_a);
    config_a.channel = channel.clone();
    config_a.subscribe = vec![path_b.clone()];
    config_a.retry_interval_ms = 500;
    let mut config_b = MoqRelayClientConfig::new(url, &path_b);
    config_b.channel = channel.clone();
    config_b.subscribe = vec![path_a.clone()];
    config_b.retry_interval_ms = 500;

    let mut client_a = primadb::NativeMoqRouteClient::connect(config_a).await?;
    let mut client_b = primadb::NativeMoqRouteClient::connect(config_b).await?;
    let router_a = Router::new(RouterConfig {
        peer_id: format!("native-a:{token}"),
        default_channel: channel.clone(),
        default_ttl: 6,
        max_seen_routes: 4096,
    });
    let router_b = Router::new(RouterConfig {
        peer_id: format!("native-b:{token}"),
        default_channel: channel.clone(),
        default_ttl: 6,
        max_seen_routes: 4096,
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut got_a = None;
    let mut got_b = None;
    while tokio::time::Instant::now() < deadline {
        let _ = client_a.send_route(route_from(&router_a, draft, "native-a"));
        let _ = client_b.send_route(route_from(&router_b, draft, "native-b"));
        while let Some(route) = client_a.try_recv_route() {
            if matches!(route.payload, RoutePayload::Signal { ref payload, .. } if payload.get("fromLabel").and_then(|value| value.as_str()) == Some("native-b"))
            {
                got_a = Some(route.route_id);
            }
        }
        while let Some(route) = client_b.try_recv_route() {
            if matches!(route.payload, RoutePayload::Signal { ref payload, .. } if payload.get("fromLabel").and_then(|value| value.as_str()) == Some("native-a"))
            {
                got_b = Some(route.route_id);
            }
        }
        if got_a.is_some() && got_b.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    client_a.close().await;
    client_b.close().await;

    Ok(serde_json::json!({
        "draft": draft,
        "url": url,
        "channel": channel,
        "nativeAGotNativeB": got_a,
        "nativeBGotNativeA": got_b,
        "ok": got_a.is_some() && got_b.is_some(),
    }))
}

#[cfg(feature = "native-moq")]
#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv();
    let candidates = relay_candidates();
    if candidates.is_empty() {
        println!(
            "Skipping live native MoQ smoke: MOQ_DRAFT14_RELAY/MOQ_DRAFT07_RELAY are not set."
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
            "all live native MoQ probes failed".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(not(feature = "native-moq"))]
fn main() {
    eprintln!("native_moq_live_probe requires --features native-moq");
}
