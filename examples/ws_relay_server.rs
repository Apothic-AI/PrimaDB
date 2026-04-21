#[cfg(not(feature = "native-websocket"))]
fn main() {
    eprintln!(
        "Run with: cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010"
    );
}

#[cfg(feature = "native-websocket")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9010".to_owned());
    let server = primadb::NativeRelayServer::bind(addr).await?;
    tokio::signal::ctrl_c().await?;
    server.close().await;
    Ok(())
}
