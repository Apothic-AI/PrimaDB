# Examples

- `browser-notes/`: Browser-only local-first task board using Primadb's WASM build, IndexedDB persistence, query filters, and cross-tab sync over `BroadcastChannel`.
- `browser-relay-notes/`: Browser task board using Primadb's WASM build, automatic IndexedDB persistence, and a real WebSocket relay server.
- `ws_relay_server.rs`: Minimal Rust WebSocket relay for browser examples. Run with `cargo run --example ws_relay_server`.
- `native_relay_client.rs`: Native client using Primadb's feature-gated `NativeWebSocketSync` adapter. Run with `cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010`.
- `crypto_foundation.rs`: Feature-gated identity/signing/encryption demo. Run with `cargo run --features crypto --example crypto_foundation`.
