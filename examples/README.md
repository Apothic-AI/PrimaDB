# Examples

- `browser-notes/`: Browser-only local-first task board using Primadb's WASM build, IndexedDB persistence, query filters, and cross-tab sync over `BroadcastChannel`.
- `browser-relay-notes/`: Browser task board using Primadb's WASM build, automatic IndexedDB persistence, and a real WebSocket relay server.
- `ws_relay_server.rs`: Minimal Rust WebSocket relay for browser examples. Run with `cargo run --example ws_relay_server`.
