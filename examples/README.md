# Examples

- `browser-notes/`: Browser-only local-first task board using Primadb's WASM build, IndexedDB persistence, query filters, and cross-tab sync over `BroadcastChannel`.
- `browser-relay-notes/`: Browser task board using Primadb's WASM build, automatic IndexedDB persistence, and a real WebSocket relay server.
- `browser-mesh-notes/`: Browser-to-browser notes board using Primadb's `WebRtcMesh` helper with peer discovery over `BroadcastChannel` and direct sync over WebRTC data channels.
- `browser-gun-notes/`: Gun-compatible browser app using `js/primadb-gun.js`, SEA-style users, and the relay-backed DAM path.
- `ws_relay_server.rs`: Rust DAM relay with peer presence, targeted routing, and signaling for browser examples. Run with `cargo run --example ws_relay_server`.
- `native_relay_client.rs`: Native client using Primadb's feature-gated `NativeWebSocketSync` adapter. Run with `cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010`.
- `crypto_foundation.rs`: Feature-gated identity/signing/encryption demo. Run with `cargo run --features crypto --example crypto_foundation`.
- `authenticated_sync.rs`: Feature-gated signed/encrypted sync policy demo. Run with `cargo run --features crypto --example authenticated_sync`.
- `radisk_storage.rs`: RADisk-style append-log storage demo. Run with `cargo run --example radisk_storage`.
- `gun_compat.rs`: Gun-compatible API and data-marker demo. Run with `cargo run --example gun_compat`.
