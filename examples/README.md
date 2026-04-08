# Examples

- `browser-notes/`: Browser-only local-first task board using Primadb's WASM build, IndexedDB persistence, query filters, and cross-tab sync over `BroadcastChannel`.
- `browser-relay-notes/`: Browser task board using Primadb's WASM build, automatic IndexedDB persistence, peer recommendations, and remote `get/query/lex/snapshot` over the relay.
- `browser-mesh-notes/`: Browser-to-browser notes board using Primadb's `WebRtcMesh` helper with peer discovery over `BroadcastChannel` and direct sync over WebRTC data channels.
- `browser-gun-notes/`: Gun-compatible browser app using `js/primadb-gun.js`, SEA-style users, and the relay-backed DAM path.
- `browser-threaded-query/`: Opt-in `wasm-threads` browser demo with COOP/COEP serving, `initThreadPool(...)`, and a Rayon-backed query workload.
- `ws_relay_server.rs`: Rust DAM relay with peer presence, peer exchange, targeted routing, batch bootstrap, and relay-friendly dedupe hints for browser examples. Run with `cargo run --example ws_relay_server`.
- `native_relay_client.rs`: Native client using Primadb's feature-gated `NativeWebSocketSync` adapter. Run with `cargo run --features native-websocket --example native_relay_client -- ws://127.0.0.1:9010`.
- `native_parallel_query.rs`: Native Rayon verification example. Run with `cargo run --example native_parallel_query`.
- `crypto_foundation.rs`: Feature-gated identity/signing/encryption demo. Run with `cargo run --features crypto --example crypto_foundation`.
- `authenticated_sync.rs`: Feature-gated signed/encrypted sync policy demo. Run with `cargo run --features crypto --example authenticated_sync`.
- `radisk_storage.rs`: RADisk-style append-log storage demo. Run with `cargo run --example radisk_storage`.
- `gun_compat.rs`: Gun-compatible API and data-marker demo. Run with `cargo run --example gun_compat`.
