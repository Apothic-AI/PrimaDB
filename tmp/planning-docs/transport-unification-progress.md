# Transport Unification Progress

## Completed

- Reviewed current router overlay types in `src/router.rs`.
- Reviewed current sync envelope and remote pull/watch types in `src/sync.rs`.
- Reviewed current network config in `src/net.rs`.
- Confirmed current MoQ work is SDK-local sync-envelope transport rather than route-overlay
  transport.
- Drafted the transport unification plan.
- Refined the plan to distinguish generic MoQ relay/services, including Cloudflare MoQ, from
  PrimaDB-aware full-node gateways that bridge MoQ with WebSocket/WebRTC route traffic.
- Reviewed local `.env` and incorporated the available Cloudflare MoQ relay, STUN/TURN, and SFU
  variables into the sprint plan without recording secret values.
- Added a transport-neutral `RouteRelayCore` plus `InMemoryRouteHub`/`InMemoryRouteSession`
  contract harness for `RouteEnvelope` broadcast, peer delivery, topic delivery, duplicate
  suppression, presence bootstrap, and disconnect/offline presence behavior.
- Refactored the native WebSocket relay server to use `RouteRelayCore` instead of private
  duplicated route-forwarding, presence, peer-index, bootstrap, and dedupe state.
- Verified `cargo test transport --lib` and `cargo test --features native-websocket --lib`.
- Added tagged relay endpoint config plus `MoqRelayClientConfig`/`MoqDraft` for route-mode MoQ
  relay endpoints.
- Updated browser, Node, and Python MoQ helpers to emit `primadb.route.v1` frames carrying
  `RouteEnvelope` sync/presence/peer-exchange payloads over the configured MoQ path/track.
- Added optional native Rust `native-moq` support with `NativeMoqRouteClient` for low-level
  `RouteEnvelope` exchange over moq-native/moq-lite and `NativeMoqSync` for DB sync over that route
  underlay.
- Added a native IETF MoQ backend using Cloudflare `moq-rs` (`moq-transport` and
  `moq-native-ietf`) for `MoqDraft::Draft14`/`DraftLatest`. It uses direct namespace/track
  subscriptions instead of `moq-lite` broadcast discovery and encodes one `RouteEnvelope` per MoQ
  object. The old `moq-lite` backend remains as the `Draft07`/legacy path while draft-07 branch
  compatibility is evaluated separately.
- Added `MoqRelayClientConfig.tlsDisableVerify` plus `PRIMADB_MOQ_TLS_DISABLE_VERIFY` support for
  local native IETF MoQ smoke testing with self-signed certificates.
- Added `scripts/smoke-native-moq-ietf-local.sh`, which starts a local Cloudflare `moq-rs`
  `moq-relay-ietf` instance and runs the native/native route probe against it.
- Added an optional MoQ uplink on the native WebSocket relay server so a gateway can forward
  WebSocket sessions and a route-mode MoQ session through the same `RouteRelayCore`.
- Added native WebRTC mesh signaling over route-mode MoQ when `MeshConfig.relayEndpoint` is a MoQ
  endpoint and the `native-moq` feature is enabled. It reuses existing `RoutePayload::Signal`
  messages rather than defining a separate signaling protocol.
- Documented the old MoQ sync helper behavior as superseded by route-mode MoQ helper frames.
- Verified `pnpm run typecheck` for `packages/primadb`, `node --check packages/primadb-node/moq.js`,
  `python -m py_compile packages/primadb-python/python/primadb/moq.py`,
  `cargo test --features native-moq --lib`, and
  `cargo check --features "crypto native-websocket native-webrtc native-moq scripting"`.
- Added browser WASM external mesh signaling support:
  `connectMeshWithExternalSignaling(...)`, `WebRtcMesh.acceptSignalingRoute(...)`, and
  `WebRtcMesh.announceSignalingPresence()`.
- Added `connectMeshViaMoq(...)` and `connectMeshViaMoqSession(...)` to `packages/primadb/moq.ts`.
  The wrapper creates/uses a route-mode `PrimadbMoqSession`, forwards outgoing WASM mesh signaling
  routes into MoQ, and injects incoming MoQ routes into the WASM mesh.
- Added deterministic MoQ-backed mesh signaling smoke coverage in
  `packages/primadb/scripts/smoke-moq-mesh-signaling.mjs`.
- Added opt-in live MoQ route probes:
  - browser/browser and browser/Node: `packages/primadb/examples/moq-sync/test-live-route.mjs`
  - Node/Node: `packages/primadb-node/scripts/smoke-moq-live.mjs`
  - native/native: `examples/native_moq_live_probe.rs`
  - WebSocket peer plus MoQ peer through native gateway:
    `examples/native_gateway_moq_ws_live_probe.rs`
- Updated MoQ and mesh docs/examples to cover route-mode MoQ, MoQ-backed WebRTC signaling, live
  smoke commands, and current Cloudflare caveats.
- Verified:
  - `cargo check --features "native-websocket native-moq" --examples`
  - `scripts/smoke-native-moq-ietf-local.sh`
  - `cargo run --features native-moq --example native_moq_live_probe`
  - `cargo run --features "native-websocket native-moq" --example native_gateway_moq_ws_live_probe`
  - `pnpm --dir packages/primadb run typecheck`
  - `pnpm --dir packages/primadb run build`
  - `pnpm --dir packages/primadb run smoke:moq-mesh-signaling`
  - `node --check` for the new JS smoke scripts
- Updated monorepo `mise.toml` to Node `26.1.0` and root package metadata to require Node `>=26`.
  `mise exec node@26.1.0 -- node -p "typeof WebTransport"` still reports `undefined`, so Node's
  WebTransport issue is not solved by the latest Node release alone.

## Remaining

- Live Cloudflare browser/Node interop probes are implemented and were run against the available
  environment, but the configured public endpoints did not pass for those JS stacks:
  - Browser/browser via `MOQ_DRAFT14_RELAY=draft-14.cloudflare.mediaoverquic.com`: timed out waiting
    for route exchange.
  - Browser/browser via `MOQ_DRAFT07_RELAY=draft-07.cloudflare.mediaoverquic.com`: browser
    WebTransport reported connection lost.
  - Browser/browser via `https://draft-07.cloudflare.mediaoverquic.com/anon`: browser WebTransport
    reported connection lost.
  - Browser/browser via `https://interop-relay.cloudflare.mediaoverquic.com:443/anon`: browser
    WebTransport reported connection lost.
  - Node/Node via both configured endpoints: timed out during MoQ connect. Node v26.1.0 has
    WebSocket but no built-in WebTransport; Cloudflare did not complete WebSocket fallback.
- Native Cloudflare draft-14 now passes:
  - Native/native via `MOQ_DRAFT14_RELAY=draft-14.cloudflare.mediaoverquic.com`: bidirectional
    route exchange passed.
  - WebSocket peer plus MoQ peer through the native gateway via
    `MOQ_DRAFT14_RELAY=draft-14.cloudflare.mediaoverquic.com`: bidirectional route exchange passed.
- Native Cloudflare draft-07 still does not pass through PrimaDB's integrated path. The current
  `MoqDraft::Draft07` backend is the legacy `moq-lite` implementation; true draft-07 validation
  requires either a separate build against Cloudflare `moq-rs`'s `draft-ietf-moq-transport-07`
  branch or a second integrated draft-07 backend because that branch has a different role/session
  API.
- Browser WebRTC MoQ signaling has deterministic route injection coverage, but browser tab-to-tab
  live WebRTC over Cloudflare MoQ/STUN/TURN still needs a passing JS/browser MoQ route exchange
  before it can be validated end to end.
