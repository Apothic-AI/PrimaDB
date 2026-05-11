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
  `RouteEnvelope` exchange over Cloudflare `moq-rs` IETF backends and `NativeMoqSync` for DB sync
  over that route underlay.
- Added a native IETF MoQ backend using Cloudflare `moq-rs` (`moq-transport` and
  `moq-native-ietf`) for `MoqDraft::Draft14`/`DraftLatest`. It uses direct namespace/track
  subscriptions instead of `moq-lite` broadcast discovery and encodes one `RouteEnvelope` per MoQ
  object.
- Replaced the broken native `MoqDraft::Draft07` legacy `moq-lite` path with a renamed dependency
  on Cloudflare `moq-rs`'s `draft-ietf-moq-transport-07` branch. Native draft-07 and draft-14 now
  share the same route-mode object shape while using draft-specific transport crates.
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
  Updated the remaining Node-22-only workspace engine constraint in `apps/aimy.space-website` to
  Node `>=26` and refreshed its direct `@types/node` dependency. Node's current release page and
  `mise latest node` both report `26.1.0` as latest; `mise exec node@26.1.0 -- node -p "typeof
  WebTransport"` still reports `undefined`, so Node's WebTransport issue is not solved by the
  latest Node release alone.
- Added `@webtransport-bun/webtransport` to `primadb-node` as the Node-only WebTransport provider.
  `connectPrimadbMoq(...)` now lazily creates a provider-backed WebTransport session when Node has
  no built-in `globalThis.WebTransport`, normalizes URL objects to strings for the provider, honors
  `PRIMADB_MOQ_TLS_DISABLE_VERIFY`, and still lets callers inject or disable transports explicitly.
- Added retrying MoQ subscription loops to the JS route-mode helpers so generic relays such as
  Cloudflare can recover from subscribe-before-announce races instead of leaving a one-shot failed
  subscription.
- Updated browser, Node, and Python route-mode MoQ helpers to carry the original drained sync
  envelope JSON inside `sync_frame` route payloads and to prefer `applyOperationsJson`/
  `apply_operations_json` when present. This avoids JS numeric coercion breaking native `u64`
  deserialization.
- Added accepted peer-id aliases to route-mode MoQ sessions. `connectMeshViaMoq(...)` registers the
  WASM mesh peer id so targeted WebRTC offer/answer/ICE `RoutePayload::Signal` routes are accepted
  even though the MoQ session peer id is `moq:<replica>`.
- Verified `packages/primadb-node/scripts/smoke-moq-live.mjs` with Node v26.1.0:
  Cloudflare draft-14 Node/Node route exchange passed through the new provider-backed WebTransport
  path; Cloudflare draft-07 still returned `E_SESSION_CLOSED`.
- Verified `packages/primadb/examples/moq-sync/test-live-route.mjs` with Node latest:
  Cloudflare draft-14 browser/browser route exchange, browser/Node route exchange, and browser
  WebRTC-over-MoQ signaling passed. The browser WebRTC probe generated short-lived Cloudflare TURN
  credentials when the configured token variables were present and opened one data channel in each
  direction.
- Verified native Cloudflare MoQ after adding the draft-07 backend:
  - `cargo run --features native-moq --example native_moq_live_probe` passed for draft-14 and
    draft-07.
  - `cargo run --features "native-websocket native-moq" --example
    native_gateway_moq_ws_live_probe` passed for draft-14 and draft-07.
- Verified after the remaining fixes:
  - `cargo check --features "native-websocket native-moq" --examples`
  - `cargo test --features native-moq --lib native_moq -- --nocapture`
  - `cargo test --features native-moq --lib draft07 -- --nocapture`
  - `pnpm --dir packages/primadb run typecheck`
  - `pnpm --dir packages/primadb/examples run build`
  - `pnpm --dir packages/primadb/examples run smoke:moq-live`
  - `node packages/primadb-node/examples/moq-sync/index.mjs`
  - `node packages/primadb-node/scripts/smoke-moq-live.mjs`
  - `pnpm --dir packages/primadb run smoke:moq-mesh-signaling`
- Rebuilt and revalidated transport-facing artifacts after the Starla route app/fan-in API work:
  - `pnpm --dir packages/primadb run build`
  - `pnpm --dir packages/primadb-node run build`
  - `uv run maturin develop --release`
  - `pnpm --dir packages/primadb/examples run build`
- Re-ran non-live transport/package smokes:
  - `pnpm --dir packages/primadb run smoke`
  - `pnpm --dir packages/primadb run smoke:moq-mesh-signaling`
  - Node native core/hooks/relay-server/relay/mesh/MoQ smoke scripts
  - Python core/hooks/relay-server/relay/offline-relay/mesh/MoQ loopback smoke scripts
  - `pnpm --dir packages/primadb/examples run smoke`
  - `bash examples/test-native-relay-smoke.sh`
  - `bash examples/test-native-mesh-smoke.sh`
  - `bash scripts/smoke-native-moq-ietf-local.sh`
- Re-ran live transport probes:
  - Node Cloudflare draft-14 route exchange passed; Node Cloudflare draft-07 still returned
    `E_SESSION_CLOSED`.
  - Native Cloudflare draft-14 and draft-07 route exchange passed.
  - Native WebSocket-plus-MoQ gateway bridge passed against Cloudflare draft-14 and draft-07.
  - Browser Cloudflare draft-14 passed browser/browser route exchange, browser/Node route exchange,
    and browser WebRTC via MoQ signaling with Cloudflare ICE configuration.
  - Browser/JS Cloudflare draft-07 still failed with WebTransport/session close errors.

## Remaining

- JS draft-07 remains unsupported by the current browser/Node stack. Evidence:
  - `@moq/lite` exposes a draft-07 version constant, but its connect path negotiates draft-14/15/16/17
    and does not send a draft-07 setup to Cloudflare's draft-07 endpoint.
  - Browser/browser draft-07 reports `WebTransportError: Connection lost`.
  - Browser/Node and Node/Node draft-07 report `E_SESSION_CLOSED` or an equivalent early stream
    close.
  - Native Rust draft-07 passes because it uses the separate Cloudflare `moq-rs` draft-07 branch
    backend.
- Python MoQ remains a deterministic route-mode loopback helper. No Python Cloudflare live client
  has been added because the current Python MoQ bindings still do not expose a stable generic byte
  track API for this use case.
