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
- Added an optional MoQ uplink on the native WebSocket relay server so a gateway can forward
  WebSocket sessions and a route-mode MoQ session through the same `RouteRelayCore`.
- Documented the old MoQ sync helper behavior as superseded by route-mode MoQ helper frames.
- Verified `pnpm run typecheck` for `packages/primadb`, `node --check packages/primadb-node/moq.js`,
  `python -m py_compile packages/primadb-python/python/primadb/moq.py`,
  `cargo test --features native-moq --lib`, and
  `cargo check --features "crypto native-websocket native-webrtc native-moq scripting"`.

## Remaining

- Direct MoQ-backed WebRTC signaling is still pending as a dedicated integration layer on top of
  route-mode MoQ. The route payload shape for signaling already exists; the WebRTC mesh connector
  still needs a MoQ signaling adapter beside the current WebSocket relay adapter.
