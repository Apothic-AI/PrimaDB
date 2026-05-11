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

## In Progress

- Route-mode MoQ profile and native/browser/Node route underlay adapters.
