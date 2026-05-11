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
