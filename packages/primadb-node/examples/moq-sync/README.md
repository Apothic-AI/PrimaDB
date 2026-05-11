# MoQ Sync

This example uses the `primadb-node/moq` helper to publish PrimaDB `RouteEnvelope` traffic over a
MoQ track. Sync frames are carried as route payloads, so the helper participates in the same overlay
shape as WebSocket/WebRTC. It runs over an in-process WebTransport pair so the example is
deterministic and does not require a public MoQ relay.

## Run

```bash
cd /path/to/primadb/packages/primadb-node
pnpm install
pnpm run build
node ./examples/moq-sync/index.mjs
```

Optional live relay probe:

```bash
MOQ_RELAY=https://relay.example.com/anon node ./scripts/smoke-moq-live.mjs
```

Node v26.1.0 still does not provide built-in WebTransport. `primadb-node/moq` uses
`@webtransport-bun/webtransport` as its Node-only WebTransport provider when no explicit transport
is supplied. The live probe validates the real WebTransport path against public relays; Cloudflare
draft-14 is expected to pass. Cloudflare draft-07 currently fails in the JS MoQ stack with
`E_SESSION_CLOSED`; native Rust uses a separate draft-07 backend.
