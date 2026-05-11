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

Node v22 does not provide built-in WebTransport. Public relays that only accept WebTransport/HTTP3
will require a Node WebTransport polyfill; otherwise the probe should report a connection timeout
rather than silently passing on the deterministic loopback.
