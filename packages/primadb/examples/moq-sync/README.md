# MoQ Sync

This browser package example publishes PrimaDB `RouteEnvelope` traffic over a MoQ track using the
`primadb/moq` helper. Sync frames are carried as route payloads, matching the WebSocket/WebRTC
overlay protocol instead of using a separate MoQ-only sync format.

It uses an in-process WebTransport pair so it runs without a public MoQ relay. The same helper can
also connect to a real MoQ relay with `connectPrimadbMoq(...)`; `@moq/lite` handles WebTransport
and WebSocket fallback selection.

## Run

```bash
cd /path/to/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4181/moq-sync/
```

## Smoke Test

```bash
cd /path/to/primadb/packages/primadb/examples
pnpm run smoke:moq
```

Optional live relay probe:

```bash
MOQ_RELAY=https://relay.example.com/anon pnpm run smoke:moq-live
```

If `MOQ_RELAY` is not set, the live probe reads `MOQ_DRAFT14_RELAY` and `MOQ_DRAFT07_RELAY` from
the project `.env`. The probe reports browser/browser, browser/Node, and browser WebRTC-over-MoQ
signaling separately so draft/runtime failures are visible instead of hidden by the deterministic
loopback. Cloudflare draft-14 is expected to pass for the browser JS stack; draft-07 currently does
not negotiate through `@moq/lite`.
