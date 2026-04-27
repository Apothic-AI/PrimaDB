# MoQ Sync

This browser package example publishes PrimaDB sync envelopes over a MoQ track using the
`primadb/moq` helper.

It uses an in-process WebTransport pair so it runs without a public MoQ relay. The same helper can
also connect to a real MoQ relay with `connectPrimadbMoq(...)`; `@moq/lite` handles WebTransport
and WebSocket fallback selection.

## Run

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4181/moq-sync/
```

## Smoke Test

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm run smoke:moq
```
