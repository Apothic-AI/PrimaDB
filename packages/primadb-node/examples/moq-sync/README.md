# MoQ Sync

This example uses the `primadb-node/moq` helper to publish PrimaDB sync envelopes over a MoQ
track. It runs over an in-process WebTransport pair so the example is deterministic and does not
require a public MoQ relay.

## Run

```bash
cd /path/to/primadb/packages/primadb-node
pnpm install
pnpm run build
node ./examples/moq-sync/index.mjs
```
