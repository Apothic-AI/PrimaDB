# OPFS Backend Plan

## Goal

Add a browser durable-storage backend using Origin Private File System for high-capacity, high-churn browser persistence.

## Decision

Do not add the `opfs` crate for the first implementation. PrimaDB should use `web-sys` directly so the backend can use the lowest-level browser primitives available and avoid an abstraction that currently targets async writable streams rather than OPFS sync access handles. The direct wrapper also keeps the API surface small and makes it easier to add a worker/sync-access fast path later.

## Requirements

- Add a browser-only `opfs_segments` durable storage config.
- Preserve the corrected incremental segment semantics from IndexedDB segment persistence.
- Avoid snapshot-per-change behavior.
- Coalesce write bursts before persistence.
- Expose write stats and logical storage estimates.
- Add a browser regression example proving repeated large writes only touch a bounded number of OPFS files.
- Document the backend and update package API docs.

## Storage Layout

Under OPFS root:

- `<directory>/segments/<encoded namespace>/meta.json`
- `<directory>/segments/<encoded namespace>/nodes/<encoded node id>.json`
- `<directory>/segments/<encoded namespace>/auth/<encoded node id>.json`

Incremental writes update only metadata plus touched node/auth files. Full refreshes replace the namespace.

## Validation

- `cargo test --lib`
- `cargo check --target wasm32-unknown-unknown --features crypto,scripting`
- `pnpm --dir packages/primadb run typecheck`
- `pnpm --dir packages/primadb/examples run smoke:opfs`
- Existing browser storage regression smoke tests where feasible

