# Record Watch Primitive Analysis Progress

## Completed

- Confirmed `RecordScan`, `RecordScanResult`, record CRUD, and record batch APIs exist in the Rust core and host bindings.
- Confirmed record values are stored as normal graph fields on hashed nodes below `__primadb_records/`.
- Confirmed `PullRequestKind`, `PullResponseBody`, and `RemoteResult` do not currently include record or record-scan variants.
- Confirmed remote watch APIs cover get, map, query, lex, node, and snapshot, but not records.
- Confirmed existing watch refresh paths recompute results and compare stable content hashes before emitting updates.
- Identified that prefix/range record watches cannot use the existing graph-path overlap check precisely because public record keys are hidden behind hashed storage node ids.

## Recommendation

- Add a first-class record-scan watch and remote pull/watch protocol variant before building higher-level watch APIs on top of PrimaDB records.
- Make record-scan invalidation explicitly record-aware instead of trying to force prefix/range semantics through hashed graph node paths.

## Implementation Plan

- Add `PullRequestKind::Records { scan: RecordScan }`, `PullResponseBody::Records { entries, next_cursor }`, and `RemoteResult::Records { result: RecordScanResult }`.
- Route `PullRequestKind::Records` through `Primadb::execute_pull_request_kind` and existing chunk/watch result machinery.
- Extend watch accumulators in native relay, native mesh, browser relay, and browser mesh paths to reconstruct `RecordScanResult` from record chunks.
- Add public record pull/watch helpers where remote helpers already exist, while keeping the underlying request semantics shared.
- Add local `watch_records(scan)` as a convenience over the same scan/recompute/hash behavior used by remote watches.
- Extend `ChangeEvent` with touched record keys and use `RecordScan::matches_key` for precise record-watch invalidation.

## Implementation Progress

- Added the core record pull/watch variants and record result chunking.
- Added local `Primadb::watch_records(...)` with initial-result emission, content-hash suppression, and logical-key invalidation.
- Routed record pull/watch support through native relay, native mesh, browser relay, browser mesh, Node, and Python bindings.
- Updated package declarations, hook types, authored docs, and API-doc generation inputs for `records` requests/results.
- Added focused Rust tests for record pulls, local record-watch invalidation, and record response chunking.

## Verification

- `cargo test record --quiet`
- `cargo test --features "crypto native-websocket native-webrtc scripting" --quiet`
- `cargo check --manifest-path packages/primadb-node/Cargo.toml --quiet`
- `cargo check --manifest-path packages/primadb-python/Cargo.toml --quiet`
- `cargo check --target wasm32-unknown-unknown --features "crypto scripting" --quiet`
- `pnpm --dir packages/primadb exec tsc --noEmit`
- `pnpm --dir packages/primadb-node exec tsc --noEmit --allowJs false index.d.ts`
- `uv run --directory packages/primadb-python python -m py_compile python/primadb/__init__.pyi`
- `pnpm --dir website run generate:api`
- `pnpm --dir website run build`
