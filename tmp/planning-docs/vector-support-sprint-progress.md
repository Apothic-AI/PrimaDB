# PrimaDB Vector Support Sprint Progress

## 2026-05-10

- Started implementation from the approved sprint plan.
- Confirmed current authoritative record path already supports the needed sync substrate:
  - `apply_record_batch` writes through normal local transactions.
  - record writes are included in `SyncFrame::Sync { ops }`.
  - `ChangeEvent` includes `records_changed` and `touched_record_keys`.
  - record watch invalidation already uses recompute plus stable content hashing.
- Confirmed existing planning docs live in `tmp/planning-docs`.
- Next implementation tranche:
  - add vector core types and encoding helpers,
  - implement authoritative split record APIs,
  - implement exact local cache and search,
  - add local vector watches.

## 2026-05-10 Update

- Added vector core types, config structs, search specs/results, manager states, cache manifests, and a backend trait.
- Implemented the mandatory exact backend and exact distance ranking for cosine, L2, and dot metrics.
- Implemented authoritative split records with `writeId`, f32 little-endian payloads, chunk headers, and checksums.
- Implemented local APIs:
  - `create_vector_collection`
  - `put_vector`
  - `delete_vector`
  - `get_vector`
  - `search_vectors`
  - `watch_vector_search`
- Implemented lazy exact cache rebuilds from authoritative records.
- Implemented incomplete split-record exclusion and surfaced incomplete counts through stats.
- Added native cache manifest/data-file writes under `vector-cache/{collection}` when segment storage is configured.
- Added `VectorSearch` pull/watch protocol variants and routed them through existing native/browser accumulation, chunking, hooks, and watch invalidation paths.
- Added optional `vector-edgevec` feature using `../edgevec` with default features disabled.
- Exposed local vector APIs in WASM, Node, and Python bindings.
- Exposed relay/mesh remote vector search/watch APIs in native, WASM, Node, and Python wrappers.
- Added initial Rust tests for split records, incomplete item handling, vector watches, and remote vector response chunking.

## Remaining Follow-Up

Superseded by the update below.

## 2026-05-10 Completion Update

- Activated EdgeVec ANN search behind `vector-edgevec` for collections configured with `backend: "edgevec"`.
- Kept exact search mandatory and routed filtered searches through exact fallback for correctness.
- Added stable logical ID mapping around EdgeVec `VectorId` values; public results return PrimaDB vector IDs.
- Added cache key records carrying logical ID, writeId, and checksum.
- Added native cache load with mmap-backed vector slab reads, manifest validation, backend-version checks, and source-hash validation.
- Added browser OPFS vector cache load/save helpers using the same manifest/data-file format.
- Added per-collection vector capability hints: metric, dimension, state, and backend.
- Updated native and browser relay/mesh peer selection to prefer matching vector collection hints and avoid peers advertising mismatched vector dimensions or non-ready state unless stale results are allowed.
- Added browser hook typings for `vector_search` pull/watch requests and results.
- Added tests for cache file round-trip, native segment cache files, vector capability hints, and EdgeVec ANN search under `vector-edgevec`.
- Verification passed:
  - `cargo check --lib`
  - `cargo check --lib --features native-websocket,native-webrtc`
  - `cargo check --lib --features vector-edgevec`
  - `cargo check --lib --target wasm32-unknown-unknown`
  - `cargo test --lib`
  - `cargo test --lib --features vector-edgevec`
  - `cargo check` in `packages/primadb-node`
  - `cargo check` in `packages/primadb-python`
  - `pnpm --dir packages/primadb typecheck`

## Residual Follow-Up

- Payload-limit stress tests for 384/768/1536-dim batch sync still need a larger integration harness.
- Browser OPFS cache helpers compile, but browser smoke coverage should be added in `packages/primadb/examples/opfs-segments` or a dedicated vector-cache page.
- Peer selection remains backward-compatible with older peers that only advertise generic vector search capability.
