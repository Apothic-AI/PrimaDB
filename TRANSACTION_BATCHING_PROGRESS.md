# Progress

## 2026-08-28

- Created isolated workspace `primadb-tranche6-rel-index-batching-20260828-083000`
  from clean staging revision `cd81a2e9`.
- Confirmed immediate reindexing in `Inner::apply_operation_internal` was the
  transaction batching boundary.
- Implemented journal-owned source deduplication, commit/lazy-read flushing,
  rollback restoration, and test instrumentation.
- Added focused coverage for repeated and multiple sources, links and sets,
  traversal before and after commit, rollback, watchers, remote applies,
  persistence, and operation/source cost.
- Focused instrumentation timings, with setup excluded: 16 operations/1 source
  516226 ns; 64/1 1987216 ns; 64/4 2114321 ns; 256/4 4633153 ns. Every case
  reported exactly its touched-source count as reindex calls.
- Verification passed: `cargo fmt --all -- --check`; `cargo test --lib` (133
  passed); `cargo test --all-targets` (133 passed); `cargo test --all-targets
  --all-features` (162 passed); `cargo check --all-targets --all-features` (0
  errors, one existing dead-code warning); and `cargo check --target
  wasm32-unknown-unknown --lib`.
