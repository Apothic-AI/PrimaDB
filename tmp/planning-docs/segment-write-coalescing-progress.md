# P2 Segment Write Coalescing Progress

## 2026-08-27

- Created isolated workspace `primadb-p2-segment-write-coalescing-20260827-1`
  from the clean P1 integration tip.
- Reworked native SegmentFiles transaction materialization to read, mutate, and
  atomically replace each affected direct-index bucket once per transaction.
- Unchanged direct-index buckets are no longer rewritten.
- Batched parent-directory syncs across node, auth, direct-index, node-manifest,
  record, and journal-prune writes while preserving the pending-journal-first
  recovery protocol and syncing all affected directories before the manifest.
- Removed a redundant pending-journal directory sync and repeated layout syncs.
- Added test-only file/direct-index/byte/fsync/directory-sync counters plus
  focused regressions for duplicate bucket changes, unchanged indexes,
  Full/Data/Relaxed policies, recovery, and reopen correctness.
- Verification passed: `cargo fmt --all -- --check`; `cargo test --lib` with
  110 tests; `cargo check --all-targets --all-features`; and
  `cargo check --target wasm32-unknown-unknown`. The all-feature check retains
  one pre-existing dead-code warning for
  `full_storage_transaction_without_pending_ops`.
