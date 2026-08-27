# Progress

## 2026-08-27

- Added `CompactedOperations`, retaining the ordered operation vector and a
  typed `HashMap` index for O(1)-average compaction.
- Preserved the original queue slot when a newer operation replaces an older
  operation with the same field or set-member identity.
- Rebuilt or maintained the index across initialization, snapshot and metadata
  restoration, transaction rollback, pending drains, and durable prefix drains.
- Added structural tests covering ordering, revisions, typed-key delimiter
  safety, restoration, drain maintenance, and 20,000-key batches.
- Verification completed: `cargo fmt -- --check`, 102 default tests,
  all-target/all-feature checking, and wasm32 checking all pass. The
  all-target/all-feature check reports one pre-existing dead-code warning.
