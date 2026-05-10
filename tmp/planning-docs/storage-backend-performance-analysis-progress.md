# Storage Backend Performance Analysis Progress

## 2026-05-10

- Confirmed legacy/simple persistence paths still use whole-snapshot JSON:
  `PersistenceTarget::File`, browser `localStorage`, `SnapshotFileAdapter`, and `SnapshotFile`.
- Confirmed the older snapshot-log adapter existed as a JSON checkpoint plus JSONL op-log adapter,
  while the public segment-storage shortcut opened `SegmentFileStore`.
- Confirmed native `SegmentFileStore` is incremental and lazy-restorable, with manifest, journal,
  node files, auth files, direct-index buckets, record entries, lock mode, durability modes, fsync,
  checksum validation, recovery, and vacuum hooks.
- Confirmed native `SegmentFileStore` still encodes persisted node/index/record metadata as JSON
  files rather than a binary page/LSM store.
- Confirmed browser IndexedDB/OPFS segment persistence writes incremental node/auth/meta entries,
  but restore currently rebuilds a full snapshot from all persisted segment entries rather than
  attaching a storage engine with lazy reads.
- Targeted storage tests were started to validate the observed storage behavior.
- `cargo test storage --lib` passed: 10 tests run, 57 filtered out.
- `cargo test segment --lib` passed: 8 tests run, 59 filtered out.
- `cargo test --lib` passed: 67 tests.
- Ran the old native storage example before this rename; it compiled, then failed because the
  example reopened the same segment directory while the first `Primadb` still holds the default
  exclusive lock.
- Inspected the example's partial temporary store.
  The native segment backend wrote `manifest.json`, per-node JSON files, per-auth JSON files,
  direct-index JSON buckets, and `journal/tx-*.json` files.
- Renamed the public segment-storage shortcut to `use_segment_storage(...)`, renamed the legacy
  adapter type to `SnapshotLogFileAdapter`, and renamed the native example to
  `segment_storage.rs`.
- Validation after rename:
  - `cargo fmt`
  - `cargo test --lib`
  - `cargo run --example segment_storage`
