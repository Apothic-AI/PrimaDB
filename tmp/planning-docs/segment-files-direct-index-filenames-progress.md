# SegmentFiles Direct Index Filename Hardening Progress

## 2026-05-06

- Confirmed native `SegmentFileStore` derives direct index paths from full logical sortable scalar keys.
- Confirmed string sortable keys are hex-encoded full string values, so large encrypted checkpoint strings can exceed filesystem component limits.
- Planned a bounded physical direct-index layout using bucket files keyed by full logical direct index keys.
- Implemented bounded native SegmentFiles direct-index physical components.
- Direct index entries are now stored in bucket files keyed by full logical direct index keys, so oversized values and physical hash collisions do not overwrite unrelated entries.
- Added focused native regression tests for large string scalar write/update/reload and large string direct-index equality/prefix queries.
- Validation so far:
  - `cargo test --lib segment_files_persist_large_string_scalar_without_filename_limit`
  - `cargo test --lib segment_files_query_large_string_scalar_direct_indexes`
  - `cargo fmt --check`
  - `cargo test --lib`
  - `cargo test`
  - `cargo test --lib --features crypto`
