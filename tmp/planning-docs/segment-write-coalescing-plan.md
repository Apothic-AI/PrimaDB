# P2 Segment Write Coalescing Plan

## Goal

Reduce native SegmentFiles write amplification by coalescing direct-index bucket
updates and durability operations within one transaction, while preserving
journal ordering, atomic file replacement, durability modes, recovery, and
query correctness.

## Plan

- [x] Batch direct-index removals and upserts by physical bucket path.
- [x] Skip unchanged direct-index buckets and write each changed bucket once.
- [x] Defer and deduplicate Full-mode parent-directory syncs across transaction
  artifacts while keeping file data sync semantics unchanged.
- [x] Add test-only write, byte, fsync, and directory-sync instrumentation.
- [x] Cover bucket coalescing, unchanged indexes, durability modes, recovery,
  reopen correctness, formatting, tests, and native/WASM checks.
