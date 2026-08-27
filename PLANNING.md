# Keyed Operation Compaction

## Goal

Replace linear operation-queue compaction with O(1)-average keyed lookup while
preserving first-key ordering and revision replacement behavior.

## Plan

- [x] Introduce a typed compaction key and cache key-to-operation indices.
- [x] Route queue creation, snapshot restoration, rollback, drain, and flush
  paths through the keyed queue.
- [x] Add focused ordering, restoration, delimiter, and large-batch tests.
- [x] Complete formatting, Rust checks, and full relevant tests.
