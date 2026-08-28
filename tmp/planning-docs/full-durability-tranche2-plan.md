# Full Durability Tranche 2 Plan

## Goal

Reduce Full-mode SegmentFiles persistence cost without weakening crash durability,
atomic visibility, checksums, journal ordering, or the distinct Data and Relaxed
policies.

## Protocol

- [x] Keep the transaction journal as the authoritative, checksummed WAL.
- [x] Materialize each changed artifact through an atomic replacement without a
  per-artifact Full-mode sync.
- [x] Finalize the journal and perform one logical Full-mode durability boundary
  for the transaction.
- [x] Replay WAL records independently of the manifest's last-materialized hint.
- [x] Add a checksummed full-state checkpoint and retain Full WAL records until
  explicit vacuum checkpointing has completed.
- [x] Preserve Data file-data sync and Relaxed no-sync behavior.

## Verification

- [x] Cover all partial artifact and commit-boundary fault points.
- [x] Cover manifest-ahead replay and checkpoint prune/reopen recovery.
- [x] Instrument file writes, bytes, file/directory syncs, and commit barriers.
- [x] Run the complete native and WASM verification matrix and controlled benchmark.
