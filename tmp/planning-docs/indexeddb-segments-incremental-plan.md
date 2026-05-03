# IndexedDB Segments Incremental Persistence Plan

## Problem

The WASM/browser `indexed_db_segments` durable storage backend is advertised as incremental, but its auto-persistence path rewrites a full graph snapshot for each coalesced change. It builds a segment transaction from `snapshot.nodes`, deletes every existing key in the IndexedDB namespace, then writes every current node/index entry again.

This creates extreme write amplification for large or high-churn values, especially opaque encrypted checkpoint records.

## Goals

- Keep `IndexedDbSegments` as a true incremental browser durable backend.
- Persist repeated updates with bounded writes proportional to changed storage nodes, not total graph size.
- Preserve explicit full snapshot replacement for initial flush/manual save/repair.
- Do not mark pending durable operations flushed unless IndexedDB write succeeds.
- Add write coalescing/backpressure diagnostics for browser callers.
- Add regression coverage proving repeated writes do not require repeated full namespace rewrites.

## Implementation

1. Add core helpers that produce browser segment transactions from current `unflushed_ops` without clearing them before a successful write.
2. Add a success marker that clears only the saved operation prefix and advances the storage transaction id after IndexedDB persistence succeeds.
3. Split IndexedDB segment writes into:
   - full replacement for explicit snapshot-style saves and initial flush
   - incremental transaction application for auto-persisted changes
4. For incremental writes, update only metadata, touched nodes, touched auth metadata, touched node-index manifests, and new direct-index entries.
5. Delete stale direct-index entries by comparing prior node-index manifests with the new transaction manifests.
6. Add browser persistence stats for queued/coalesced events, successful/failed transactions, full vs incremental writes, key counts, deleted keys, and estimated bytes written.
7. Add tests around transaction construction and incremental write planning so repeated large writes stay bounded.

## Validation

- `cargo fmt`
- targeted Rust tests for incremental segment transaction behavior
- WASM/browser package build if available in this workspace
- browser smoke/regression test where practical
