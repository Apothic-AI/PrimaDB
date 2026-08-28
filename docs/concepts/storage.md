---
title: Storage And Durability
sidebar_position: 7
---

PrimaDB’s current storage path is incremental and segment-backed rather than “always load the full
snapshot first.”

## Supported Durable Storage Paths

Native:

- segment-backed durable storage with crash recovery
- native SegmentFiles single-writer locking
- file-backed blobs
- explicit vacuum and blob GC

Browser:

- `localStorage`
- IndexedDB persistence helpers
- OPFS segment persistence
- IndexedDB blob storage

## Why This Matters

The storage engine now supports:

- lazy node restore
- canonical node/index records
- nested scalar indexes
- ordered keyed records for point reads, prefix/range scans, conditional batches, and batch mutation
- bounded journal retention
- pending/final commit journals with checksum validation and roll-forward recovery
- explicit `sync_storage()` / `syncStorage()` hooks for native fsync-style flushing
- exclusive native storage locks by default
- explicit vacuum/GC
- BLAKE3-prefixed content-addressed blob references
- bounded incremental browser segment writes for IndexedDB and OPFS

That closes a meaningful gap relative to the older snapshot-centered design.

## Browser Backend Choice

Use OPFS segments for large or high-churn browser-local data when available. OPFS stores segment
records as browser-private files and avoids IndexedDB's structured-clone overhead for large opaque
values. IndexedDB segments remain the compatibility path for browsers without OPFS.

Browser storage does not expose OS-level fsync or file locks. OPFS and IndexedDB segment persistence
are still incremental and coalesced, but they rely on browser durability semantics.

## Native SegmentFiles Guarantees

`SegmentFiles` is the strongest native local-durability backend. It defaults to:

- `durability: "full"`: atomically materialize the transaction, make its checksummed WAL commit record durable at one transaction boundary, and replay that WAL after a crash if materialized files were interrupted. Full-mode WAL records remain available until an explicit storage vacuum writes a checksummed full-state checkpoint and safely prunes them.
- `lockMode: { kind: "exclusive" }`: fail fast if another process already owns the same segment directory.
- startup recovery: validate checkpoint and pending/final journal records, then roll forward materialized node/index/auth/record files in transaction order when needed.

Callers can explicitly choose `durability: "data"` when they only need file-data sync without
directory fsync, or `durability: "relaxed"` when the surrounding application owns durability.
`lockMode: { kind: "disabled" }` should only be used when an external process lock protects the
directory.

Native SDKs also expose:

- `sync_storage()` / `syncStorage()` to force a storage flush report.
- `storage_recovery_report()` / `storageRecoveryReport()` to inspect the latest startup recovery pass.
- `close_durable_storage()` / `closeDurableStorage()` to release the store and its file lock deterministically.

Native file-backed blobs use the same durability vocabulary. `FileBlobStore` defaults to
`durability: "full"` and writes blob data/metadata through temp-file replacement plus fsync before
reporting success. This matters when keyed records store `BlobRef` values for larger chunk payloads.

## Keyed Records

Keyed records are graph-native primitives for workloads that need ordered lookup without building a
SQL-like layer. They are useful for filesystem-shaped data such as inodes, dentries, and chunk keys:

- `put_record` / `putRecord` stores JSON.
- `put_record_bytes` / `putRecordBytes` stores binary data.
- `put_record_blob` / `putRecordBlob` stores larger binary data in the configured blob store and records the blob ref.
- `scan_records` / `scanRecords` supports prefix, start/end bounds, reverse order, limit, and cursor.
- `watch_records` / `watchRecords` emits an initial scan result and later emits only when matching record keys change.
- `apply_record_batch` / `applyRecordBatch` applies conditional put/delete/delete-range mutations atomically through the graph transaction path.

Native `SegmentFiles` stores records under ordered, bounded key paths instead of hashing every key into
an unscannable bucket. Prefix scans descend directly into the matching key subtree and still filter the
stored record key for correctness. Very long keys use a bounded indexed-prefix plus hash overflow layout
so record storage does not depend on unbounded filename or path components.

Record watches use the same graph change pipeline as normal subscriptions and remote watches, but
their invalidation tracks logical record keys instead of the internal hashed storage node ids. Prefix
and range watches therefore recompute based on `RecordScan` overlap with touched keys, and fall back
to a broad refresh only when a remote/imported change cannot expose the logical key.

Record batches can include preconditions:

```js
db.applyRecordBatch({
  preconditions: [{ kind: "absent", key: "agentfs/inodes/2" }],
  mutations: [
    {
      kind: "put",
      key: "agentfs/inodes/2",
      value: { kind: "json", value: { mode: "file", size: 0 } },
    },
  ],
});
```

## What Is Deferred

PrimaDB does not currently implement Gun’s `Book`, and that is intentional. The storage direction is
closer to a PrimaDB-native segment/index engine than to a direct port of Gun’s experimental string-
packed page format.
