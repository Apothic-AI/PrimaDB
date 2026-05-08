# AgentFS Backend Feasibility Plan

## Goal

Support investigation of whether AgentFS could use PrimaDB as its storage backend instead of SQLite/Turso.

## Questions

- What PrimaDB API should an external system depend on?
- What persistence and transaction guarantees are currently implemented?
- How are records, binary values, indexes, snapshots, sync, and storage backends represented?
- What gaps would block AgentFS-style filesystem metadata and chunk storage?

## Deliverable

Notes sufficient to assess compatibility from the PrimaDB side.

## AgentFS-Oriented PrimaDB Hardening Plan

This section is the concrete development plan for making PrimaDB suitable for an
AgentFS backend while staying aligned with PrimaDB's own design.

Single-artifact storage is intentionally excluded. PrimaDB should continue to
support directory, browser, OPFS, IndexedDB, and future packed backends without
forcing the core storage model into a single SQLite-style file.

### Design Principles

- Keep PrimaDB graph-native. Add storage and record primitives that preserve
  graph transactions, watches, sync, traversal, auth metadata, and eventual mesh
  convergence.
- Do not create SQL compatibility layers or table-shaped APIs inside PrimaDB.
- Make native durability explicit and testable. If a write is reported durable
  under the strict mode, it must survive process crash and ordinary OS restart
  assumptions.
- Avoid performance regressions by making durability policy configurable while
  keeping safe behavior available for applications that require it.
- Treat recovery as a storage-engine responsibility. External applications
  should not need to repair partial PrimaDB commits.
- Preserve cross-platform direction. Native file locking and fsync helpers must
  be isolated behind platform-aware code paths; browser storage should keep its
  own IndexedDB/OPFS transaction semantics.

### Non-Goals

- No single-file or packed database backend in this tranche.
- No SQL query planner, SQL schema layer, or relational compatibility surface.
- No weakening of mesh eventual consistency, SEA-style trust semantics, watches,
  or on-demand graph traversal.
- No silent best-effort durability claims. If a platform cannot provide a given
  guarantee, the API should report that accurately.

### 1. Crash-Safe Durability And Fsync

Current evidence:

- `SegmentFileStore::apply_transaction` writes a pending journal, node files,
  auth files, index files, manifest, then renames the journal.
- The path currently uses `std::fs::write` and `std::fs::rename`, but no explicit
  file or directory fsync.
- Therefore, a successful API return does not currently mean the full segment
  commit is durable across crash/power-loss boundaries.

Plan:

1. Add a native-only durability policy to `SegmentFileStore`.
   - `Full`: fsync journal records, materialized files, affected directories,
     and manifest before reporting the transaction durable.
   - `Data`: fsync file contents where supported but allow reduced metadata sync
     where the platform exposes that distinction.
   - `Relaxed`: preserve current high-throughput behavior for workloads that
     value speed over local crash durability.
   - The default should be decided explicitly before implementation. My
     recommendation is `Full` for named durable storage configs and `Relaxed`
     only for examples/benchmarks that opt into it.

2. Replace direct `std::fs::write` hot-path commits with atomic write helpers.
   - Write to a temp file in the same directory.
   - Flush and fsync the temp file according to durability policy.
   - Rename temp file to the final path.
   - Fsync the parent directory when the policy requires metadata durability.

3. Add directory sync helpers.
   - Sync the root directory after layout creation.
   - Sync `journal`, `nodes`, `auth`, `node_indexes`, and direct-index bucket
     directories when files are created, renamed, or deleted under `Full`.
   - Implement platform-specific fallbacks with explicit error/reporting
     behavior rather than pretending every platform has identical semantics.

4. Make blob durability consistent enough for AgentFS chunk storage.
   - If AgentFS chunks use PrimaDB blobs, file-backed blob writes need the same
     atomic write and fsync policy as segment files.
   - If chunks are inline `Bytes`, segment durability covers them.

5. Expose explicit sync/flush semantics.
   - Add a Rust-level `sync_storage()` or equivalent that forces outstanding
     durable backend work to the selected guarantee level.
   - Surface this through Node and Python bindings.
   - Browser packages should expose the method only where the backend can make a
     meaningful guarantee, or return a clear unsupported/relaxed result.

Verification:

- Unit tests for atomic write helper success and injected write failures.
- Native tests that reopen after simulated mid-commit failures.
- `cargo test` with `Full`, `Data`, and `Relaxed` policy coverage.
- Microbenchmarks comparing current behavior, `Relaxed`, and `Full` so the
  performance cost is visible rather than accidental.

### 2. Cross-Process Single-Writer Locking

Current evidence:

- PrimaDB serializes mutations inside one `Primadb` instance with an in-process
  mutex.
- Multiple OS processes can still attach to the same `SegmentFiles` directory
  and race at the filesystem layer.

Plan:

1. Add a native `SegmentStoreLock` owned by `SegmentFileStore`.
   - Use a lock file under the segment root, for example `.primadb.lock`.
   - Acquire an exclusive advisory lock before loading or writing a native
     segment store.
   - Hold the lock for the store lifetime.
   - Release on drop.

2. Define lock acquisition modes.
   - `Exclusive`: fail fast if another writer owns the directory.
   - `Wait { timeout }`: wait for another process to release the lock.
   - `Disabled`: available only for explicit advanced/testing cases.

3. Make the lock part of durable storage opening, not an AgentFS wrapper.
   - `open_durable_storage(SegmentFiles { ... })` should acquire the lock.
   - `use_radisk_storage` should acquire the same lock.
   - Direct construction of `SegmentFileStore` should still make the lock
     behavior explicit.

4. Decide read-only semantics separately.
   - Initial implementation should be single-writer/single-attacher for
     simplicity and correctness.
   - Later read-only follower support can be added only after recovery and
     manifest observation are robust.

Verification:

- Same-process test that two stores cannot open the same directory in exclusive
  mode.
- Multi-process test that a second process fails or waits according to mode.
- Drop/reopen test to ensure stale lock files do not permanently brick the
  directory.

### 3. Efficient Keyed / Range Batch APIs

Current evidence:

- PrimaDB has graph transactions and direct scalar index pushdown.
- AgentFS needs fast point lookup, directory listing, chunk range reads, chunk
  deletes, and atomic multi-record mutation.
- These requirements can be met with graph-native record/range APIs rather than
  SQL tables.
- Follow-up review found two remaining production-parity gaps after the first
  record API tranche: SegmentFiles record scans still walked record files before
  filtering, and record batches lacked native preconditions for create-if-absent
  and compare-and-set style updates.

Plan:

1. Introduce a graph-native record keyspace API.
   - Add typed `RecordKey`, `RecordRange`, `RecordBatch`, and `RecordMutation`
     primitives.
   - Keys should be byte-safe, ordered, and prefix-scannable.
   - Records should support JSON values, inline bytes, and blob references.
   - Records should map into deterministic PrimaDB graph paths/nodes so sync,
     watches, auth metadata, and transactions still apply.

2. Add batch mutation entrypoints.
   - `get_many(keys)`.
   - `put_many(entries)`.
   - `delete_many(keys)`.
   - `apply_record_batch(batch)` with atomic graph transaction semantics.
   - Preserve existing change events and touched-path invalidation.

3. Add range and prefix scan entrypoints.
   - `scan_prefix(prefix, options)`.
   - `scan_range(start, end, options)`.
   - Cursor or continuation-token support for large result sets.
   - Direction, limit, and optional value projection.

4. Add chunk-friendly byte paths.
   - Support inline `Bytes` for small chunks.
   - Support `BlobRef` for larger chunks.
   - Provide convenience helpers that make chunk range reads efficient without
     forcing each consumer to rebuild the same graph/index layout.

5. Push scans down into storage when possible.
   - Extend `IncrementalStore` with keyed/range primitives below the graph
     facade.
   - SegmentFiles should use ordered key/index layout for prefix and range
     scans.
   - SegmentFiles record storage should avoid using unbounded key material as a
     single filename/path component. Use bounded ordered key chunks plus an
     overflow hash lane for very long keys, and always filter the stored record
     key after pushdown for correctness.
   - Browser IndexedDB/OPFS segment paths should implement the same logical
     contract where practical.

6. Add conditional record mutations and transaction-scoped record assertions.
   - `RecordBatch` should accept preconditions such as exists, absent, and value
     equality.
   - Preconditions must be checked inside the same local transaction lock as
     the mutations so rollback semantics are preserved.
   - `DeleteRange` expansion should also happen inside the transaction rather
     than before it.
   - Rust transactions should expose record get/assert/put/delete helpers for
     callers that need custom graph-native mutation logic.

7. Keep APIs package-consistent.
   - Rust core first.
   - Node package bindings second.
   - Python package bindings third.
   - Browser TypeScript package last, using async shapes where required by
     browser storage.

AgentFS mapping target:

- Inodes as records keyed by `agentfs/inode/{ino}`.
- Dentries as records keyed by `agentfs/dentry/{parent_ino}/{name}`.
- File chunks as records keyed by `agentfs/chunk/{ino}/{chunk_index}`.
- Symlinks/config/tool-call data as separate prefixes.
- Directory listing becomes a prefix scan.
- `pread` becomes a chunk range scan.
- Truncate/unlink becomes an atomic batch of metadata updates plus range deletes.

Verification:

- Core tests for atomic batch commit, rollback on error, and watch invalidation.
- Prefix/range tests with ordering, limits, cursors, deletes, and binary values.
- Storage-backed tests that prove scans do not require full graph traversal.
- Package smoke tests for Rust, Node, Python, and browser APIs.
- AgentFS prototype benchmark comparing SQLite/Turso path operations to
  PrimaDB-backed record APIs before replacing the backend.

### 4. Recovery From Partial Segment Commits

Current evidence:

- Startup currently loads `manifest.json` and does not reconcile pending/final
  journal files.
- A crash during the current multi-file commit can leave node, auth, index, and
  manifest files from different logical transactions.

Plan:

1. Replace the current journal payload with an explicit commit record.
   - Include transaction id, metadata, node writes, auth writes, index manifest
     writes, direct-index upserts, direct-index deletes, and any future record
     keyspace mutations.
   - Include checksum/hash of the serialized commit record.
   - Include schema/layout version so future recovery code can reject unknown
     formats safely.

2. Make commit records idempotent.
   - Reapplying the same commit record must produce the same filesystem state.
   - Direct-index stale removals should be stored explicitly in the commit
     record, not derived from whatever partial manifest happens to exist after a
     crash.
   - Record/range deletes should be explicit tombstone operations in the commit
     record.

3. Add recovery before metadata load.
   - `load_metadata` should call `recover()` before reading `manifest.json`.
   - Recovery should inspect pending and final journal records.
   - Valid durable records that may have been partially materialized should be
     rolled forward.
   - Incomplete temp files and invalid pending records should be ignored or
     quarantined with diagnostics.

4. Define transaction ordering.
   - Apply commit records in transaction-id order.
   - Track last fully materialized transaction id in the manifest.
   - Never prune journal records needed to recover from the current manifest.
   - Prune only after the manifest and all materialized files are synced.

5. Add repair diagnostics.
   - Return a `StorageRecoveryReport` with applied transactions, ignored temp
     files, quarantined corrupt records, and repaired/deleted stale files.
   - Expose the report through native APIs and package bindings.

6. Add fault injection.
   - Add test-only crash/error points after each critical stage:
     journal write, journal sync, node write, index update, manifest write,
     manifest rename, directory sync, and journal prune.
   - Reopen the store after each injected failure and assert graph/index/query
     consistency.

Verification:

- Recovery tests for every injected crash point.
- Tests for direct-index consistency after crashes during index update.
- Tests for record keyspace consistency once batch APIs land.
- Corrupt/partial JSON journal tests.
- Journal pruning tests that prove required recovery records are not removed too
  early.

### Development Sequence

1. Add SegmentFiles durability policy, atomic write helpers, directory fsync
   helpers, and focused tests.
2. Add SegmentFiles lock acquisition and multi-process lock tests.
3. Redesign commit records and startup recovery, then wire `load_metadata`
   through recovery.
4. Add fault-injection recovery tests and run full native test suite.
5. Add graph-native record/range/batch API in Rust core.
6. Add SegmentFiles storage pushdown for record/range scans.
7. Add package bindings for Node, Python, and browser TypeScript.
8. Add package smoke tests and an AgentFS prototype benchmark before attempting
   an AgentFS backend migration.

### Review Questions Before Implementation

- Should `SegmentFiles` default to `Full` durability whenever opened through
  `open_durable_storage`, accepting slower writes for stronger guarantees?
- Should `use_radisk_storage` be renamed as part of this work to avoid implying
  it is Gun RADisk-compatible?
- Should lock mode default be fail-fast or wait-with-timeout?
- Should record/range APIs be exposed as `db.records(...)`, `db.scope(...).records(...)`,
  or as methods directly on `Primadb`?
