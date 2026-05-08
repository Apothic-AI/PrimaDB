# AgentFS Backend Feasibility Progress

## 2026-05-07

- Started cross-repo compatibility investigation.
- Existing PrimaDB planning docs found under `tmp/planning-docs`.
- Confirmed PrimaDB currently exposes graph nodes, local transactions, change subscriptions, snapshots, segment-file storage, direct scalar indexes, binary field values, and content-addressed blob storage.
- Confirmed segment-file storage persists nodes and indexes as multiple JSON files plus manifest/journal files, not as one SQLite-style database file.
- Ran targeted tests:
  - `cargo test segment --lib`: 3 passed.
  - `cargo test transaction --lib`: 9 passed.
  - `cargo test binary --lib`: 0 matched, 59 filtered.
- Current finding: PrimaDB is plausible as an AgentFS experimental backend if AgentFS maps inode/dentry/chunk concepts into PrimaDB nodes or adds lower-level keyed-record primitives. It is not currently a SQL-compatible replacement.
- Follow-up classification: AgentFS can model inode/dentry/chunk data on PrimaDB, but PrimaDB should own any guarantees for crash-safe segment commits, fsync-like durability, recovery/replay of committed segment journals, high-throughput keyed/batched access, and optional compact/single-artifact storage.
- Evaluated external recommendations against current PrimaDB implementation:
  - Crash-safe durability/fsync and startup recovery are real SegmentFiles gaps. `SegmentFileStore::apply_transaction` writes pending journal, node/index/auth files, manifest, then renames the journal, but does not fsync files/directories and `load_metadata` only reads `manifest.json`.
  - Cross-process/single-writer enforcement is currently not provided by SegmentFiles. In-process mutation is serialized by `Primadb`'s mutex, but separate processes can attach to the same directory.
  - Keyed/range batch APIs would fit PrimaDB if modeled as storage/query primitives, but should not become SQL-shaped APIs. AgentFS-style inodes/dentries/chunks need efficient point reads, prefix/range scans, batch mutation, and byte/blob paths.
  - Single-artifact storage is optional product positioning, not a core PrimaDB requirement. A packed backend could be useful, but the current directory-backed segment store is consistent with PrimaDB's cross-platform graph/store model.
- Expanded `agentfs-backend-feasibility-plan.md` with a reviewable implementation plan for crash-safe fsync durability, cross-process single-writer locking, graph-native keyed/range batch APIs, and partial segment commit recovery. Single-artifact storage is explicitly excluded from this tranche.
- Implemented native `SegmentFiles` durability options:
  - `SegmentDurability::Full` is the default and fsyncs written files plus parent directories.
  - `SegmentDurability::Data` syncs file contents without parent-directory fsync.
  - `SegmentDurability::Relaxed` keeps the lower-overhead write path for callers that explicitly opt out.
- Implemented cross-process native segment-store locking:
  - default lock mode is exclusive/fail-fast.
  - optional wait mode retries lock acquisition until timeout.
  - disabled mode is available only when the caller owns external exclusion.
- Implemented segment commit records with BLAKE3 checksums, pending/final journal recovery, and startup roll-forward from valid pending or final journal records.
- Added durable file replacement helpers so hot node/index/auth/record/manifest writes use temp-file replacement rather than delete-then-write.
- Added graph-native keyed record APIs backed by hidden graph nodes and SegmentFiles record buckets:
  - point get/put/delete
  - JSON, binary, and blob record values
  - prefix/range scans with cursor/limit/reverse
  - atomic record batches with put/delete/delete-range mutations
- Hardened native file-backed blob storage:
  - file blobs default to `durability: "full"`.
  - blob data and metadata are written through temp-file replacement and fsync.
  - blob bindings report the selected durability through Rust, Node, and Python.
- Exposed storage sync, recovery report, explicit durable-storage close, and record APIs through Rust, Node, Python, and browser WASM bindings.
- Added regression coverage for:
  - cross-process/single-writer segment lock rejection.
  - recovery from pending commit journal after injected crash.
  - transaction-id ordered recovery when pending and final journal filenames would sort incorrectly lexicographically.
  - SegmentFiles pushdown for record prefix/range scans and range deletes.
- Final validation completed:
  - `cargo check --lib`
  - `cargo check --manifest-path packages/primadb-node/Cargo.toml`
  - `cargo check --manifest-path packages/primadb-python/Cargo.toml`
  - `cargo test --lib`
  - `cargo test`
  - `cargo test --examples`
  - `cargo fmt --check`
  - `git diff --check`
  - `pnpm --dir website run build`
  - `pnpm --dir packages/primadb run build`
  - `pnpm --dir packages/primadb run typecheck`
  - `pnpm --dir packages/primadb run smoke`
  - `pnpm --dir packages/primadb-node run smoke:core`
  - `uv run maturin develop --manifest-path Cargo.toml && uv run python scripts/smoke_core.py`

## 2026-05-08

- Cross-checked PrimaDB branch `staging` at commit `8059489` from AgentFS.
- Confirmed AgentFS has no current PrimaDB source/test integration, so the new Rust construction changes do not require an AgentFS patch yet.
- Confirmed the updated PrimaDB APIs map cleanly to the AgentFS storage adapter design:
  - `SegmentFiles` full durability and exclusive locks cover local filesystem durability/exclusion requirements.
  - `sync_storage` can back AgentFS `fsync` semantics for the PrimaDB backend.
  - `close_durable_storage` handles test/script reopen cases caused by exclusive locks.
  - keyed record APIs are a better fit than hand-modeling every inode/dentry/chunk as visible application graph paths.
- Remaining production-parity caveat: `SegmentFiles` record prefix/range scans currently collect record bucket files before filtering, so large AgentFS directory listings and range deletes may still need prefix-indexed record storage in PrimaDB rather than AgentFS-specific secondary indexes.
- Remaining API ergonomics caveat: AgentFS can serialize mutations itself, but PrimaDB-native record preconditions or transaction-scoped record get/assert methods would make create-if-absent, rename conflict checks, and inode allocation less brittle.
- Re-ran targeted checks:
  - `cargo test record --lib`: 1 passed, 62 filtered.
  - `cargo test segment --lib`: 7 passed, 56 filtered.
  - `cargo test lock --lib`: 1 passed, 62 filtered.
- Implemented the next AgentFS-oriented record-storage tranche:
  - Replaced native SegmentFiles record buckets with ordered `records/by_key`
    entries so prefix/range scans can prune by key prefix instead of walking all
    record files first.
  - Added a bounded indexed-prefix plus hash overflow layout for very long
    record keys, avoiding unbounded filename/path components while preserving
    record-key filtering correctness.
  - Added `RecordPrecondition` to record batches with `exists`, `absent`, and
    value-equality checks.
  - Moved `DeleteRange` expansion into the local transaction lock so conditional
    checks, range expansion, mutations, and rollback share one atomic graph
    transaction.
  - Added Rust transaction-scoped record helpers:
    `get_record`, `assert_record_exists`, `assert_record_absent`,
    `assert_record_value`, `put_record`, and `delete_record`.
  - Updated browser TypeScript, native Node, and Python package declarations and
    smoke scripts for conditional record batches.
  - Updated storage/package docs and regenerated API reference docs.
- Added focused regression coverage for:
  - SegmentFiles prefix scans not reading unrelated record-key subtrees.
  - conditional record batch rollback on failed preconditions.
  - transaction-scoped record get/assert/put/delete helpers.
  - very long record keys persisting and prefix-scanning without filename/path
    length failures.
- Fixed native SegmentFiles recovery-test fault injection to be keyed by segment
  root. The previous single global fault slot could be overwritten by another
  parallel recovery test during `cargo test`.
- Re-ran targeted check:
  - `cargo test record --lib`: 2 passed, 62 filtered.
- Final validation completed:
  - `cargo test --lib`: 64 passed.
  - `cargo test`: 64 passed.
  - `cargo test --examples`: 0 passed across 11 example suites.
  - `cargo check --lib`: passed with existing dead-code warnings.
  - `cargo check --manifest-path packages/primadb-node/Cargo.toml`: passed with existing dead-code warnings.
  - `cargo check --manifest-path packages/primadb-python/Cargo.toml`: passed with existing dead-code warnings.
  - `pnpm --dir packages/primadb run build`: passed.
  - `pnpm --dir packages/primadb run typecheck`: passed.
  - `pnpm --dir packages/primadb run smoke`: passed.
  - `pnpm --dir packages/primadb-node run smoke:core`: passed.
  - `uv run maturin develop --manifest-path Cargo.toml && uv run python scripts/smoke_core.py`: passed; maturin reported the existing local `patchelf` warning.
  - `pnpm --dir website run build`: passed.
  - `cargo fmt --check`: passed.
  - `git diff --check`: passed.
