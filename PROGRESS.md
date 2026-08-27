# Progress

## 2026-08-27

### P1 Baseline

- Retained keyed operation compaction, including indexed replacement ordering,
  restoration, rollback, drains, and its structural/large-batch tests.

### P2 Changes

- Exact vector search now uses a bounded borrowed-candidate heap, precomputes ID
  filters, and clones payloads only for retained matches. Its focused tie,
  filtering, payload, and 20,000-entry tests are preserved in `src/vector.rs`.
- BM25 collection scoring now selects only the requested page plus offset, and
  one-shot scoring computes query-term and document statistics directly. Tests
  preserve indexed/one-shot scores, details, paging, and deterministic ordering.
- Graph queries now use lightweight candidates and required-path evaluation,
  applying pagination before full projection while preserving indexed scans,
  linked values, cycles, and fallback semantics.
- Equivalent local watchers now share recomputation, indexed record changes use
  post-apply keys, and bounded queues keep newest state with stale cleanup.
- Segment transaction writes now coalesce direct-index buckets and directory
  syncs while retaining atomic replacement, durability modes, and recovery.
- Full direct-index builds now cache completed acyclic subgraphs across roots;
  cycle-tainted traversal remains root-relative and signed scalar verification
  remains in the materialization path.

### Integration

- Created `primadb-staging` from committed P1 integration change `uumxmkst`.
- Merged exact vector, BM25, query projection, watch coalescing, segment writes,
  and direct-index memoization one head at a time with jj merge revisions.
- Resolved `src/db.rs` by retaining both additive test counters and constructor
  initializers; focused query and watch tests passed after the resolution.
- Resolved `src/engine.rs` by consolidating both added test modules, retaining
  the native segment-write test and every cross-target memoization test.
- Combined the conflicting BM25 and direct-index top-level documentation while
  retaining all P2/task-specific planning and progress files.
- Final verification passed: `cargo fmt --all -- --check`; `cargo test --lib`
  (127 passed); `cargo test --all-targets` (127 passed); `cargo test
  --all-targets --all-features` (156 passed); `cargo check --all-targets
  --all-features` (0 errors, one pre-existing dead-code warning); and `cargo
  check --target wasm32-unknown-unknown --lib` (pass).
- Final `jj resolve -l` and `jj log -r '::@ & conflicts()'` report no conflicts.
