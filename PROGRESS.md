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

## 2026-08-28

### Tranche 5 BM25

- Created isolated workspace `primadb-tranche5-bm25-20260828-055044` from clean
  source parent `cd81a2e9`.
- Candidate scoring now consumes owned candidates once, deduplicates with the
  prior last-write behavior, tokenizes each indexed field once, accumulates
  query-term statistics, and scores only generated matching postings.
- Collection caches retain their existing serialized postings and document
  format, with runtime-only dense document/field IDs and compact postings used
  for sparse score accumulation and bounded top-k selection.
- Added correctness-oracle regressions for single/multi-term queries, all/half/
  rare hit rates, candidate limits, pagination, selected fields, weighted fields,
  deterministic ties, metadata, snippets, explanations, and exact match values.
- Controlled release benchmark used seed `0x502d42454e4348`, two warmups, nine
  repetitions, and ten iterations. Against the clean source parent, collection
  medians changed by -38.6% (all, limit 10), -35.6% (half, limit 10), -34.1%
  (rare, limit 10), and -35.4% (rare, limit 50). The established rare candidate
  workload changed by -0.1% median with a lower p95; new candidate workload
  medians were 2.179 ms (all), 1.899 ms (half), and 1.751 ms (rare).
- An initial dense score-slot experiment regressed sparse collection queries and
  was discarded; the committed implementation uses sparse score state.

### Verification

- `cargo fmt --all -- --check` passed.
- `cargo test --lib text_search` passed (16 tests); `cargo test --lib` passed
  (129 tests); `cargo test --all-targets` passed (129 tests); and
  `cargo test --all-targets --all-features` passed (158 tests).
- `cargo check --all-targets --all-features` passed with zero errors and the
  existing `full_storage_transaction_without_pending_ops` dead-code warning.
- `cargo check --target wasm32-unknown-unknown --lib` passed.
