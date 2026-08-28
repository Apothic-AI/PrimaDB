# P2 Performance Integration

## Goal

Integrate all six completed P2 performance changes on the committed P1 baseline
while preserving behavior, focused regression coverage, and task documentation.

## Integrated Work

- [x] Bound exact-vector top-k selection with deterministic distance and ID
  ordering, precomputed filters, and deferred payload cloning.
- [x] Bound BM25 page selection and score one-shot candidates without rebuilding
  a full postings index while preserving scores and result details.
- [x] Evaluate graph-query filters, ordering, offset, and limit before full linked
  result projection, with correctness-first fallback behavior.
- [x] Coalesce equivalent local watcher recomputations, repair indexed-record
  invalidation, and bound local queues while retaining newest state.
- [x] Coalesce direct-index bucket writes and transaction directory syncs while
  preserving durability and journal recovery semantics.
- [x] Memoize acyclic linked-subgraph materialization across direct-index roots
  while preserving root-relative cycle truncation and crypto inspection.

## Integration Plan

- [x] Merge each supplied P2 head separately in the requested order.
- [x] Inspect the working revision and ancestor graph for conflicts after every
  merge.
- [x] Resolve additive query/watch test counters and consolidate segment/direct
  index tests semantically.
- [x] Preserve P2 and task-specific planning/progress documents and combine the
  conflicting top-level BM25/direct-index records here.
- [x] Run the complete native default/all-feature and installed WASM verification
  matrix.
- [x] Record final graph, conflict, status, and test evidence.

## Tranche 5 BM25 Optimization

### Goal

Optimize exact BM25 candidate and collection scoring without changing public
types, score semantics, result detail behavior, stale handling, or ordering.

### Design

- [x] Fuse candidate tokenization, document statistics, query-term statistics,
  and matching postings construction into one pass over the selected candidates.
- [x] Score candidate postings directly, avoiding cloned `TextDocument` maps and
  the old query-term-by-document rescan.
- [x] Add runtime-only dense document/field IDs and compact postings for
  collection scoring while retaining the existing serialized cache format.
- [x] Keep score state sparse by dense document ID so rare-hit searches do not
  allocate or scan the whole collection.
- [x] Preserve duplicate-candidate last-write behavior, exact f32 accumulation
  order, top-k tie ordering, pagination, metadata, snippets, and explanations.
- [x] Add an independent pre-optimization correctness oracle covering candidate
  and collection result equivalence.
- [x] Extend the controlled native benchmark with all-, half-, and rare-hit
  candidate workloads.
