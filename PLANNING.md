# Full-Graph Direct-Index Construction

## Goal

Avoid repeated recursive linked-subgraph materialization while building a full
storage transaction, preserving direct-index, cycle, crypto, and transaction
semantics.

## Plan

- [x] Share acyclic linked-subgraph materializations across all indexed roots.
- [x] Detect cycle-tainted traversals and preserve per-root cycle truncation.
- [x] Add focused fan-out, large shared-graph, cycle, and crypto tests.
- [x] Complete formatting, default, crypto, all-feature, and all-target checks.
