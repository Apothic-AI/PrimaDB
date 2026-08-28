# Transaction Relationship-Index Batching

## Goal

Defer relationship-index maintenance for accepted operations in a local
multi-operation transaction, deduplicate affected source nodes, and flush each
source once at commit without changing state, journal, remote-apply, traversal,
watcher, persistence, or operation-order semantics.

## Design

- The transaction journal owns deterministic sets of affected sources and
  sources awaiting index refresh.
- Local operation application records a source instead of rebuilding its index
  immediately. Remote operation application keeps the existing immediate path.
- A transaction traversal read flushes pending sources lazily so its index view
  is current; the successful commit flushes any remaining sources.
- Rollback restores node and queue state first, then rebuilds every affected
  source from restored state. This also covers a traversal read before failure.
- Test-only counters measure actual source reindex calls, and a focused timing
  test separates setup from transaction work across operation/source counts.

## Verification

- [x] Focused relationship-index transaction tests
- [x] `cargo fmt --all -- --check`
- [x] Native library, all-target, and all-feature checks
- [x] WASM library check when the target is available
