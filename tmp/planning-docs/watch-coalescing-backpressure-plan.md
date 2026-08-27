# Local Watch Coalescing and Backpressure Plan

## Goal

Reduce redundant local watcher work, invalidate indexed collections from the logical records actually changed, and keep local subscription queues bounded without changing public subscription APIs.

## Scope

- Share one recomputation among equivalent watchers within a change notification.
- Derive touched record keys from applied record-node state so value updates invalidate the correct text/vector collection.
- Use bounded newest-state queues for local subscriptions; retain closed-channel stale removal and normal FIFO delivery when consumers keep up.
- Cover initial delivery, hash suppression, ordering, indexed updates, equivalent watchers, and queue saturation.

## Verification

- `cargo fmt --all -- --check`
- Focused watch tests
- `cargo test --lib`
- Native transport feature tests
- `cargo check --lib`
