# Transaction Rollback Journal Plan

## Goal

Remove the full database-state clone from local transaction startup while preserving exact rollback behavior.

## Approach

1. Record the clock and operation-vector boundaries when a transaction starts.
2. Journal each node and lazy-loading set entry on first mutation.
3. Record overwritten compacted operations by original vector index; appended operations are removed by truncation.
4. On failure, restore journaled state and rebuild relationship edges only for touched source nodes.
5. Add rollback-integrity and touched-state-scaling regression tests.

## Verification

- `cargo fmt --all -- --check`
- Focused local transaction tests
- Full default-feature library tests
