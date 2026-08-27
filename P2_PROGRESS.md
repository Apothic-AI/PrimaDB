# Progress

## 2026-08-27

- Replaced full-corpus `VectorMatch` allocation and sort in exact search with a
  bounded max-heap containing borrowed entries and distances.
- Preserved ascending distance and ascending ID tie-breaking by evicting the
  worst retained candidate and sorting only the retained candidates.
- Prepared the requested ID filter set once per search, before scanning entries.
- Deferred optional metadata and vector clones until final result materialization.
- Added focused tests for filtered ties and payload behavior, plus a 20,000-entry
  small-top-k scan.
- Verification: `cargo fmt`, focused vector tests (10 passed), default
  `cargo test --all-targets` (109 passed), and
  `cargo test --all-targets --all-features` (137 passed).
- `cargo check --all-targets --all-features` passes with 0 errors and one
  pre-existing dead-code warning in `src/db.rs`.
- `jj resolve -l` and the ancestor conflict query report no conflicts.
