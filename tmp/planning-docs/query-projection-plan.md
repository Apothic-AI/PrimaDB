# Query Projection Performance

## Goal

Separate graph-query candidate evaluation from result projection so filters,
ordering, offset, and limit reject candidates before complete linked JSON values
are materialized, while preserving query semantics and output shapes.

## Plan

- [x] Represent query inputs as lightweight node/field candidates.
- [x] Evaluate only filter and ordering paths, with full-projection fallback for
  whole values and unsupported terminal shapes.
- [x] Project complete values only for the selected result page.
- [x] Preserve direct-index eligibility, ordering, early-stop, and lazy-loading
  behavior.
- [x] Add semantic, cycle, indexed-path, and projection-count regression tests.
- [x] Run formatting, focused tests, the full library suite, all-feature checks,
  and wasm32 checks.
