# Query Projection Progress

## 2026-08-27

- Added lightweight node/field query candidates and precomputed filter/order
  path plans.
- Filters and ordering now read only required scalar and linked-node paths;
  whole values, sets, binary/blob terminals, missing nodes, and cycles retain a
  correctness-first fallback to the existing materializer.
- Offset and limit are applied before complete output projection. The storage
  index path still performs indexed eligibility and ordered scanning first.
- Added tests for nested linked projections, order/offset/limit, `$key`,
  `$value`, cycles, existing nested direct indexes, and a 128-candidate linked
  payload workload that asserts exactly one full projection.
- Verification completed: `cargo fmt -- --check`, 110 default library tests,
  138 all-feature library tests, all-target/all-feature checking, and wasm32
  checking pass. The all-target/all-feature check reports one pre-existing
  dead-code warning.
