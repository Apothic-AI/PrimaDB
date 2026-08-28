# Direct-Index Tranche 4 Progress

## 2026-08-28

- Created isolated workspace from clean staging revision `cd81a2e9`.
- Added candidate-aware, offset-aware `DirectIndexScan` behavior and native
  ordered physical traversal with early stop for literal scalar-key paths.
- Preserved deterministic descending ordering by reversing only sortable values;
  node and path ties remain ascending.
- Added hashed long-key fallback sorting and regression coverage for long-key
  physical hashing and tie ordering.
- Replaced whole materialized JSON cache entries with shared compact relative
  scalar-leaf fragments, preserving cycle truncation and crypto unwrapping.
- Focused direct-index, shared-graph, cycle, and query-window tests pass.
- Verification passed: `cargo fmt --all -- --check`; `cargo test --all-targets`
  (130 passed); `cargo test --all-targets --all-features` (159 passed); `cargo
  check --all-targets --all-features` (0 errors, one pre-existing unused-method
  warning); and `cargo check --target wasm32-unknown-unknown --lib` (pass).
- Benchmark harness completed with one warmup and three repetitions: direct
  index build median `974396 ns` for 64 roots/depth 8/fanout 2 and `9793159 ns`
  for 256 roots/depth 16/fanout 4. The raw report is
  `tmp/direct-index-tranche4-benchmark.json`.
