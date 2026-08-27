# Benchmark Progress

## 2026-08-27

- Created the isolated `primadb-improved-benchmark-20260827` workspace from
  staging `eec33f7c`.
- Inspected existing infrastructure: no committed benchmark target was present;
  existing correctness tests and public native APIs are used by the harness.
- Added the controlled Rust runner and report-generation protocol. Execution and
- revision comparison completed after fixing the initial watcher snapshot shape
  assertion and bounding the synthetic graph fan-out generator.
- Final protocol: seed `56394203049952392`, two warmups, nine repetitions, one
  timed iteration, `cargo run --release`, CPU 2 affinity, and temporary
  `performance` governor. Both `1e00d93f` and staging `eec33f7c` completed all
  18 workload variants with correctness assertions.
- Staging correctness gate: `cargo test --lib` passed with 127 tests; formatting
  and release benchmark-binary checks passed.
- Generated `benchmarks/controlled-p2-report-20260827.md` with raw samples,
  median/p95/min/max, throughput, resource proxies, environment, interpretation,
  and limitations. The report finds transaction median improvements, mixed BM25
  behavior, no direct-index speedup in these workloads, and full durability as
  the dominant measured path.
