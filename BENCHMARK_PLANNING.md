# Controlled Benchmark Pass

## Goal

Compare the committed pre-P2 baseline `1e00d93f` / `tqluxntq` with the
`primadb-staging` tree using one reproducible native Rust harness. Keep setup
and warmup outside timed sections, retain raw repetition samples, assert every
workload result, and report unavailable counters instead of inventing them.

## Scope

- [x] Transactions across small and large in-memory states, including failures.
- [x] Native paginated/full record scans.
- [x] Exact vector top-k at two corpus sizes.
- [x] BM25 collection hit rates and limits plus record-candidate search.
- [x] Query projection/filter/order and equivalent watcher updates.
- [x] Full-durability segment writes.
- [x] Direct-index construction over shared graphs with varied sizes/fan-outs.
- [x] Baseline/staging raw JSON and generated comparison report.

## Protocol

The committed `controlled-benchmark` binary uses a fixed seed, two warmups, nine
repetitions, and ten timed iterations per repetition. Both revisions are run
with `cargo run --release`, pinned to one CPU when available, with identical
environment variables and workload arguments.
