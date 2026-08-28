# Controlled Benchmark Pass

## Goal

Compare the actual committed P1 integration baseline `815b2194` with the
current `primadb-staging` tree `b0f21bea` using one reproducible native Rust harness. Keep
setup and warmup outside timed sections, retain raw repetition samples, assert
every workload result, and report unavailable counters instead of inventing
them. Do not use the empty staging child as the source identity.

## Scope

- [x] Transactions across small and large in-memory states, including failures.
- [x] Native paginated/full record scans.
- [x] Exact vector top-k at two corpus sizes.
- [x] BM25 collection hit rates and limits plus record-candidate search.
- [x] Query projection/filter/order and equivalent watcher updates.
- [x] Full-durability segment writes.
- [x] Direct-index construction over shared graphs with varied sizes/fan-outs.
- [x] Baseline/staging raw JSON and generated comparison report with source
  revisions, distinct source-tree fingerprints, and a shared runner revision.
- [x] Setup, verification, and applicable persistence phases recorded separately
  from timed operation samples.

## Protocol

The committed `controlled-benchmark` binary uses a fixed seed, two warmups,
nine repetitions, and one timed iteration per repetition. Both revisions are run
with the same runner source under `cargo run --release`, pinned to CPU 2, with
identical environment variables and workload arguments. The raw run requires a
role, full source revision, source-tree fingerprint, and runner revision; compare
rejects same-tree or mismatched-protocol inputs.
