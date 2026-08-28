# Benchmark Progress

## 2026-08-28

- Created the isolated `primadb-benchmark-provenance-20260828` workspace from
  clean staging `cd81a2e9`; benchmark target identity is the non-empty staging
  tree `b0f21bea`, not that empty child.
- Corrected the runner to require `baseline`/`staging` roles, full source
  revisions, source-tree fingerprints, and a shared runner revision. Comparison
  rejects the wrong P1 baseline, same source tree, same revision, or protocol
  mismatch.
- Preserved the deterministic workload suite and actual protocol: seed
  `22567760790700872`, two warmups, nine repetitions, one timed iteration,
  `cargo run --release`, CPU 2 affinity, and `powersave` governor.
- Ran both revisions successfully with runtime correctness assertions: P1
  baseline `815b2194013cf419c6134060fd57e13bb4ed4af9` and staging
  `b0f21beaec75de0bafff944dde1e9d0838540644`; the shared corrected runner is
  `5d63ab031a910775f5e262d1aad7c9ef08bdb692`. All 20 samples and raw samples
  are retained in `/tmp/primadb-tranche1-{baseline,staging}.json` during the
  pass; the committed report is
  `benchmarks/controlled-tranche1-report-20260828.md`.
- Added separate setup and verification phase measurements where practical and
  per-repetition persistence samples for full durability. Resource reporting is
  limited to process RSS/CPU proxies and filesystem footprint; allocation,
  syscall, fsync, file-open, and lock counters remain explicitly unavailable.
