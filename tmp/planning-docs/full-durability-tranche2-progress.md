# Full Durability Tranche 2 Progress

## 2026-08-28

- Created isolated workspace `primadb-tranche2-full-durability-20260828` from
  the clean `primadb-staging` tip.
- Added transaction-level Full-mode WAL finalization: changed segment artifacts
  remain atomic, but only the finalized checksummed WAL record receives the
  transaction durability boundary.
- Added independent checksummed full-state checkpoints. Full WAL records are not
  automatically pruned; explicit storage vacuum checkpoints the complete state
  before pruning records covered by that checkpoint.
- Recovery now replays committed/pending WAL records after the checkpoint rather
  than trusting `manifest.json`'s non-durable materialization hint.
- Added focused tests for six partial-commit fault points, manifest-ahead replay,
  checkpoint checksum validation, journal pruning, reopen recovery, and all
  durability-mode resource policies.
- Added test instrumentation for file writes, bytes, file syncs, directory syncs,
  direct-index directory syncs, and logical durability barriers.
- Verification passed: formatting, library/all-target/all-feature tests and
  checks, and the native WASM library check. The controlled benchmark was run
  for the Full-mode persistence workload.

### Verification Results

- Focused engine recovery tests: 7 passed.
- Focused SegmentFiles tests: 8 passed.
- Full library: 131 passed.
- All targets: 131 passed.
- All targets/all features: 160 passed.
- All-feature check: 0 errors, one pre-existing dead-code warning.
- WASM library check: passed.
- Controlled benchmark: 2 warmups, 9 repetitions, 1 iteration, seed
  `56394203049952392`; Full persistence median `44,266,555 ns/op`, p95
  `66,968,322 ns/op`, min `34,615,558`, max `66,968,322`, throughput `22.6/s`.
  The timed sample had unchanged RSS, 36 process CPU ticks, and a
  `6,631,049`-byte filesystem footprint delta.
- Test instrumentation measured one Full WAL file sync and one logical
  durability barrier per transaction, with no per-artifact Full file or direct
  index directory barriers. Data retains file syncs; Relaxed retains none.

### Limitations

- The benchmark's RSS, CPU ticks, and filesystem footprint are process-level
  proxies; it does not count allocations, syscalls, or real power-loss events.
- The WAL/checkpoint protocol is crash/reopen tested with injected fault points,
  but durability across particular filesystem, hardware, and power-loss
  implementations still depends on their `fsync` guarantees.
