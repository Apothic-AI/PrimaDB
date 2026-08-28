# Tranche 1 Benchmark Report

Baseline: `815b2194013cf419c6134060fd57e13bb4ed4af9` (p1-integration-baseline-815b2194) [tree `c213706258834f101970e3611fb1ce594af3b3f3502c9d284b81e88157a4a894`]
Staging: `b0f21beaec75de0bafff944dde1e9d0838540644` (current-staging-b0f21bea) [tree `0a877f5e5deec2540576f73db90dbf61ee3717df4daa0fbc11e4edd9e60b82c6`]
Runner source: `19e5efe6f23e4b78a000a901d119262239a77184`

## Protocol

- Seed: `22567760790700872`
- Warmups: `2`; repetitions: `9`; iterations per repetition: `1`
- Timed values are nanoseconds per operation; setup and warmups are outside timed sections.
- Throughput is `1e9 / median_ns`; p95 is the nearest-rank 95th percentile of repetition medians.
- The same runner source, seed, workload sizes, compiler profile, and process protocol were used for both revisions.

## Environment

| Field | Baseline | Staging |
|---|---|---|
| OS | linux | linux |
| Kernel | Linux vici 7.1.8+deb13-amd64 #1 SMP PREEMPT_DYNAMIC Debian 7.1.8-1~bpo13+1 (2026-08-16) x86_64 GNU/Linux | Linux vici 7.1.8+deb13-amd64 #1 SMP PREEMPT_DYNAMIC Debian 7.1.8-1~bpo13+1 (2026-08-16) x86_64 GNU/Linux |
| CPU | 13th Gen Intel(R) Core(TM) i5-13420H | 13th Gen Intel(R) Core(TM) i5-13420H |
| Rust | rustc 1.95.0 (59807616e 2026-04-14) | rustc 1.95.0 (59807616e 2026-04-14) |
| Cargo | cargo 1.95.0 (f2d3ce0bd 2026-03-21) | cargo 1.95.0 (f2d3ce0bd 2026-03-21) |
| Compiler profile | release | release |
| Features | default (no optional features) | default (no optional features) |
| Governor | powersave | powersave |
| Affinity | cpu-2 | cpu-2 |
| Filesystem | btrfs | btrfs |
| Measured resource proxies | /proc/self/status VmRSS + /proc/self/stat utime+stime; no allocations/syscalls/locks | /proc/self/status VmRSS + /proc/self/stat utime+stime; no allocations/syscalls/locks |

## Summary

| Workload | Baseline median | Staging median | Change | Baseline p95 | Staging p95 | Baseline min-max | Staging min-max | Staging throughput |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `transactions/local/success/small` | 84068 ns | 26597 ns | -68.4% | 88828 ns | 27977 ns | 77293-88828 ns | 25065-27977 ns | 37598.2/s |
| `transactions/local/failure/small` | 11218 ns | 5053 ns | -55.0% | 13399 ns | 6098 ns | 10913-13399 ns | 4314-6098 ns | 197902.2/s |
| `transactions/local/success/large` | 1860830 ns | 2485222 ns | +33.6% | 3012917 ns | 2858345 ns | 1439404-3012917 ns | 1714392-2858345 ns | 402.4/s |
| `transactions/local/failure/large` | 226627 ns | 310583 ns | +37.0% | 252045 ns | 548650 ns | 219895-252045 ns | 223074-548650 ns | 3219.8/s |
| `records/scan/paginated/small` | 782434 ns | 939266 ns | +20.0% | 1284194 ns | 2128008 ns | 551041-1284194 ns | 755682-2128008 ns | 1064.7/s |
| `records/scan/full/small` | 2280757 ns | 2458950 ns | +7.8% | 3431151 ns | 3734405 ns | 2030680-3431151 ns | 2052448-3734405 ns | 406.7/s |
| `records/scan/paginated/large` | 11430068 ns | 9877852 ns | -13.6% | 12717287 ns | 10967468 ns | 10145669-12717287 ns | 8723421-10967468 ns | 101.2/s |
| `records/scan/full/large` | 49009807 ns | 42685873 ns | -12.9% | 52130482 ns | 45736543 ns | 47630893-52130482 ns | 40071688-45736543 ns | 23.4/s |
| `vectors/exact/top-k/1024` | 530268 ns | 15955 ns | -97.0% | 638653 ns | 19392 ns | 364798-638653 ns | 14031-19392 ns | 62676.3/s |
| `vectors/exact/top-k/4096` | 2726554 ns | 146407 ns | -94.6% | 4120746 ns | 3123108 ns | 2178773-4120746 ns | 118578-3123108 ns | 6830.3/s |
| `text/bm25/collection/all/limit-10` | 695817 ns | 524480 ns | -24.6% | 788521 ns | 608865 ns | 658361-788521 ns | 484403-608865 ns | 1906.7/s |
| `text/bm25/collection/half/limit-10` | 465148 ns | 213587 ns | -54.1% | 573169 ns | 248674 ns | 359829-573169 ns | 207468-248674 ns | 4681.9/s |
| `text/bm25/collection/rare/limit-10` | 53118 ns | 21135 ns | -60.2% | 90025 ns | 27457 ns | 40468-90025 ns | 19528-27457 ns | 47314.9/s |
| `text/bm25/collection/rare/limit-50` | 31254 ns | 39382 ns | +26.0% | 35248 ns | 42657 ns | 26581-35248 ns | 37079-42657 ns | 25392.3/s |
| `text/bm25/record-candidates/rare-limit-10` | 6500757 ns | 1844323 ns | -71.6% | 7554768 ns | 2428503 ns | 5442364-7554768 ns | 1518056-2428503 ns | 542.2/s |
| `query/projection-filter-order/indexed` | 40969311 ns | 36302769 ns | -11.4% | 41763921 ns | 41083170 ns | 39093116-41763921 ns | 35081023-41083170 ns | 27.5/s |
| `watchers/equivalent-update-coalescing/8` | 13379 ns | 10838 ns | -19.0% | 16012 ns | 14484 ns | 13026-16012 ns | 10517-14484 ns | 92267.9/s |
| `persistence/segment-writes/full-durability` | 946990269 ns | 1132962574 ns | +19.6% | 1008136863 ns | 1626105614 ns | 870040590-1008136863 ns | 734203434-1626105614 ns | 0.9/s |
| `direct-index/build/roots-64-depth-8-fanout-2` | 484904 ns | 944163 ns | +94.7% | 2469522 ns | 5213180 ns | 459590-2469522 ns | 837955-5213180 ns | 1059.1/s |
| `direct-index/build/roots-256-depth-16-fanout-4` | 4704713 ns | 6318224 ns | +34.3% | 5897713 ns | 7670912 ns | 4165798-5897713 ns | 5492767-7670912 ns | 158.3/s |

## Phase Timings

Setup and verification are one measured pre-operation sample where available. Persistence is reported only for the full-durability operation; omitted phases were not separately measurable without changing the workload.

| Workload | Baseline setup | Staging setup | Baseline verification | Staging verification | Baseline persistence | Staging persistence |
|---|---:|---:|---:|---:|---:|---:|
| `transactions/local/success/small` | 472608 ns (raw: 472608) | 343453 ns (raw: 343453) | not separately measured | not separately measured | not separately measured | not separately measured |
| `transactions/local/failure/small` | 472608 ns (raw: 472608) | 343453 ns (raw: 343453) | not separately measured | not separately measured | not separately measured | not separately measured |
| `transactions/local/success/large` | 23669070 ns (raw: 23669070) | 21700231 ns (raw: 21700231) | not separately measured | not separately measured | not separately measured | not separately measured |
| `transactions/local/failure/large` | 23669070 ns (raw: 23669070) | 21700231 ns (raw: 21700231) | not separately measured | not separately measured | not separately measured | not separately measured |
| `records/scan/paginated/small` | 157491723 ns (raw: 157491723) | 128819642 ns (raw: 128819642) | 6910921 ns (raw: 6910921) | 3289720 ns (raw: 3289720) | not separately measured | not separately measured |
| `records/scan/full/small` | 157491723 ns (raw: 157491723) | 128819642 ns (raw: 128819642) | 6910921 ns (raw: 6910921) | 3289720 ns (raw: 3289720) | not separately measured | not separately measured |
| `records/scan/paginated/large` | 2440926984 ns (raw: 2440926984) | 2737114725 ns (raw: 2737114725) | 65866900 ns (raw: 65866900) | 58606665 ns (raw: 58606665) | not separately measured | not separately measured |
| `records/scan/full/large` | 2440926984 ns (raw: 2440926984) | 2737114725 ns (raw: 2737114725) | 65866900 ns (raw: 65866900) | 58606665 ns (raw: 58606665) | not separately measured | not separately measured |
| `vectors/exact/top-k/1024` | 4262268649 ns (raw: 4262268649) | 3995141715 ns (raw: 3995141715) | 12771397 ns (raw: 12771397) | 11911208 ns (raw: 11911208) | not separately measured | not separately measured |
| `vectors/exact/top-k/4096` | 90215919757 ns (raw: 90215919757) | 82867487975 ns (raw: 82867487975) | 71121825 ns (raw: 71121825) | 254247114 ns (raw: 254247114) | not separately measured | not separately measured |
| `text/bm25/collection/all/limit-10` | 1389072688 ns (raw: 1389072688) | 1961842403 ns (raw: 1961842403) | 8219996 ns (raw: 8219996) | 7158940 ns (raw: 7158940) | not separately measured | not separately measured |
| `text/bm25/collection/half/limit-10` | 1316526259 ns (raw: 1316526259) | 1302103887 ns (raw: 1302103887) | 6497648 ns (raw: 6497648) | 7571152 ns (raw: 7571152) | not separately measured | not separately measured |
| `text/bm25/collection/rare/limit-10` | 1324914166 ns (raw: 1324914166) | 1273901557 ns (raw: 1273901557) | 5586998 ns (raw: 5586998) | 5887286 ns (raw: 5887286) | not separately measured | not separately measured |
| `text/bm25/collection/rare/limit-50` | 1331763310 ns (raw: 1331763310) | 1308610889 ns (raw: 1308610889) | 5034143 ns (raw: 5034143) | 5359160 ns (raw: 5359160) | not separately measured | not separately measured |
| `text/bm25/record-candidates/rare-limit-10` | 9595409 ns (raw: 9595409) | 9938867 ns (raw: 9938867) | 7655327 ns (raw: 7655327) | 1720365 ns (raw: 1720365) | not separately measured | not separately measured |
| `query/projection-filter-order/indexed` | 6276536720 ns (raw: 6276536720) | 4226101641 ns (raw: 4226101641) | 43364550 ns (raw: 43364550) | 51972684 ns (raw: 51972684) | not separately measured | not separately measured |
| `watchers/equivalent-update-coalescing/8` | 111615 ns (raw: 111615) | 102582 ns (raw: 102582) | 7042 ns (raw: 7042) | 6888 ns (raw: 6888) | not separately measured | not separately measured |
| `persistence/segment-writes/full-durability` | 58900179 ns (raw: 58900179) | 59427081 ns (raw: 59427081) | not separately measured | not separately measured | 946827350 ns (raw: 869960507, 888880465, 890551588, 908479958, 946827350, 950917550, 970304176, 993276858, 1008054972) | 1132854287 ns (raw: 734069275, 799005694, 899200629, 1100396115, 1132854287, 1149636649, 1277968458, 1510210590, 1626017373) |
| `direct-index/build/roots-64-depth-8-fanout-2` | 51695 ns (raw: 51695) | 89049 ns (raw: 89049) | not separately measured | not separately measured | not separately measured | not separately measured |
| `direct-index/build/roots-256-depth-16-fanout-4` | 129096 ns (raw: 129096) | 183103 ns (raw: 183103) | not separately measured | not separately measured | not separately measured | not separately measured |

## Raw Samples

Raw repetition medians in nanoseconds per operation, retained to expose variance:

| Workload | Baseline raw samples | Staging raw samples |
|---|---|---|
| `transactions/local/success/small` | `80698, 88828, 84068, 86260, 88386, 84205, 79070, 77742, 77293` | `25228, 27931, 27977, 26422, 27597, 26745, 25405, 26597, 25065` |
| `transactions/local/failure/small` | `13399, 11813, 10913, 10999, 11265, 11034, 11218, 11018, 11358` | `6098, 5244, 5706, 4314, 4466, 5034, 5053, 4552, 5099` |
| `transactions/local/success/large` | `1885644, 1785787, 1669505, 1535723, 1439404, 3012917, 2930677, 2066515, 1860830` | `2858345, 2506156, 2503077, 2485222, 2079404, 1974560, 1714392, 2127632, 2689927` |
| `transactions/local/failure/large` | `239431, 252045, 233154, 228601, 226627, 226114, 223711, 221386, 219895` | `298503, 236289, 223074, 310583, 311834, 533124, 548650, 339646, 288376` |
| `records/scan/paginated/small` | `759688, 1284194, 1219255, 1196414, 1260950, 782434, 570217, 555229, 551041` | `1116393, 1021107, 922266, 2128008, 939266, 755682, 874488, 893179, 940451` |
| `records/scan/full/small` | `2280757, 2141418, 2030680, 3431151, 2296024, 3080376, 2464879, 2247333, 2093303` | `3734405, 2777698, 2491996, 2564618, 2458950, 2206172, 2126114, 2052448, 2326381` |
| `records/scan/paginated/large` | `11430068, 12142297, 11322637, 12364854, 10474811, 10145669, 12717287, 11384603, 11625198` | `8923031, 9877852, 10144620, 8723421, 10967468, 9508646, 10273913, 9953967, 9804419` |
| `records/scan/full/large` | `49263176, 47640648, 50659005, 52130482, 48722363, 49009807, 49826532, 48604202, 47630893` | `43256710, 41504292, 44134009, 42685873, 42249921, 42498147, 45107761, 40071688, 45736543` |
| `vectors/exact/top-k/1024` | `638653, 613861, 597010, 530268, 619873, 445169, 400863, 394627, 364798` | `19392, 14031, 14644, 16970, 19018, 18251, 14962, 15955, 15314` |
| `vectors/exact/top-k/4096` | `3291625, 2604161, 3000200, 2726554, 2664774, 2736693, 4120746, 2499071, 2178773` | `309178, 159448, 133652, 3123108, 320485, 146407, 120989, 118578, 119558` |
| `text/bm25/collection/all/limit-10` | `704964, 695817, 788521, 761511, 712899, 689836, 666781, 658361, 662143` | `608865, 600227, 524480, 569231, 537449, 490172, 484403, 507241, 495550` |
| `text/bm25/collection/half/limit-10` | `573169, 557518, 521243, 418173, 375209, 365926, 359829, 520914, 465148` | `248674, 220005, 213587, 218478, 208721, 207468, 208187, 214560, 210383` |
| `text/bm25/collection/rare/limit-10` | `90025, 62925, 53118, 55643, 54157, 49500, 40468, 47944, 52487` | `27457, 25409, 22136, 21558, 21135, 20210, 20331, 19761, 19528` |
| `text/bm25/collection/rare/limit-50` | `31043, 28425, 26669, 26581, 35248, 33717, 34069, 32657, 31254` | `42290, 40186, 41285, 42657, 38372, 39382, 37996, 37727, 37079` |
| `text/bm25/record-candidates/rare-limit-10` | `6550992, 6143319, 7316299, 5922265, 7544444, 6500757, 5442364, 7554768, 5576631` | `2428503, 2209741, 1836981, 1769367, 1844323, 1668024, 1518056, 2043310, 2389322` |
| `query/projection-filter-order/indexed` | `39556274, 40900879, 41664749, 41267619, 39093116, 41763921, 41218842, 40969311, 40948681` | `36302769, 35986555, 41083170, 36551788, 35081023, 35251213, 39107368, 36905159, 35615080` |
| `watchers/equivalent-update-coalescing/8` | `16012, 14496, 14045, 13152, 13320, 13026, 13684, 13272, 13379` | `14484, 12422, 11771, 11112, 10838, 10645, 10669, 10568, 10517` |
| `persistence/segment-writes/full-durability` | `890638446, 993362603, 870040590, 946990269, 970367954, 908533770, 1008136863, 888969390, 951066210` | `799084585, 734203434, 899267801, 1100503118, 1626105614, 1278067773, 1510320692, 1149755544, 1132962574` |
| `direct-index/build/roots-64-depth-8-fanout-2` | `459692, 487643, 459590, 462887, 473667, 2469522, 521890, 484904, 507044` | `868771, 914535, 873299, 837955, 5213180, 950267, 1088738, 5031215, 944163` |
| `direct-index/build/roots-256-depth-16-fanout-4` | `4704713, 5052986, 4165798, 4669576, 4463235, 5199967, 4378699, 5897713, 5743020` | `6147090, 7670912, 6318224, 6857357, 6315171, 6829054, 5492767, 5704789, 7367906` |

## Resource Proxies

RSS and process CPU are process-level deltas captured after warmup. Filesystem values are footprint deltas, a proxy rather than write-volume accounting.

| Workload | Baseline RSS delta | Staging RSS delta | Baseline CPU ticks | Staging CPU ticks | Baseline filesystem footprint delta | Staging filesystem footprint delta |
|---|---:|---:|---:|---:|---:|---:|
| `transactions/local/success/small` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `transactions/local/failure/small` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `transactions/local/success/large` | 0 KiB | 0 KiB | 2 ticks | 2 ticks | unavailable | unavailable |
| `transactions/local/failure/large` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `records/scan/paginated/small` | 0 KiB | 0 KiB | 1 ticks | 3 ticks | 0 B | 0 B |
| `records/scan/full/small` | 0 KiB | 0 KiB | 3 ticks | 4 ticks | 0 B | 0 B |
| `records/scan/paginated/large` | 0 KiB | 0 KiB | 24 ticks | 19 ticks | 0 B | 0 B |
| `records/scan/full/large` | 0 KiB | 0 KiB | 57 ticks | 49 ticks | 0 B | 0 B |
| `vectors/exact/top-k/1024` | 0 KiB | 0 KiB | 1 ticks | 0 ticks | unavailable | unavailable |
| `vectors/exact/top-k/4096` | 0 KiB | 0 KiB | 3 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/collection/all/limit-10` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/collection/half/limit-10` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/collection/rare/limit-10` | 0 KiB | 0 KiB | 1 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/collection/rare/limit-50` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/record-candidates/rare-limit-10` | 0 KiB | 0 KiB | 6 ticks | 2 ticks | unavailable | unavailable |
| `query/projection-filter-order/indexed` | 0 KiB | 0 KiB | 44 ticks | 39 ticks | 0 B | 0 B |
| `watchers/equivalent-update-coalescing/8` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `persistence/segment-writes/full-durability` | 0 KiB | 0 KiB | 268 ticks | 248 ticks | 5699669 B | 5699515 B |
| `direct-index/build/roots-64-depth-8-fanout-2` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `direct-index/build/roots-256-depth-16-fanout-4` | 0 KiB | 0 KiB | 5 ticks | 6 ticks | unavailable | unavailable |

## Interpretation

- The summary and phase tables are descriptive comparisons of the measured workloads; they do not attribute a change to a single production optimization.
- Interpret medians together with p95, min/max, raw samples, and the separately reported setup, verification, and persistence phases. Large spread or a phase dominated by setup/persistence weakens an operation-only conclusion.
- No confidence intervals or hypothesis tests are calculated. These nine-repetition results should not be treated as statistically significant claims or as evidence that unmeasured counters changed.

## Limitations

- Allocation counts, opened-file counts, fsync/syscall counts, and mutex hold time were not available without changing production code or adding an external tracing tool; they are not fabricated here.
- RSS and `/proc/self/stat` CPU ticks are process-level proxies and include benchmark-side orchestration outside the timed operation; they are not per-allocation or per-lock counters.
- Filesystem footprint delta does not equal bytes written when files are overwritten, and no cache drop or machine-idle guarantee is asserted.
- Confidence is limited by nine repetitions per workload and the host's available scheduling controls; use the raw samples and p95 rather than single medians alone.
