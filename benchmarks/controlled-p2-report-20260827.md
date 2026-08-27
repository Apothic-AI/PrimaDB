# Controlled P2 Benchmark Report

Baseline: `1e00d93f` (pre-p2-baseline)
Staging: `eec33f7c` (p2-staging)

## Protocol

- Seed: `56394203049952392`
- Warmups: `2`; repetitions: `9`; iterations per repetition: `1`
- Timed values are nanoseconds per operation; setup and warmups are outside timed sections.
- Throughput is `1e9 / median_ns`; p95 is the nearest-rank 95th percentile of repetition medians.
- The same binary source, seed, workload sizes, compiler profile, and process protocol were used for both revisions.

## Environment

| Field | Baseline | Staging |
|---|---|---|
| OS | linux | linux |
| Kernel | Linux 7.1.8+deb13-amd64 x86_64 GNU/Linux | Linux 7.1.8+deb13-amd64 x86_64 GNU/Linux |
| CPU | 13th Gen Intel(R) Core(TM) i5-13420H | 13th Gen Intel(R) Core(TM) i5-13420H |
| Rust | rustc 1.95.0 (59807616e 2026-04-14) | rustc 1.95.0 (59807616 2026-04-14) |
| Cargo | cargo 1.95.0 (f2d3ce0bd 2026-03-21) | cargo 1.95.0 (f2d3ce0bd 2026-03-21) |
| Compiler profile | release | release |
| Features | default | default |
| Governor | performance | performance |
| Affinity | cpu-2 | cpu-2 |
| Filesystem | btrfs | btrfs |
| Counters | /proc/self/status VmRSS + /proc/self/stat utime+stime; no allocations/syscalls/locks | /proc/self/status VmRSS + /proc/self/stat utime+stime; no allocations/syscalls/locks |

## Summary

| Workload | Baseline median | Staging median | Change | Baseline p95 | Staging p95 | Baseline min-max | Staging min-max | Staging throughput |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `transactions/local/success/small` | 21746 ns | 21303 ns | -2.0% | 28809 ns | 23876 ns | 21120-28809 ns | 20235-23876 ns | 46941.7/s |
| `transactions/local/failure/small` | 5241 ns | 2562 ns | -51.1% | 5805 ns | 3098 ns | 3375-5805 ns | 2501-3098 ns | 390320.1/s |
| `transactions/local/success/large` | 7247781 ns | 1864913 ns | -74.3% | 51420443 ns | 3453940 ns | 2324311-51420443 ns | 1356005-3453940 ns | 536.2/s |
| `transactions/local/failure/large` | 620308 ns | 212674 ns | -65.7% | 22932745 ns | 298423 ns | 382310-22932745 ns | 201609-298423 ns | 4702.0/s |
| `records/scan/paginated/small` | 1059017 ns | 660870 ns | -37.6% | 2350693 ns | 839385 ns | 729600-2350693 ns | 548407-839385 ns | 1513.2/s |
| `records/scan/full/small` | 4691525 ns | 2169212 ns | -53.8% | 7900054 ns | 3149838 ns | 3959877-7900054 ns | 1975685-3149838 ns | 461.0/s |
| `records/scan/paginated/large` | 9634033 ns | 10552677 ns | +9.5% | 10975974 ns | 12125285 ns | 9238816-10975974 ns | 9256036-12125285 ns | 94.8/s |
| `records/scan/full/large` | 42603133 ns | 44436793 ns | +4.3% | 49033054 ns | 51042583 ns | 39641472-49033054 ns | 42836108-51042583 ns | 22.5/s |
| `vectors/exact/top-k/1024` | 9685 ns | 9834 ns | +1.5% | 11498 ns | 20376 ns | 9357-11498 ns | 9680-20376 ns | 101688.0/s |
| `vectors/exact/top-k/4096` | 82387 ns | 90284 ns | +9.6% | 90671 ns | 155075 ns | 73044-90671 ns | 88771-155075 ns | 11076.2/s |
| `text/bm25/collection/all/limit-10` | 499444 ns | 563537 ns | +12.8% | 617503 ns | 937141 ns | 429993-617503 ns | 485346-937141 ns | 1774.5/s |
| `text/bm25/collection/half/limit-10` | 213850 ns | 284650 ns | +33.1% | 358043 ns | 388052 ns | 199956-358043 ns | 190718-388052 ns | 3513.1/s |
| `text/bm25/collection/rare/limit-10` | 31554 ns | 25703 ns | -18.5% | 41117 ns | 41810 ns | 29605-41117 ns | 22667-41810 ns | 38906.0/s |
| `text/bm25/collection/rare/limit-50` | 34166 ns | 36101 ns | +5.7% | 48310 ns | 44710 ns | 31323-48310 ns | 32455-44710 ns | 27700.1/s |
| `text/bm25/record-candidates/rare-limit-10` | 1801899 ns | 1755531 ns | -2.6% | 3178939 ns | 3971892 ns | 1352251-3178939 ns | 1443186-3971892 ns | 569.6/s |
| `query/projection-filter-order/indexed` | 36177816 ns | 40933305 ns | +13.1% | 37955096 ns | 51643677 ns | 33318722-37955096 ns | 36405097-51643677 ns | 24.4/s |
| `watchers/equivalent-update-coalescing/8` | 8469 ns | 8168 ns | -3.6% | 10154 ns | 10337 ns | 8179-10154 ns | 7847-10337 ns | 122429.0/s |
| `persistence/segment-writes/full-durability` | 498713965 ns | 732365685 ns | +46.9% | 616339736 ns | 1223411743 ns | 447497379-616339736 ns | 598050756-1223411743 ns | 1.4/s |
| `direct-index/build/roots-64-depth-8-fanout-2` | 550881 ns | 668156 ns | +21.3% | 1431075 ns | 891241 ns | 496622-1431075 ns | 529574-891241 ns | 1496.7/s |
| `direct-index/build/roots-256-depth-16-fanout-4` | 5176730 ns | 6119592 ns | +18.2% | 5948625 ns | 7567617 ns | 4501451-5948625 ns | 4727269-7567617 ns | 163.4/s |

## Raw Samples

Raw repetition medians in nanoseconds per operation, retained to expose variance:

| Workload | Baseline raw samples | Staging raw samples |
|---|---|---|
| `transactions/local/success/small` | `21120, 21391, 21487, 21574, 21746, 22611, 26578, 27983, 28809` | `20235, 20725, 20761, 20989, 21303, 22022, 22212, 22857, 23876` |
| `transactions/local/failure/small` | `3375, 4229, 4984, 5068, 5241, 5362, 5588, 5724, 5805` | `2501, 2515, 2525, 2550, 2562, 2599, 2603, 2704, 3098` |
| `transactions/local/success/large` | `2324311, 2732292, 3345624, 3820787, 7247781, 26572385, 47224263, 48125648, 51420443` | `1356005, 1616802, 1724548, 1782429, 1864913, 2531541, 2656265, 3164411, 3453940` |
| `transactions/local/failure/large` | `382310, 531889, 532383, 612923, 620308, 714032, 6412215, 7220485, 22932745` | `201609, 202742, 203903, 204280, 212674, 213095, 216281, 218932, 298423` |
| `records/scan/paginated/small` | `729600, 823508, 825258, 913472, 1059017, 1092599, 1230528, 2328934, 2350693` | `548407, 569512, 591909, 639876, 660870, 681251, 705560, 797825, 839385` |
| `records/scan/full/small` | `3959877, 4089224, 4274383, 4285365, 4691525, 5097256, 5395021, 7518593, 7900054` | `1975685, 2006210, 2071945, 2124731, 2169212, 2296424, 2406159, 2641054, 3149838` |
| `records/scan/paginated/large` | `9238816, 9414060, 9481696, 9483073, 9634033, 9753424, 9932246, 10633299, 10975974` | `9256036, 9354271, 9804407, 9999547, 10552677, 10753367, 11325424, 11855427, 12125285` |
| `records/scan/full/large` | `39641472, 41690818, 41830438, 41957713, 42603133, 43506943, 43737197, 46470635, 49033054` | `42836108, 42870273, 43924982, 44144367, 44436793, 44641204, 46559658, 50962644, 51042583` |
| `vectors/exact/top-k/1024` | `9357, 9401, 9541, 9623, 9685, 9704, 9827, 10337, 11498` | `9680, 9759, 9782, 9817, 9834, 9850, 10008, 10386, 20376` |
| `vectors/exact/top-k/4096` | `73044, 73761, 78383, 78638, 82387, 82827, 84205, 84923, 90671` | `88771, 89287, 89676, 89735, 90284, 91573, 114632, 119295, 155075` |
| `text/bm25/collection/all/limit-10` | `429993, 437271, 463741, 493068, 499444, 501124, 507183, 530722, 617503` | `485346, 503499, 522271, 528857, 563537, 599473, 684198, 819706, 937141` |
| `text/bm25/collection/half/limit-10` | `199956, 202218, 203621, 212222, 213850, 232132, 237403, 244212, 358043` | `190718, 204035, 204489, 231020, 284650, 288316, 294827, 298848, 388052` |
| `text/bm25/collection/rare/limit-10` | `29605, 29640, 30703, 31481, 31554, 31785, 33066, 33968, 41117` | `22667, 23479, 23720, 25630, 25703, 26920, 26984, 27862, 41810` |
| `text/bm25/collection/rare/limit-50` | `31323, 31401, 31544, 33419, 34166, 34830, 35358, 35864, 48310` | `32455, 34020, 34197, 34657, 36101, 37593, 38799, 42713, 44710` |
| `text/bm25/record-candidates/rare-limit-10` | `1352251, 1430921, 1533284, 1750845, 1801899, 2125048, 2825576, 3114835, 3178939` | `1443186, 1583535, 1692219, 1719295, 1755531, 2446618, 2549497, 2733596, 3971892` |
| `query/projection-filter-order/indexed` | `33318722, 34990624, 35080528, 35784908, 36177816, 36256585, 37024055, 37525863, 37955096` | `36405097, 38442028, 38656663, 40818270, 40933305, 41208338, 41461188, 45564776, 51643677` |
| `watchers/equivalent-update-coalescing/8` | `8179, 8301, 8323, 8340, 8469, 8589, 8817, 9097, 10154` | `7847, 7990, 7996, 8156, 8168, 8573, 8650, 8675, 10337` |
| `persistence/segment-writes/full-durability` | `447497379, 464427004, 465684267, 482899844, 498713965, 499559366, 500334741, 517639049, 616339736` | `598050756, 626195538, 631661973, 713129316, 732365685, 1047568102, 1060766316, 1112592583, 1223411743` |
| `direct-index/build/roots-64-depth-8-fanout-2` | `496622, 498980, 524388, 532659, 550881, 560905, 595933, 648153, 1431075` | `529574, 534944, 569778, 646393, 668156, 688148, 721870, 758898, 891241` |
| `direct-index/build/roots-256-depth-16-fanout-4` | `4501451, 4628359, 4761536, 5120605, 5176730, 5250687, 5286139, 5473095, 5948625` | `4727269, 5160025, 5395864, 5447729, 6119592, 6201012, 6722408, 7016297, 7567617` |

## Resource Counters

RSS and process CPU are process-level deltas captured after warmup. Filesystem values are footprint deltas, a proxy rather than write-volume accounting.

| Workload | Baseline RSS delta | Staging RSS delta | Baseline CPU ticks | Staging CPU ticks | Baseline filesystem footprint delta | Staging filesystem footprint delta |
|---|---:|---:|---:|---:|---:|---:|
| `transactions/local/success/small` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `transactions/local/failure/small` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `transactions/local/success/large` | 0 KiB | -1940 KiB | 4 ticks | 2 ticks | unavailable | unavailable |
| `transactions/local/failure/large` | 0 KiB | 0 KiB | 1 ticks | 0 ticks | unavailable | unavailable |
| `records/scan/paginated/small` | 0 KiB | 0 KiB | 2 ticks | 1 ticks | 0 B | 0 B |
| `records/scan/full/small` | 0 KiB | 0 KiB | 4 ticks | 3 ticks | 0 B | 0 B |
| `records/scan/paginated/large` | 0 KiB | 0 KiB | 19 ticks | 20 ticks | 0 B | 0 B |
| `records/scan/full/large` | 0 KiB | 0 KiB | 49 ticks | 52 ticks | 0 B | 0 B |
| `vectors/exact/top-k/1024` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `vectors/exact/top-k/4096` | 0 KiB | 0 KiB | 0 ticks | 1 ticks | unavailable | unavailable |
| `text/bm25/collection/all/limit-10` | 0 KiB | 0 KiB | 0 ticks | 1 ticks | unavailable | unavailable |
| `text/bm25/collection/half/limit-10` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/collection/rare/limit-10` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/collection/rare/limit-50` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `text/bm25/record-candidates/rare-limit-10` | 0 KiB | 0 KiB | 2 ticks | 2 ticks | unavailable | unavailable |
| `query/projection-filter-order/indexed` | 0 KiB | 0 KiB | 39 ticks | 45 ticks | 0 B | 0 B |
| `watchers/equivalent-update-coalescing/8` | 0 KiB | 0 KiB | 0 ticks | 0 ticks | unavailable | unavailable |
| `persistence/segment-writes/full-durability` | 0 KiB | 0 KiB | 114 ticks | 131 ticks | 5699904 B | 5701066 B |
| `direct-index/build/roots-64-depth-8-fanout-2` | 0 KiB | 0 KiB | 0 ticks | 1 ticks | unavailable | unavailable |
| `direct-index/build/roots-256-depth-16-fanout-4` | 0 KiB | 0 KiB | 5 ticks | 5 ticks | unavailable | unavailable |

## Interpretation

- Transactions: staging changed small successful transactions by -2.0% and large successful transactions by -74.3%; failed transactions changed by -51.1% (small) and -65.7% (large). The large baseline also contains 22-51 ms outliers versus a 0.2-3.5 ms staging range, so the prior apparent transaction regression is not reproduced here.
- Direct-index construction was 21.3% slower for the 64-root graph and 18.2% slower for the 256-root graph. This pass therefore finds no direct-index memoization speedup relative to the requested baseline; the result is descriptive and may reflect the cost mix of this synthetic shared-chain workload.
- BM25 collection search was slower for all-hit (+12.8%) and half-hit (+33.1%) limit-10 cases, faster for rare-hit limit-10 (-18.5%), and nearly unchanged for rare-hit limit-50 (+5.7%). The collection optimization is therefore workload-dependent rather than an across-the-board improvement; record-candidate search changed by -2.6% but had a wider staging p95.
- Full durability was 46.9% slower (0.50 s to 0.73 s median) and remains the dominant cost. Large full scans were 44-49 ms, while indexed query projection/filter/order was 36-41 ms; small scans improved substantially but large scans did not. Persistence and scans remain dominant for their native workloads.
- Statistical confidence: these are nine independent repetition medians per revision with no confidence intervals or hypothesis test. The conclusions are directional observations supported by median, p95, min/max, and raw samples; they should not be treated as statistically significant claims.

## Limitations

- Allocation counts, opened-file counts, fsync/syscall counts, and mutex hold time were not available without changing production code or adding an external tracing tool; they are not fabricated here.
- RSS and `/proc/self/stat` CPU ticks are process-level proxies and include benchmark-side orchestration outside the timed operation; they are not per-allocation or per-lock counters.
- Filesystem footprint delta does not equal bytes written when files are overwritten, and no cache drop or machine-idle guarantee is asserted.
- Confidence is limited by nine repetitions per workload and the host's available scheduling controls; use the raw samples and p95 rather than single medians alone.
