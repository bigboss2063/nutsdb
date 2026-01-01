# Merge Benchmarks

## Purpose
Track merge performance for BTree-only workloads under concurrent read/write load using KV sizing aligned to common Badger-style benchmarks. Results include merge diagnostics (peak lookup bytes, max lock hold, batch count) and concurrent load metrics (throughput and latency).

## Scenario Matrix

### Workloads
| workload | readers | writers | description |
| --- | --- | --- | --- |
| baseline | 0 | 0 | merge only |
| read | 2 | 0 | continuous Get on a hot keyset |
| write | 0 | 1 | continuous Put on a hot keyset |
| readwrite | 2 | 1 | concurrent Get and Put |

### Datasets
| dataset | entries | key size | value size | segment size | notes |
| --- | --- | --- | --- | --- | --- |
| small-100k-128b | 100,000 | 16B | 128B | 8MB | best-practice baseline |
| medium-500k-128b | 500,000 | 16B | 128B | 16MB | scale check |
| large-1m-512b | 1,000,000 | 16B | 512B | 32MB | long-run profile |

### Data Mix
- Initial load: all keys inserted once
- Update phase: first ~33% of keys overwritten
- Delete phase: next ~10% of keys deleted
- Hotset: ~1% of keys (capped at 10,000) used by concurrent load

### Modes
- BTree only
- Hint file enabled and disabled

## New Benchmark Scenarios

### Update Pattern Scenarios

Test merge performance with different update patterns to identify bottlenecks in specific workload scenarios.

#### Patterns
| pattern | description | overwrites | deletes | appends |
| --- | --- | --- | --- | --- |
| overwrite-heavy | High percentage of key overwrites | 70% | 0% | 0% |
| delete-heavy | High percentage of key deletions | 0% | 50% | 0% |
| append-only | Only new key insertions | 0% | 0% | 100% |
| mixed | Balanced mix of operations | 30% | 20% | 50% |

#### Datasets
Same as standard benchmarks (small-100k, medium-500k, large-1m)

#### Metrics
- All standard metrics plus `reduction_ratio_pct` to measure data compaction effectiveness

### Memory Budget Scenarios

Test merge performance with different memory budget configurations to identify optimal settings.

#### Memory Budgets
| budget | size | description |
| --- | --- | --- |
| 16MB | 16 * 1024 * 1024 | Constrained memory environment |
| 64MB | 64 * 1024 * 1024 | Default configuration |
| 256MB | 256 * 1024 * 1024 | High memory environment |
| 1GB | 1024 * 1024 * 1024 | Maximum memory environment |

#### Datasets
Same as standard benchmarks (small-100k, medium-500k, large-1m)

#### Metrics
- All standard metrics plus `bytes_per_mb_budget` to measure memory efficiency
- `batch_count` increases as memory budget decreases
- Optimal budget identification based on duration/memory balance

### Concurrent Interaction Scenarios

Test merge performance with concurrent read/write load to measure latency impact and throughput degradation.

#### Concurrency Scenarios
| scenario | readers | writers | description |
| --- | --- | --- | --- |
| read-heavy | 4 | 0 | High concurrent read load |
| write-heavy | 0 | 2 | High concurrent write load |
| balanced | 2 | 2 | Balanced read/write load |

#### Datasets
Same as standard benchmarks (small-100k, medium-500k, large-1m)

#### Metrics
- All standard metrics plus:
  - `read_p50_us`, `read_p99_us`: Additional latency percentiles
  - `write_p50_us`, `write_p99_us`: Additional latency percentiles
  - `throughput_degradation_pct`: Throughput degradation compared to baseline
  - `baseline_ops_per_sec`: Baseline throughput without merge
  - `merge_ops_per_sec`: Throughput during merge

#### Baseline Measurement
Each concurrent scenario first measures baseline throughput without merge (3 seconds), then measures throughput during merge to calculate degradation percentage.

## Metrics
- `ms/op`: time per merge (derived from `ns/op`)
- `MiB/op`: memory allocated per merge (derived from `B/op`)
- `allocs/op`: allocations per merge
- `peak_lookup_bytes`: peak in-flight lookup memory tracked during commit
- `max_lock_hold_us`: max exclusive lock hold time per commit batch (proxy for blocking time)
- `batch_count`: average number of commit batches per merge
- `reduction_ratio_pct`: data reduction ratio as percentage ((original - final) / original * 100)
- `bytes_per_mb_budget`: memory efficiency (bytes processed per MB of memory budget)
- `read_ops_per_sec`: read throughput during merge (hotset reads)
- `read_p50_us`: p50 read latency during merge (sampled)
- `read_p95_us`: p95 read latency during merge (sampled)
- `read_p99_us`: p99 read latency during merge (sampled)
- `read_max_us`: max read latency during merge (sampled)
- `write_ops_per_sec`: write throughput during merge (hotset updates)
- `write_p50_us`: p50 write latency during merge (sampled)
- `write_p95_us`: p95 write latency during merge (sampled)
- `write_p99_us`: p99 write latency during merge (sampled)
- `write_max_us`: max write latency during merge (sampled)
- `throughput_degradation_pct`: throughput degradation percentage compared to baseline
- `baseline_ops_per_sec`: baseline throughput without merge
- `merge_ops_per_sec`: throughput during merge
- `degradation_pct`: throughput degradation percentage

Note: Read/write latency metrics are sampled every 100 ops to limit overhead. Concurrent load begins when merge starts, so throughput and latency reflect the merge window.

## How To Run

### Standard Merge Benchmarks
```bash
GOCACHE=$PWD/.gocache-mergebench go test -run '^$' -bench BenchmarkMerge -benchmem -count=1
```

### Update Pattern Benchmarks
Test merge performance with different update patterns (overwrite-heavy, delete-heavy, append-only, mixed):
```bash
go test -run '^$' -bench BenchmarkMergePatterns -benchmem -count=1
```

### Memory Budget Benchmarks
Test merge performance with different memory budget configurations (16MB, 64MB, 256MB, 1GB):
```bash
go test -run '^$' -bench BenchmarkMergeMemoryBudget -benchmem -count=1
```

Identify optimal memory budget for each dataset:
```bash
go test -run '^$' -bench BenchmarkMergeMemoryOptimal -benchmem -count=1
```

### Concurrent Interaction Benchmarks
Test merge performance with concurrent read/write load (read-heavy, write-heavy, balanced):
```bash
go test -run '^$' -bench BenchmarkMergeConcurrent -benchmem -count=1
```

Measure throughput degradation during merge:
```bash
go test -run '^$' -bench BenchmarkMergeConcurrentDegradation -benchmem -count=1
```

### Run All Benchmarks
```bash
go test -run '^$' -bench 'BenchmarkMerge.*' -benchmem -count=1
```

## Runs

### Baseline (pre-optimization)
- Date: 2025-12-31 14:26:22 UTC
- Commit: 100659c
- Go: go1.25.3 linux/amd64
- CPU: AMD EPYC 7K62 48-Core Processor
- Command: `GOCACHE=$PWD/.gocache-mergebench go test -run '^$' -bench Merge -benchmem -count=1`
- Options: `MergeLookupMemoryBudget=64MB`, `MergeLookupBatchSize=2048`, `RWMode=FileIO`, `EntryIdxMode=HintKeyValAndRAMIdxMode`

### small-100k-128b (hint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 461.78 | 44.82 | 573601 | 1024 | 285.70 | 1.00 |
| read | 595.04 | 181.06 | 3945297 | 953 | 297.50 | 1.00 |
| write | 511.38 | 45.22 | 578923 | 988 | 679.00 | 1.00 |
| readwrite | 502.59 | 45.58 | 587447 | 1059 | 287.30 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 706108 | 4 | 10625 | - | - | - |
| write | - | - | - | 650 | 2466 | 3047 |
| readwrite | 2090 | 3164 | 9093 | 676 | 1749 | 2048 |

### small-100k-128b (nohint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 464.11 | 44.73 | 573566 | 1059 | 319.30 | 1.00 |
| read | 578.14 | 190.56 | 4181823 | 1006 | 301.00 | 1.00 |
| write | 492.69 | 45.14 | 578904 | 1024 | 290.70 | 1.00 |
| readwrite | 490.31 | 45.36 | 584193 | 1059 | 318.30 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 777671 | 4 | 10563 | - | - | - |
| write | - | - | - | 677 | 2126 | 6898 |
| readwrite | 1386 | 3022 | 6977 | 662 | 1882 | 3233 |

### medium-500k-128b (hint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 2365.51 | 223.69 | 2867375 | 4875 | 400.00 | 1.00 |
| read | 2839.29 | 901.07 | 19626480 | 4875 | 531.00 | 1.00 |
| write | 2459.43 | 225.62 | 2893164 | 4769 | 410.00 | 1.00 |
| readwrite | 2461.83 | 227.69 | 2943800 | 4769 | 426.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 735441 | 4 | 6693 | - | - | - |
| write | - | - | - | 655 | 1781 | 2359 |
| readwrite | 2535 | 3195 | 33034 | 671 | 1916 | 12457 |

### medium-500k-128b (nohint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 2314.45 | 223.60 | 2867227 | 4769 | 486.00 | 1.00 |
| read | 2911.81 | 863.96 | 18709598 | 4875 | 580.00 | 1.00 |
| write | 2414.20 | 225.60 | 2893881 | 4769 | 414.00 | 1.00 |
| readwrite | 2463.96 | 226.72 | 2922230 | 4875 | 500.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 677887 | 4 | 18580 | - | - | - |
| write | - | - | - | 690 | 1674 | 2597 |
| readwrite | 1483 | 3577 | 11463 | 653 | 2066 | 11649 |

### large-1m-512b (hint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 5576.26 | 996.70 | 5734829 | 9706 | 710.00 | 1.00 |
| read | 8445.39 | 2480.45 | 42569894 | 9706 | 688.00 | 1.00 |
| write | 6003.50 | 1001.48 | 5782515 | 9706 | 667.00 | 1.00 |
| readwrite | 6162.23 | 1008.09 | 5934836 | 9706 | 632.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 544099 | 3 | 33388 | - | - | - |
| write | - | - | - | 496 | 4275 | 31012 |
| readwrite | 2925 | 3611 | 17973 | 565 | 3474 | 14820 |

### large-1m-512b (nohint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 5498.00 | 996.60 | 5734546 | 9706 | 686.00 | 1.00 |
| read | 6768.01 | 2434.20 | 41299003 | 9706 | 667.00 | 1.00 |
| write | 6190.95 | 1002.54 | 5793726 | 9599 | 648.00 | 1.00 |
| readwrite | 5976.33 | 1005.16 | 5862372 | 9599 | 606.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 654704 | 3 | 18637 | - | - | - |
| write | - | - | - | 598 | 2259 | 10801 |
| readwrite | 1485 | 3920 | 15179 | 594 | 2403 | 17893 |

### Optimized (post-optimization)
- Date: 2025-12-31 14:29:30 UTC
- Commit: 9f9deff
- Go: go1.25.3 linux/amd64
- CPU: AMD EPYC 7K62 48-Core Processor
- Command: `GOCACHE=$PWD/.gocache-mergebench go test -run '^$' -bench Merge -benchmem -count=1`
- Options: `MergeLookupMemoryBudget=64MB`, `MergeLookupBatchSize=2048`, `RWMode=FileIO`, `EntryIdxMode=HintKeyValAndRAMIdxMode`

### small-100k-128b (hint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 469.27 | 44.82 | 573617 | 1024 | 32.00 | 1.00 |
| read | 634.03 | 184.97 | 4044126 | 1059 | 26.50 | 1.00 |
| write | 505.55 | 45.21 | 578732 | 1094 | 60.33 | 1.00 |
| readwrite | 513.44 | 45.95 | 596827 | 1059 | 49.33 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 682262 | 4 | 13455 | - | - | - |
| write | - | - | - | 631 | 2123 | 3133 |
| readwrite | 4421 | 2831 | 10331 | 608 | 2207 | 2687 |

### small-100k-128b (nohint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 460.58 | 44.74 | 573622 | 1024 | 40.33 | 1.00 |
| read | 573.20 | 186.70 | 4086659 | 1059 | 46.00 | 1.00 |
| write | 493.73 | 45.14 | 578830 | 1059 | 39.67 | 1.00 |
| readwrite | 528.67 | 45.40 | 584904 | 1024 | 42.33 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 763729 | 4 | 10284 | - | - | - |
| write | - | - | - | 658 | 1744 | 2075 |
| readwrite | 1392 | 3464 | 26011 | 637 | 2598 | 7574 |

### medium-500k-128b (hint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 2284.77 | 223.70 | 2867418 | 4769 | 177.00 | 1.00 |
| read | 3031.97 | 906.22 | 19752211 | 4769 | 168.00 | 1.00 |
| write | 2430.26 | 225.71 | 2894280 | 4875 | 181.00 | 1.00 |
| readwrite | 2518.55 | 226.80 | 2921511 | 4769 | 166.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 693846 | 4 | 10401 | - | - | - |
| write | - | - | - | 690 | 2430 | 15807 |
| readwrite | 1370 | 3143 | 5692 | 657 | 2131 | 10180 |

### medium-500k-128b (nohint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 2305.18 | 223.62 | 2867469 | 4875 | 175.00 | 1.00 |
| read | 2938.00 | 894.95 | 19470791 | 4875 | 181.00 | 1.00 |
| write | 2612.53 | 225.45 | 2891894 | 4769 | 411.00 | 1.00 |
| readwrite | 2448.85 | 228.82 | 2974759 | 4769 | 174.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 704030 | 4 | 10384 | - | - | - |
| write | - | - | - | 584 | 2205 | 8098 |
| readwrite | 4178 | 2688 | 5389 | 649 | 2409 | 4423 |

### large-1m-512b (hint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 5341.38 | 996.70 | 5734924 | 9706 | 339.00 | 1.00 |
| read | 6871.57 | 2463.91 | 42019742 | 9599 | 381.00 | 1.00 |
| write | 5832.01 | 1002.48 | 5792465 | 9706 | 387.00 | 1.00 |
| readwrite | 6030.81 | 1005.56 | 5868158 | 9492 | 365.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 657818 | 4 | 16020 | - | - | - |
| write | - | - | - | 617 | 2267 | 10534 |
| readwrite | 1557 | 3578 | 27328 | 602 | 2299 | 14415 |

### large-1m-512b (nohint)
| workload | ms/op | MiB/op | allocs/op | peak_lookup_bytes | max_lock_hold_us | batch_count |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 5588.08 | 996.68 | 5735003 | 9599 | 419.00 | 1.00 |
| read | 6716.82 | 2411.99 | 40740143 | 9599 | 376.00 | 1.00 |
| write | 5521.41 | 1002.18 | 5789899 | 9706 | 384.00 | 1.00 |
| readwrite | 5573.85 | 1004.58 | 5848418 | 9706 | 397.00 | 1.00 |

| workload | read_ops_per_sec | read_p95_us | read_max_us | write_ops_per_sec | write_p95_us | write_max_us |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | - | - | - | - | - | - |
| read | 649264 | 3 | 14031 | - | - | - |
| write | - | - | - | 621 | 2216 | 11230 |
| readwrite | 1297 | 3980 | 28064 | 623 | 2426 | 10894 |

### Legacy Merge Benchmarks
From `hintfile_bench_test.go` (not part of the new matrix):

Baseline:
- `BenchmarkMergeWithHintFile-4`: 3595.87 ms/op, 9.78 MiB/op, 77232 allocs/op
- `BenchmarkMergeWithoutHintFile-4`: 3667.21 ms/op, 9.41 MiB/op, 72624 allocs/op

Optimized:
- `BenchmarkMergeWithHintFile-4`: 3543.87 ms/op, 9.80 MiB/op, 78747 allocs/op
- `BenchmarkMergeWithoutHintFile-4`: 3521.14 ms/op, 9.74 MiB/op, 80139 allocs/op
