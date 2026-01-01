# Merge Evolution Benchmark Analysis

## Run Details
- Command: `GOCACHE=$(pwd)/.gocache go test -run '^$' -bench Merge -benchmem -race -count=1 ./...`
- Output: `openspec/changes/merge-evolution/bench-results.txt`
- Host: goos linux, goarch amd64, CPU Intel(R) Core(TM) i5-14600KF
- Duration: 845.787s

## Summary
- All merge benchmark suites completed without failures.
- Hint vs no-hint baseline merge durations are similar (~68-69 ms).
- Reduction ratios align with dataset size and update patterns (small ~66.67-80%, medium ~83.33-90.91%, large ~94.44-97.06%).

## Memory Budget Sensitivity
- Batch count stayed at 1 across 16MB-1GB budgets for all dataset sizes, and durations were effectively flat (small ~0.44s, medium ~2.24s, large ~4.85s).
- Bytes processed per MB budget decreases as budget increases:
  - small-100k: 1,572,864 bytes/MB (16MB) -> 24,576 bytes/MB (1GB)
  - large-1m: 46,137,344 bytes/MB (16MB) -> 720,896 bytes/MB (1GB)
- Peak lookup bytes did not change with budget (953 small, 4769 medium, 9599 large).
- Optimal budget logs (fresh data per scenario): small-100k -> 64MB, medium-500k -> 1GB, large-1m -> 64MB.
- Conclusion: no clear memory bottleneck under these datasets; budgets are not currently constraining merge.

## Concurrent Load Interaction
- Throughput degradation is highest for write-heavy loads:
  - Concurrent benchmark: small 48.89%, medium 39.43%, large 28.94%.
  - Degradation benchmark: read-heavy 3.59%, write-heavy 39.42%, balanced 2.93%.
- Latency percentiles are stable: read p99 ~88-110 us, write p99 ~876-1155 us; max write latency peaked around 4.5 ms in large balanced scenario.
- Lock hold time scales with dataset size (max_lock_hold_us ~0.85-0.95 ms small, ~4.7 ms medium, ~9.5 ms large).

## Update Pattern Observations
- Append-only patterns show the highest lock hold times (large append-only: ~21.3 ms max_lock_hold_us).
- Overwrite-heavy patterns remain lower than append-only (large overwrite-heavy: ~10.7 ms max_lock_hold_us).

## Potential Optimization Targets
- Focus on write-heavy concurrency degradation; this remains the largest throughput loss.
- Investigate lock hold spikes in append-only patterns, especially on large datasets.

## Next Steps
- Only if needed: run a larger dataset or smaller memory budget to force batch_count > 1 to validate memory budget handling.
- Decide whether to prioritize lock contention improvements or write-heavy throughput optimizations before task 11.
