# Implementation Plan: Merge Evolution

## Overview

This implementation plan focuses on enhancing NutsDB's Merge mechanism through comprehensive testing and targeted optimizations. The approach is incremental: first establish the benchmark framework, then collect detailed metrics, and finally implement data-driven optimizations.

## Tasks

- [x] 1. Implement update pattern generator
  - [x] 1.1 Create UpdatePatternConfig struct and constants
    - Define UpdatePattern type and constants (overwrite-heavy, delete-heavy, append-only, mixed)
    - Define UpdatePatternConfig with percentage fields
    - _Requirements: 1.1_
  
  - [x] 1.2 Implement pattern application logic
    - Implement overwrite-heavy pattern (60-80% overwrites)
    - Implement delete-heavy pattern (40-60% deletes)
    - Implement append-only pattern (no updates/deletes)
    - Implement mixed pattern (30% overwrites, 20% deletes, 50% appends)
    - _Requirements: 1.2, 1.3, 1.4, 1.5_
  
  - [ ]* 1.3 Write unit tests for pattern generator
    - Test each pattern produces correct distribution
    - Test boundary conditions (0%, 100%)
    - Test invalid configurations
    - _Requirements: 1.2, 1.3, 1.4, 1.5_

- [x] 2. Implement data reduction ratio calculation
  - [x] 2.1 Add reduction ratio tracking to mergeJob
    - Track original total size before merge
    - Track final total size after merge
    - Calculate ratio as (original - final) / original
    - _Requirements: 1.6_
  
  - [x] 2.2 Add reduction ratio to benchmark reporting
    - Report ratio in benchmark metrics
    - Format as percentage for readability
    - _Requirements: 1.6_
  
  - [ ]* 2.3 Write unit tests for reduction ratio calculation
    - Test calculation correctness with known sizes
    - Test edge cases (no reduction, 100% reduction)
    - _Requirements: 1.6_

- [ ] 3. Implement memory budget benchmark scenarios
  - [x] 3.1 Create MemoryBudgetScenario struct
    - Define scenario configuration (name, budget)
    - Define result struct (duration, peak memory, batch count, efficiency)
    - _Requirements: 2.1_
  
  - [x] 3.2 Implement memory budget benchmark runner
    - Run merge with different memory budgets (16MB, 64MB, 256MB, 1GB)
    - Collect metrics for each budget level
    - Calculate memory efficiency (bytes processed per MB)
    - _Requirements: 2.1, 2.2, 2.3, 2.5_
  
  - [x] 3.3 Implement optimal budget identification
    - Analyze results across budget levels
    - Identify budget with best duration/memory balance
    - Report recommendation
    - _Requirements: 2.4_
  
  - [ ]* 3.4 Write benchmark tests for memory budget scenarios
    - Benchmark each budget level with small dataset
    - Verify batch count increases as budget decreases
    - Verify efficiency calculation correctness
    - _Requirements: 2.1, 2.2, 2.3, 2.5_

- [x] 4. Checkpoint - Verify pattern and memory budget functionality
  - Ensure all tests pass with `-race` flag
  - Verify benchmark metrics are collected correctly
  - Ask user if questions arise

- [-] 5. Implement concurrent load generator
  - [x] 5.1 Create ConcurrentLoadConfig struct
    - Define configuration (readers, writers, hot keys, sample rate)
    - Define LatencyStats struct (p50, p95, p99, max, samples)
    - Define ConcurrentLoadMetrics struct
    - _Requirements: 3.1, 3.5_
  
  - [x] 5.2 Implement latency sampling and percentile calculation
    - Sample latency every N operations
    - Store samples in fixed-size buffer
    - Calculate percentiles (p50, p95, p99) from samples
    - Track max latency
    - _Requirements: 3.1_
  
  - [x] 5.3 Implement concurrent reader goroutines
    - Start N reader goroutines
    - Each reader performs Get operations on hot keys
    - Sample and record read latency
    - Count total read operations
    - _Requirements: 3.1, 3.5, 3.6_
  
  - [x] 5.4 Implement concurrent writer goroutines
    - Start N writer goroutines
    - Each writer performs Put operations on hot keys
    - Sample and record write latency
    - Count total write operations
    - Track write conflicts
    - _Requirements: 3.1, 3.4, 3.5, 3.6_
  
  - [x] 5.5 Implement ConcurrentLoadController
    - Provide Stop() method to gracefully shutdown load
    - Provide Metrics() method to retrieve current metrics
    - Ensure clean shutdown and resource cleanup
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  
  - [ ]* 5.6 Write unit tests for concurrent load generator
    - Test latency sampling and percentile calculation
    - Test concurrent reader/writer coordination
    - Test metrics collection accuracy
    - Test graceful shutdown
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 6. Implement throughput degradation calculation
  - [x] 6.1 Add baseline throughput measurement
    - Run workload without merge to establish baseline
    - Measure read/write operations per second
    - _Requirements: 3.2_
  
  - [x] 6.2 Calculate and report degradation
    - Calculate degradation as (baseline - merge) / baseline
    - Report as percentage
    - Include in benchmark metrics
    - _Requirements: 3.2_
  
  - [ ]* 6.3 Write unit tests for degradation calculation
    - Test calculation with known baseline and merge throughput
    - Test edge cases (no degradation, 100% degradation)
    - _Requirements: 3.2_

- [x] 7. Enhance merge diagnostics with lock contention metrics
  - [x] 7.1 Add lock wait time tracking to mergeJob
    - Track time spent waiting for locks
    - Track number of lock acquisitions
    - _Requirements: 3.3_
  
  - [x] 7.2 Update EnhancedMergeDiagnostics struct
    - Add LockWaitTime field
    - Add PhaseTimings map for per-phase duration
    - Add EntriesProcessed, BytesWritten, SpaceReclaimed fields
    - _Requirements: 3.3_
  
  - [x] 7.3 Integrate enhanced diagnostics into merge pipeline
    - Collect phase timings during merge
    - Track entries processed and bytes written
    - Calculate space reclaimed
    - _Requirements: 3.3_
  
  - [ ]* 7.4 Write unit tests for enhanced diagnostics
    - Test metrics collection during merge
    - Test phase timing accuracy
    - Test space reclaimed calculation
    - _Requirements: 3.3_

- [ ] 8. Checkpoint - Verify concurrent interaction functionality
  - Ensure all tests pass with `-race` flag
  - Verify latency percentiles are calculated correctly
  - Verify throughput degradation is measured accurately
  - Ask user if questions arise

- [x] 9. Create comprehensive benchmark test suite
  - [x] 9.1 Create merge_bench_patterns_test.go
    - Benchmark all four update patterns
    - Test with small, medium, large datasets
    - Test with hint enabled and disabled
    - Report reduction ratio for each pattern
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_
  
  - [x] 9.2 Create merge_bench_memory_test.go
    - Benchmark four memory budget levels (16MB, 64MB, 256MB, 1GB)
    - Test with different dataset sizes
    - Report batch count, peak memory, efficiency
    - Identify and report optimal budget
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_
  
  - [x] 9.3 Create merge_bench_concurrent_test.go
    - Benchmark three concurrency scenarios (read-heavy, write-heavy, balanced)
    - Measure baseline throughput without merge
    - Measure throughput during merge
    - Report latency percentiles, degradation, lock metrics
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_
  
  - [x] 9.4 Update docs/merge-benchmarks.md with new scenarios
    - Document update pattern scenarios
    - Document memory budget scenarios
    - Document concurrent interaction scenarios
    - Provide example commands to run new benchmarks
    - _Requirements: 1.1, 2.1, 3.1_

- [x] 10. Run comprehensive benchmark suite and analyze results
  - [x] 10.1 Execute all benchmark scenarios
    - Run with `-race` flag
    - Run with `-benchmem` for memory profiling
    - Collect results for all scenarios
    - _Requirements: 1.1, 2.1, 3.1_
  
  - [x] 10.2 Analyze benchmark results
    - Identify memory bottlenecks (high peak memory, many batches)
    - Identify lock contention issues (high lock hold time, high wait time)
    - Identify latency spikes (high p99, high max latency)
    - Document findings in analysis report
    - _Requirements: 4.1, 4.2, 4.3_

- [ ] 11. Implement targeted optimizations based on findings
  - [ ] 11.1 Implement memory optimization (if bottleneck identified)
    - Analyze memory usage patterns from benchmarks
    - Implement optimization to reduce peak memory
    - Document optimization strategy
    - _Requirements: 4.1_
  
  - [ ] 11.2 Implement lock hold time optimization (if contention identified)
    - Analyze lock hold patterns from benchmarks
    - Implement optimization to reduce lock duration
    - Document optimization strategy
    - _Requirements: 4.2_
  
  - [ ] 11.3 Implement latency optimization (if spikes identified)
    - Analyze latency patterns from benchmarks
    - Implement optimization to reduce p99 latency
    - Document optimization strategy
    - _Requirements: 4.3_
  
  - [ ]* 11.4 Write unit tests for optimizations
    - Test each optimization maintains correctness
    - Test optimizations don't break existing functionality
    - Run with `-race` flag
    - _Requirements: 4.6_

- [ ] 12. Validate optimizations with before/after benchmarks
  - [ ] 12.1 Run baseline benchmarks before optimization
    - Collect comprehensive metrics
    - Save as baseline for comparison
    - _Requirements: 4.4_
  
  - [ ] 12.2 Run benchmarks after optimization
    - Use same scenarios as baseline
    - Collect comprehensive metrics
    - _Requirements: 4.4_
  
  - [ ] 12.3 Compare and document results
    - Calculate improvement percentages for target metrics
    - Verify no regression in other metrics (>5%)
    - Verify improvement in target metric (>10%)
    - Document in optimization report
    - _Requirements: 4.4, 4.5_
  
  - [ ] 12.4 Update docs/merge-benchmarks.md with optimization results
    - Add "Optimized" section with new benchmark run
    - Document optimization strategies applied
    - Show before/after comparison
    - _Requirements: 4.5_

- [ ] 13. Final checkpoint - Ensure all requirements met
  - Verify all tests pass with `-race` flag
  - Verify all benchmarks run successfully
  - Verify optimizations show measurable improvement
  - Verify documentation is complete and accurate
  - Ask user for final review

## Notes

- Tasks marked with `*` are optional test tasks that can be skipped for faster MVP
- Each task references specific requirements for traceability
- Benchmarks must be run with `-race` flag to detect data races
- All temporary files must be cleaned up after tests
- Optimization tasks (11.1-11.3) are conditional based on benchmark findings
