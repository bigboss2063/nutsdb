# Design Document

## Overview

This design document outlines the implementation strategy for enhancing NutsDB's Merge mechanism testing and performance optimization. The design focuses on four key areas:

1. **Comprehensive Test Coverage**: Expand benchmark scenarios to cover diverse update patterns (overwrite-heavy, delete-heavy, append-only, mixed) that reflect real-world workloads
2. **Memory Budget Analysis**: Systematically evaluate how MergeLookupMemoryBudget affects merge performance and identify optimal configurations
3. **Concurrent Interaction Optimization**: Measure and optimize the impact of merge operations on concurrent read/write workloads
4. **Targeted Performance Improvements**: Implement data-driven optimizations based on benchmark findings to reduce memory usage and latency

The design maintains backward compatibility with existing merge functionality while adding new benchmark scenarios and optimization paths. All changes will be validated through comprehensive benchmarking with race detection enabled.

## Architecture

### Current Merge Architecture

The existing merge pipeline consists of four phases:

1. **Prepare Phase**: Enumerate data files, validate state, create new active file
2. **Rewrite Phase**: Read entries from old files, filter invalid/expired entries, write valid entries to merge output files
3. **Commit Phase**: Update in-memory indexes with new file locations, write hint files
4. **Finalize Phase**: Sync and close output files, clean up old files

Key components:
- `mergeJob`: Orchestrates the entire merge operation
- `mergeOutput`: Represents a single merge output file with hint collector
- `mergeLookupEntry`: Tracks minimal information for index updates during commit
- `mergeDiagnostics`: Records performance metrics (peak memory, lock hold time, batch count)

### Enhanced Architecture

The enhanced architecture adds three new layers:

1. **Benchmark Framework Layer**: Extensible test scenarios with configurable update patterns, memory budgets, and concurrent loads
2. **Metrics Collection Layer**: Enhanced diagnostics including latency percentiles, throughput measurements, lock contention tracking
3. **Optimization Layer**: Targeted improvements based on benchmark analysis

```
┌─────────────────────────────────────────────────────────────┐
│                  Benchmark Framework                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │Update Pattern│  │Memory Budget │  │Concurrent    │      │
│  │  Scenarios   │  │  Scenarios   │  │Load Scenarios│      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  Metrics Collection                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Diagnostics │  │   Latency    │  │  Throughput  │      │
│  │  Aggregator  │  │  Percentiles │  │   Tracker    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Merge Pipeline                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Prepare  │→ │ Rewrite  │→ │  Commit  │→ │ Finalize │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  Optimization Layer                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │Memory Budget │  │  Lock Hold   │  │   Latency    │      │
│  │ Optimization │  │ Optimization │  │ Optimization │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Update Pattern Generator

Generates test datasets with different update characteristics.

```go
type UpdatePattern string

const (
    UpdatePatternOverwriteHeavy UpdatePattern = "overwrite-heavy"
    UpdatePatternDeleteHeavy    UpdatePattern = "delete-heavy"
    UpdatePatternAppendOnly     UpdatePattern = "append-only"
    UpdatePatternMixed          UpdatePattern = "mixed"
)

type UpdatePatternConfig struct {
    Pattern       UpdatePattern
    OverwritePct  float64  // Percentage of keys to overwrite (0.0-1.0)
    DeletePct     float64  // Percentage of keys to delete (0.0-1.0)
    AppendPct     float64  // Percentage of new keys to append (0.0-1.0)
}

// GenerateUpdatePattern applies the specified update pattern to a dataset
func GenerateUpdatePattern(db *DB, bucket string, baseEntries int, config UpdatePatternConfig) error
```

### 2. Memory Budget Scenario Runner

Executes merge benchmarks with different memory budget configurations.

```go
type MemoryBudgetScenario struct {
    Name   string
    Budget int64  // Memory budget in bytes
}

type MemoryBudgetResult struct {
    Scenario      MemoryBudgetScenario
    Duration      time.Duration
    PeakMemory    int64
    BatchCount    int
    Efficiency    float64  // Bytes processed per MB budget
}

// RunMemoryBudgetScenarios executes merge with different memory budgets
func RunMemoryBudgetScenarios(db *DB, scenarios []MemoryBudgetScenario) ([]MemoryBudgetResult, error)
```

### 3. Concurrent Load Generator

Generates concurrent read/write load during merge operations.

```go
type ConcurrentLoadConfig struct {
    Readers      int
    Writers      int
    HotKeys      [][]byte
    WriteValue   []byte
    SampleRate   int  // Sample latency every N operations
}

type ConcurrentLoadMetrics struct {
    ReadOps       uint64
    WriteOps      uint64
    ReadLatency   LatencyStats
    WriteLatency  LatencyStats
    Duration      time.Duration
}

type LatencyStats struct {
    P50    time.Duration
    P95    time.Duration
    P99    time.Duration
    Max    time.Duration
    Samples []time.Duration
}

// StartConcurrentLoad starts background read/write operations
func StartConcurrentLoad(db *DB, bucket string, config ConcurrentLoadConfig) (*ConcurrentLoadController, error)

type ConcurrentLoadController struct {
    Stop    func()
    Metrics func() ConcurrentLoadMetrics
}
```

### 4. Enhanced Diagnostics Collector

Extends existing mergeDiagnostics with additional metrics.

```go
type EnhancedMergeDiagnostics struct {
    // Existing metrics
    PeakLookupBytes int64
    MaxLockHold     time.Duration
    Batches         int
    
    // New metrics
    PhaseTimings    map[string]time.Duration  // Per-phase duration
    EntriesProcessed int64
    BytesWritten    int64
    SpaceReclaimed  int64
    LockWaitTime    time.Duration
}

// CollectEnhancedDiagnostics wraps merge execution with enhanced metrics collection
func CollectEnhancedDiagnostics(mergeFn func() error) (*EnhancedMergeDiagnostics, error)
```

### 5. Optimization Strategies

Implements targeted optimizations based on benchmark findings.

```go
// MemoryOptimization reduces peak memory usage during commit phase
type MemoryOptimization interface {
    Apply(job *mergeJob) error
    Validate() error
}

// LockOptimization reduces lock hold time during commit phase
type LockOptimization interface {
    Apply(job *mergeJob) error
    Validate() error
}

// LatencyOptimization reduces concurrent operation latency during merge
type LatencyOptimization interface {
    Apply(db *DB) error
    Validate() error
}
```

## Data Models

### Benchmark Configuration

```go
type MergeBenchmarkConfig struct {
    Dataset       MergeBenchDataset
    UpdatePattern UpdatePatternConfig
    MemoryBudget  int64
    ConcurrentLoad ConcurrentLoadConfig
    HintEnabled   bool
}

type MergeBenchDataset struct {
    Name        string
    Entries     int
    KeySize     int
    ValueSize   int
    SegmentSize int64
}
```

### Benchmark Results

```go
type MergeBenchmarkResult struct {
    Config      MergeBenchmarkConfig
    Duration    time.Duration
    Memory      MemoryMetrics
    Diagnostics EnhancedMergeDiagnostics
    Concurrent  ConcurrentLoadMetrics
}

type MemoryMetrics struct {
    AllocatedMiB float64
    Allocations  int
    PeakBytes    int64
}
```

### Optimization Report

```go
type OptimizationReport struct {
    Strategy     string
    Baseline     MergeBenchmarkResult
    Optimized    MergeBenchmarkResult
    Improvements map[string]float64  // Metric name -> improvement percentage
    Description  string
}
```


## Validation Criteria

The implementation must satisfy the following validation criteria derived from requirements:

### Update Pattern Validation

1. **Pattern Distribution Accuracy**: Update patterns must produce the specified distribution of operations within ±5% tolerance
   - Overwrite-heavy: 60-80% overwrites
   - Delete-heavy: 40-60% deletes
   - Append-only: 0% updates/deletes
   - Mixed: 30% overwrites, 20% deletes, 50% appends

2. **Data Reduction Reporting**: Merge must correctly calculate and report data reduction ratio as (original_size - final_size) / original_size

### Memory Budget Validation

3. **Memory Budget Impact**: Benchmarks must demonstrate measurable correlation between memory budget and batch count
4. **Memory Efficiency Calculation**: System must correctly calculate memory efficiency as total_bytes_processed / memory_budget_MB
5. **Optimal Budget Identification**: Analysis must identify the memory budget that provides best balance of duration and memory usage

### Concurrent Interaction Validation

6. **Metrics Completeness**: Concurrent load scenarios must collect and report all required metrics: p50/p95/p99/max latency, throughput, lock hold time, lock wait time
7. **Throughput Degradation**: System must calculate and report throughput degradation during merge compared to baseline
8. **Conflict Rate Tracking**: Multi-writer scenarios must track and report write conflict rate

### Optimization Validation

9. **Optimization Triggering**: Optimizations must be applied when benchmarks identify performance issues (high memory, lock contention, latency spikes)
10. **Before/After Comparison**: Each optimization must be validated with before/after benchmark comparison showing measurable improvement
11. **Correctness Preservation**: All optimizations must pass existing correctness tests without regression

## Error Handling

### Benchmark Execution Errors

1. **Invalid Configuration**: When benchmark configuration is invalid (e.g., negative memory budget, invalid pattern), return descriptive error without executing
2. **Insufficient Resources**: When system resources are insufficient for benchmark, return error with resource requirements
3. **Concurrent Load Failure**: When concurrent load generator fails to start, clean up and return error with failure reason

### Merge Operation Errors

1. **Memory Budget Exceeded**: When actual memory usage exceeds configured budget significantly (>150%), log warning but continue with increased batch count
2. **Lock Timeout**: When lock acquisition times out during concurrent scenarios, record timeout event in diagnostics
3. **Optimization Application Failure**: When optimization cannot be applied, log error and continue with baseline implementation

### Data Consistency Errors

1. **Correctness Test Failure**: When optimization causes correctness tests to fail, immediately rollback optimization and report failure
2. **Index Inconsistency**: When merge produces index inconsistencies, abort merge and return to pre-merge state
3. **Data Corruption**: When data corruption is detected during merge, abort immediately and preserve original files

## Testing Strategy

### Testing Approach

The testing strategy focuses on comprehensive unit tests and benchmarks:

- **Unit tests**: Verify specific examples, edge cases, and error conditions
- **Benchmarks**: Measure performance across different scenarios and configurations
- Both approaches work together to ensure correctness and performance

### Unit Testing Focus

Unit tests should cover:
- Specific update pattern examples (e.g., exactly 70% overwrites)
- Boundary conditions (e.g., 0% deletes, 100% overwrites)
- Error conditions (e.g., invalid configurations, resource exhaustion)
- Integration points between benchmark framework and merge pipeline
- Metrics calculation correctness (reduction ratio, efficiency, degradation)

### Benchmark Testing Focus

Benchmarks should cover:
- All four update patterns (overwrite-heavy, delete-heavy, append-only, mixed)
- Multiple memory budget levels (16MB, 64MB, 256MB, 1GB)
- Various concurrent load scenarios (read-heavy, write-heavy, balanced)
- Different dataset sizes (small, medium, large)
- Hint file enabled and disabled modes

### Test Organization

```
merge_bench_test.go              # Existing benchmark tests
merge_bench_patterns_test.go     # New: Update pattern benchmarks
merge_bench_memory_test.go       # New: Memory budget benchmarks
merge_bench_concurrent_test.go   # New: Concurrent interaction benchmarks
merge_optimization_test.go       # New: Optimization validation tests
merge_pattern_unit_test.go       # New: Update pattern unit tests
merge_metrics_unit_test.go       # New: Metrics calculation unit tests
```

### Benchmark Validation

All benchmarks must:
1. Run with `-race` flag to detect data races
2. Report all required metrics (duration, memory, diagnostics, concurrent load)
3. Clean up temporary files after execution
4. Be deterministic and repeatable
5. Use `testing.B.ReportMetric()` for custom metrics

### Optimization Validation

Each optimization must:
1. Pass all existing unit tests
2. Pass all existing benchmarks
3. Show measurable improvement in target metric (>10%)
4. Not regress other metrics by more than 5%
5. Be documented with before/after benchmark results
6. Include analysis of why the optimization works

### Test Execution

```bash
# Run all tests with race detection
go test -race -timeout 15m ./...

# Run only benchmarks
go test -run '^$' -bench . -benchmem -count=1

# Run specific benchmark suite
go test -run '^$' -bench BenchmarkMergePatterns -benchmem -count=1

# Run with coverage
go test -race -coverprofile=coverage.out -covermode=atomic ./...
```

