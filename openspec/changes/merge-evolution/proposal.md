# Requirements Document

## Introduction

本文档定义了 NutsDB Merge 机制测试和性能优化的需求。当前的 Merge benchmark 测试覆盖了基本的 BTree 场景，但在不同更新模式、内存预算配置、并发交互等方面存在测试盲区。本提案聚焦于补全测试覆盖度、优化内存使用效率、改善并发性能这三个核心方向。

## Glossary

- **Merge**: 数据库的压缩操作，将多个数据文件中的有效记录重写到新文件中，删除过期和无效记录
- **BTree**: B树索引数据结构，用于键值对存储
- **Hotset**: 并发负载测试中频繁访问的键集合
- **MergeLookupMemoryBudget**: Merge 过程中用于查找表的内存预算
- **MergeLookupBatchSize**: Merge 提交时的批次大小
- **Segment**: 数据文件分段，由 SegmentSize 控制大小
- **HintFile**: 提示文件，用于加速启动时的索引重建
- **Workload_Pattern**: 工作负载模式，如读密集、写密集、混合等
- **Update_Pattern**: 更新模式，如覆盖写、删除密集、追加密集等
- **Concurrent_Load**: 并发负载，在 Merge 期间同时进行的读写操作
- **Merge_Diagnostics**: Merge 诊断信息，包括内存使用、锁持有时间、批次数等
- **Fragmentation_Ratio**: 碎片率，表示数据文件中无效数据所占的比例

## Requirements

### Requirement 1: Update Pattern Diversity

**User Story:** As a performance engineer, I want to test Merge under different update patterns, so that I can identify performance bottlenecks in specific scenarios.

#### Acceptance Criteria

1. THE System SHALL support at least four update patterns: overwrite-heavy, delete-heavy, append-only, and mixed
2. WHEN using overwrite-heavy pattern, THE System SHALL update 60-80% of existing keys
3. WHEN using delete-heavy pattern, THE System SHALL delete 40-60% of keys after initial load
4. WHEN using append-only pattern, THE System SHALL only insert new keys without updates or deletes
5. WHEN using mixed pattern, THE System SHALL combine 30% overwrites, 20% deletes, and 50% new inserts
6. THE benchmark SHALL report the effective data reduction ratio after merge for each pattern

### Requirement 2: Memory Budget Sensitivity Analysis

**User Story:** As a system administrator, I want to understand how MergeLookupMemoryBudget affects Merge performance, so that I can tune it for my deployment.

#### Acceptance Criteria

1. THE System SHALL benchmark Merge with at least four memory budget levels: 16MB, 64MB, 256MB, and 1GB
2. WHEN memory budget is insufficient, THE System SHALL report the number of commit batches required
3. WHEN memory budget increases, THE benchmark SHALL measure the impact on merge duration and peak memory usage
4. THE System SHALL identify the optimal memory budget for each dataset size
5. THE benchmark SHALL report memory efficiency as bytes-processed-per-MB-budget

### Requirement 3: Concurrent Merge Interaction

**User Story:** As a database developer, I want to understand how concurrent operations interact with Merge, so that I can optimize locking and contention.

#### Acceptance Criteria

1. THE System SHALL measure read/write latency percentiles (p50, p95, p99, max) during Merge
2. WHEN concurrent load is active, THE System SHALL report throughput degradation compared to no-merge baseline
3. THE System SHALL measure lock contention metrics including max lock hold time and lock wait time
4. WHEN multiple writers are active, THE benchmark SHALL report write conflict rate during Merge
5. THE System SHALL support configurable concurrent load intensity (light, medium, heavy)
6. THE benchmark SHALL test at least three concurrency scenarios: read-heavy (4 readers, 0 writers), write-heavy (0 readers, 2 writers), and balanced (2 readers, 2 writers)

### Requirement 4: Performance Optimization Based on Test Results

**User Story:** As a database developer, I want to implement targeted optimizations based on benchmark findings, so that I can improve Merge memory efficiency and reduce latency impact.

#### Acceptance Criteria

1. WHEN benchmark results identify memory bottlenecks, THE System SHALL implement optimizations to reduce peak memory usage
2. WHEN benchmark results identify lock contention issues, THE System SHALL implement optimizations to reduce lock hold time
3. WHEN benchmark results identify latency spikes, THE System SHALL implement optimizations to reduce p99 latency during Merge
4. THE optimizations SHALL be validated by re-running benchmarks and comparing before/after metrics
5. THE System SHALL document optimization strategies and their measured impact in the benchmark report
6. WHEN optimizations are applied, THE System SHALL maintain data consistency and correctness guarantees


