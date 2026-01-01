// Copyright 2023 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConcurrentLoadConfig configures concurrent read/write load during merge operations
type ConcurrentLoadConfig struct {
	Readers    int      // Number of concurrent reader goroutines
	Writers    int      // Number of concurrent writer goroutines
	HotKeys    [][]byte // Keys to access during load generation
	WriteValue []byte   // Value to write for Put operations
	SampleRate int      // Sample latency every N operations
}

// LatencyStats captures latency percentiles and samples
type LatencyStats struct {
	P50     time.Duration   // 50th percentile latency
	P95     time.Duration   // 95th percentile latency
	P99     time.Duration   // 99th percentile latency
	Max     time.Duration   // Maximum latency observed
	Samples []time.Duration // Raw latency samples
}

// ConcurrentLoadMetrics captures metrics from concurrent load generation
type ConcurrentLoadMetrics struct {
	ReadOps        uint64        // Total read operations performed
	WriteOps       uint64        // Total write operations performed
	WriteConflicts uint64        // Number of write conflicts encountered
	ReadLatency    LatencyStats  // Read operation latency statistics
	WriteLatency   LatencyStats  // Write operation latency statistics
	Duration       time.Duration // Total duration of load generation
}

// ThroughputMetrics captures throughput measurements
type ThroughputMetrics struct {
	ReadOpsPerSec  float64 // Read operations per second
	WriteOpsPerSec float64 // Write operations per second
	TotalOpsPerSec float64 // Total operations per second
}

// ThroughputDegradation captures throughput degradation during merge
type ThroughputDegradation struct {
	Baseline         ThroughputMetrics // Baseline throughput without merge
	DuringMerge      ThroughputMetrics // Throughput during merge
	ReadDegradation  float64           // Read throughput degradation percentage
	WriteDegradation float64           // Write throughput degradation percentage
	TotalDegradation float64           // Total throughput degradation percentage
}

// ConcurrentLoadController manages concurrent load generation and provides control methods
type ConcurrentLoadController struct {
	db        *DB
	bucket    string
	config    ConcurrentLoadConfig
	stopCh    chan struct{}
	wg        sync.WaitGroup
	startTime time.Time

	// Metrics tracking
	readOps        atomic.Uint64
	writeOps       atomic.Uint64
	writeConflicts atomic.Uint64
	readSamples    *latencySampler
	writeSamples   *latencySampler
}

// latencySampler collects latency samples with a fixed-size buffer
type latencySampler struct {
	mu      sync.Mutex
	samples []time.Duration
	maxSize int
	counter atomic.Uint64
}

// newLatencySampler creates a new latency sampler with the specified buffer size
func newLatencySampler(maxSize int) *latencySampler {
	return &latencySampler{
		samples: make([]time.Duration, 0, maxSize),
		maxSize: maxSize,
	}
}

// sample records a latency sample if sampling criteria is met
func (ls *latencySampler) sample(latency time.Duration, sampleRate int) {
	count := ls.counter.Add(1)
	if sampleRate > 0 && count%uint64(sampleRate) != 0 {
		return
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	if len(ls.samples) < ls.maxSize {
		ls.samples = append(ls.samples, latency)
	}
}

// calculateStats computes percentile statistics from collected samples
func (ls *latencySampler) calculateStats() LatencyStats {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if len(ls.samples) == 0 {
		return LatencyStats{}
	}

	// Create a copy and sort for percentile calculation
	sorted := make([]time.Duration, len(ls.samples))
	copy(sorted, ls.samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	stats := LatencyStats{
		Samples: sorted,
		Max:     sorted[len(sorted)-1],
	}

	// Calculate percentiles
	stats.P50 = sorted[len(sorted)*50/100]
	stats.P95 = sorted[len(sorted)*95/100]
	stats.P99 = sorted[len(sorted)*99/100]

	return stats
}

// runReaders starts N reader goroutines that perform Get operations
func (c *ConcurrentLoadController) runReaders() {
	if c.config.Readers <= 0 || len(c.config.HotKeys) == 0 {
		return
	}

	for i := 0; i < c.config.Readers; i++ {
		c.wg.Add(1)
		go c.readerWorker(i)
	}
}

// readerWorker performs continuous Get operations until stopped
func (c *ConcurrentLoadController) readerWorker(id int) {
	defer c.wg.Done()

	keyCount := len(c.config.HotKeys)
	keyIndex := id % keyCount

	for {
		select {
		case <-c.stopCh:
			return
		default:
			// Perform Get operation and measure latency
			key := c.config.HotKeys[keyIndex]
			start := time.Now()

			err := c.db.View(func(tx *Tx) error {
				_, err := tx.Get(c.bucket, key)
				return err
			})

			latency := time.Since(start)

			// Record metrics
			if err == nil || err == ErrKeyNotFound {
				c.readOps.Add(1)
				c.readSamples.sample(latency, c.config.SampleRate)
			}

			// Move to next key in round-robin fashion
			keyIndex = (keyIndex + 1) % keyCount
		}
	}
}

// runWriters starts N writer goroutines that perform Put operations
func (c *ConcurrentLoadController) runWriters() {
	if c.config.Writers <= 0 || len(c.config.HotKeys) == 0 {
		return
	}

	for i := 0; i < c.config.Writers; i++ {
		c.wg.Add(1)
		go c.writerWorker(i)
	}
}

// writerWorker performs continuous Put operations until stopped
func (c *ConcurrentLoadController) writerWorker(id int) {
	defer c.wg.Done()

	keyCount := len(c.config.HotKeys)
	keyIndex := id % keyCount
	writeValue := c.config.WriteValue
	if writeValue == nil {
		writeValue = []byte("concurrent_write_value")
	}

	for {
		select {
		case <-c.stopCh:
			return
		default:
			// Perform Put operation and measure latency
			key := c.config.HotKeys[keyIndex]
			start := time.Now()

			err := c.db.Update(func(tx *Tx) error {
				return tx.Put(c.bucket, key, writeValue, Persistent)
			})

			latency := time.Since(start)

			// Record metrics
			if err == nil {
				c.writeOps.Add(1)
				c.writeSamples.sample(latency, c.config.SampleRate)
			} else if err == ErrTxnTooBig || err == ErrDBClosed {
				// Track write conflicts
				c.writeConflicts.Add(1)
			}

			// Move to next key in round-robin fashion
			keyIndex = (keyIndex + 1) % keyCount
		}
	}
}

// StartConcurrentLoad starts background read/write operations and returns a controller
func StartConcurrentLoad(db *DB, bucket string, config ConcurrentLoadConfig) (*ConcurrentLoadController, error) {
	if db == nil {
		return nil, ErrDBClosed
	}

	if len(config.HotKeys) == 0 {
		return nil, ErrBucket
	}

	// Default sample rate if not specified
	if config.SampleRate <= 0 {
		config.SampleRate = 100
	}

	controller := &ConcurrentLoadController{
		db:           db,
		bucket:       bucket,
		config:       config,
		stopCh:       make(chan struct{}),
		startTime:    time.Now(),
		readSamples:  newLatencySampler(10000),
		writeSamples: newLatencySampler(10000),
	}

	// Start reader and writer goroutines
	controller.runReaders()
	controller.runWriters()

	return controller, nil
}

// Stop gracefully shuts down the concurrent load generation
func (c *ConcurrentLoadController) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// Metrics retrieves current metrics from the concurrent load generation
func (c *ConcurrentLoadController) Metrics() ConcurrentLoadMetrics {
	duration := time.Since(c.startTime)

	return ConcurrentLoadMetrics{
		ReadOps:        c.readOps.Load(),
		WriteOps:       c.writeOps.Load(),
		WriteConflicts: c.writeConflicts.Load(),
		ReadLatency:    c.readSamples.calculateStats(),
		WriteLatency:   c.writeSamples.calculateStats(),
		Duration:       duration,
	}
}

// CalculateThroughput computes throughput metrics from concurrent load metrics
func CalculateThroughput(metrics ConcurrentLoadMetrics) ThroughputMetrics {
	if metrics.Duration == 0 {
		return ThroughputMetrics{}
	}

	seconds := metrics.Duration.Seconds()
	return ThroughputMetrics{
		ReadOpsPerSec:  float64(metrics.ReadOps) / seconds,
		WriteOpsPerSec: float64(metrics.WriteOps) / seconds,
		TotalOpsPerSec: float64(metrics.ReadOps+metrics.WriteOps) / seconds,
	}
}

// MeasureBaselineThroughput runs workload without merge to establish baseline
func MeasureBaselineThroughput(db *DB, bucket string, config ConcurrentLoadConfig, duration time.Duration) (ThroughputMetrics, error) {
	controller, err := StartConcurrentLoad(db, bucket, config)
	if err != nil {
		return ThroughputMetrics{}, err
	}

	// Run for specified duration
	time.Sleep(duration)

	// Stop and collect metrics
	controller.Stop()
	metrics := controller.Metrics()

	return CalculateThroughput(metrics), nil
}

// CalculateDegradation computes throughput degradation percentage
func CalculateDegradation(baseline, duringMerge ThroughputMetrics) ThroughputDegradation {
	degradation := ThroughputDegradation{
		Baseline:    baseline,
		DuringMerge: duringMerge,
	}

	// Calculate degradation as (baseline - merge) / baseline
	if baseline.ReadOpsPerSec > 0 {
		degradation.ReadDegradation = (baseline.ReadOpsPerSec - duringMerge.ReadOpsPerSec) / baseline.ReadOpsPerSec * 100
	}

	if baseline.WriteOpsPerSec > 0 {
		degradation.WriteDegradation = (baseline.WriteOpsPerSec - duringMerge.WriteOpsPerSec) / baseline.WriteOpsPerSec * 100
	}

	if baseline.TotalOpsPerSec > 0 {
		degradation.TotalDegradation = (baseline.TotalOpsPerSec - duringMerge.TotalOpsPerSec) / baseline.TotalOpsPerSec * 100
	}

	return degradation
}
