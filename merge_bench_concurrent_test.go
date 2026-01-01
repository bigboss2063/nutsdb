package nutsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	concurrentLoadDuration = 5 * time.Second
	baselineDuration       = 3 * time.Second
)

// BenchmarkMergeConcurrent benchmarks merge operations with concurrent read/write load
func BenchmarkMergeConcurrent(b *testing.B) {
	datasets := []mergeBenchDataset{
		{name: "small-100k", entries: 100_000, keySize: 16, valueSize: 128, segmentSize: 8 * MB},
		{name: "medium-500k", entries: 500_000, keySize: 16, valueSize: 128, segmentSize: 16 * MB},
		{name: "large-1m", entries: 1_000_000, keySize: 16, valueSize: 512, segmentSize: 32 * MB},
	}

	scenarios := []struct {
		name    string
		readers int
		writers int
	}{
		{name: "read-heavy", readers: 4, writers: 0},
		{name: "write-heavy", readers: 0, writers: 2},
		{name: "balanced", readers: 2, writers: 2},
	}

	for _, dataset := range datasets {
		dataset := dataset
		for _, scenario := range scenarios {
			scenario := scenario
			name := fmt.Sprintf("%s/%s", dataset.name, scenario.name)
			b.Run(name, func(b *testing.B) {
				benchmarkMergeConcurrent(b, dataset, scenario.readers, scenario.writers)
			})
		}
	}
}

func benchmarkMergeConcurrent(b *testing.B, dataset mergeBenchDataset, readers, writers int) {
	b.ReportAllocs()
	b.ResetTimer()

	var diagAgg mergeBenchDiagnosticsAgg
	var totalReadOps, totalWriteOps uint64
	var totalReadP50, totalReadP95, totalReadP99, totalReadMax int64
	var totalWriteP50, totalWriteP95, totalWriteP99, totalWriteMax int64
	var totalDegradation float64
	var degradationCount int

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-concurrent-%s-%d-%d-%d", dataset.name, readers, writers, i))
		removeDir(dir)

		opts := DefaultOptions
		opts.Dir = dir
		opts.SegmentSize = dataset.segmentSize
		opts.EnableHintFile = true

		db, err := Open(opts)
		if err != nil {
			b.Fatalf("open db: %v", err)
		}

		bucket := "bench"
		if err := db.Update(func(tx *Tx) error {
			return tx.NewBucket(DataStructureBTree, bucket)
		}); err != nil {
			b.Fatalf("create bucket: %v", err)
		}

		// Populate dataset
		populateConcurrentBenchData(b, db, bucket, dataset)
		ensureMultipleDataFiles(b, db, dir, bucket, dataset)
		originalBytes, err := dataFileBytes(dir)
		if err != nil {
			b.Fatalf("measure original bytes: %v", err)
		}

		// Build hot keys for concurrent load
		hotKeys := buildMergeBenchHotKeys(dataset)
		writeValue := bytes.Repeat([]byte("w"), dataset.valueSize)

		config := ConcurrentLoadConfig{
			Readers:    readers,
			Writers:    writers,
			HotKeys:    hotKeys,
			WriteValue: writeValue,
			SampleRate: 100,
		}

		// Measure baseline throughput without merge
		baseline, err := MeasureBaselineThroughput(db, bucket, config, baselineDuration)
		if err != nil {
			b.Fatalf("measure baseline throughput: %v", err)
		}

		// Start concurrent load
		controller, err := StartConcurrentLoad(db, bucket, config)
		if err != nil {
			b.Fatalf("start concurrent load: %v", err)
		}

		// Wait a bit for load to stabilize
		time.Sleep(100 * time.Millisecond)

		b.StartTimer()
		mergeStart := time.Now()
		if err := db.Merge(); err != nil {
			b.Fatalf("merge: %v", err)
		}
		mergeDuration := time.Since(mergeStart)
		b.StopTimer()

		// Stop concurrent load
		controller.Stop()
		metrics := controller.Metrics()

		// Calculate throughput during merge
		duringMerge := CalculateThroughput(metrics)

		// Calculate degradation
		degradation := CalculateDegradation(baseline, duringMerge)

		finalBytes, err := dataFileBytes(dir)
		if err != nil {
			b.Fatalf("measure final bytes: %v", err)
		}

		// Aggregate metrics
		diagAgg.add(db.GetEnhancedMergeDiagnostics(), originalBytes, finalBytes)
		totalReadOps += metrics.ReadOps
		totalWriteOps += metrics.WriteOps

		if len(metrics.ReadLatency.Samples) > 0 {
			totalReadP50 += metrics.ReadLatency.P50.Microseconds()
			totalReadP95 += metrics.ReadLatency.P95.Microseconds()
			totalReadP99 += metrics.ReadLatency.P99.Microseconds()
			totalReadMax += metrics.ReadLatency.Max.Microseconds()
		}

		if len(metrics.WriteLatency.Samples) > 0 {
			totalWriteP50 += metrics.WriteLatency.P50.Microseconds()
			totalWriteP95 += metrics.WriteLatency.P95.Microseconds()
			totalWriteP99 += metrics.WriteLatency.P99.Microseconds()
			totalWriteMax += metrics.WriteLatency.Max.Microseconds()
		}

		totalDegradation += degradation.TotalDegradation
		degradationCount++

		b.Logf("Merge duration: %v, Read ops: %d, Write ops: %d, Degradation: %.2f%%",
			mergeDuration, metrics.ReadOps, metrics.WriteOps, degradation.TotalDegradation)

		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeDir(dir)
	}

	// Report aggregated metrics
	diagAgg.report(b)

	if b.N > 0 {
		if totalReadOps > 0 {
			b.ReportMetric(float64(totalReadP50)/float64(b.N), "read_p50_us")
			b.ReportMetric(float64(totalReadP95)/float64(b.N), "read_p95_us")
			b.ReportMetric(float64(totalReadP99)/float64(b.N), "read_p99_us")
			b.ReportMetric(float64(totalReadMax)/float64(b.N), "read_max_us")
		}

		if totalWriteOps > 0 {
			b.ReportMetric(float64(totalWriteP50)/float64(b.N), "write_p50_us")
			b.ReportMetric(float64(totalWriteP95)/float64(b.N), "write_p95_us")
			b.ReportMetric(float64(totalWriteP99)/float64(b.N), "write_p99_us")
			b.ReportMetric(float64(totalWriteMax)/float64(b.N), "write_max_us")
		}

		if degradationCount > 0 {
			avgDegradation := totalDegradation / float64(degradationCount)
			b.ReportMetric(avgDegradation, "throughput_degradation_pct")
		}
	}
}

func populateConcurrentBenchData(b *testing.B, db *DB, bucket string, dataset mergeBenchDataset) {
	const batchSize = 10_000
	value := bytes.Repeat([]byte("v"), dataset.valueSize)
	updateValue := bytes.Repeat([]byte("u"), dataset.valueSize)

	// Initial population
	for start := 0; start < dataset.entries; start += batchSize {
		end := start + batchSize
		if end > dataset.entries {
			end = dataset.entries
		}
		if err := db.Update(func(tx *Tx) error {
			for j := start; j < end; j++ {
				key := makeBenchKey(uint64(j), dataset.keySize)
				if err := tx.Put(bucket, key, value, Persistent); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatalf("populate data: %v", err)
		}
	}

	// Apply updates (30% of entries)
	updateCount := dataset.entries * 3 / 10
	for start := 0; start < updateCount; start += batchSize {
		end := start + batchSize
		if end > updateCount {
			end = updateCount
		}
		if err := db.Update(func(tx *Tx) error {
			for j := start; j < end; j++ {
				key := makeBenchKey(uint64(j), dataset.keySize)
				if err := tx.Put(bucket, key, updateValue, Persistent); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatalf("update data: %v", err)
		}
	}

	// Apply deletes (10% of entries)
	deleteStart := dataset.entries - (dataset.entries / 10)
	for start := deleteStart; start < dataset.entries; start += batchSize {
		end := start + batchSize
		if end > dataset.entries {
			end = dataset.entries
		}
		if err := db.Update(func(tx *Tx) error {
			for j := start; j < end; j++ {
				key := makeBenchKey(uint64(j), dataset.keySize)
				if err := tx.Delete(bucket, key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatalf("delete data: %v", err)
		}
	}
}

// BenchmarkMergeConcurrentDegradation specifically measures throughput degradation
func BenchmarkMergeConcurrentDegradation(b *testing.B) {
	dataset := mergeBenchDataset{
		name:        "medium-500k",
		entries:     500_000,
		keySize:     16,
		valueSize:   128,
		segmentSize: 16 * MB,
	}

	scenarios := []struct {
		name    string
		readers int
		writers int
	}{
		{name: "read-heavy", readers: 4, writers: 0},
		{name: "write-heavy", readers: 0, writers: 2},
		{name: "balanced", readers: 2, writers: 2},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkMergeConcurrentDegradation(b, dataset, scenario.readers, scenario.writers)
		})
	}
}

func benchmarkMergeConcurrentDegradation(b *testing.B, dataset mergeBenchDataset, readers, writers int) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-degradation-%s-%d-%d-%d", dataset.name, readers, writers, i))
		removeDir(dir)

		opts := DefaultOptions
		opts.Dir = dir
		opts.SegmentSize = dataset.segmentSize
		opts.EnableHintFile = true

		db, err := Open(opts)
		if err != nil {
			b.Fatalf("open db: %v", err)
		}

		bucket := "bench"
		if err := db.Update(func(tx *Tx) error {
			return tx.NewBucket(DataStructureBTree, bucket)
		}); err != nil {
			b.Fatalf("create bucket: %v", err)
		}

		// Populate dataset
		populateConcurrentBenchData(b, db, bucket, dataset)
		ensureMultipleDataFiles(b, db, dir, bucket, dataset)

		// Build hot keys
		hotKeys := buildMergeBenchHotKeys(dataset)
		writeValue := bytes.Repeat([]byte("w"), dataset.valueSize)

		config := ConcurrentLoadConfig{
			Readers:    readers,
			Writers:    writers,
			HotKeys:    hotKeys,
			WriteValue: writeValue,
			SampleRate: 100,
		}

		b.StartTimer()

		// Measure baseline throughput
		baseline, err := MeasureBaselineThroughput(db, bucket, config, baselineDuration)
		if err != nil {
			b.Fatalf("measure baseline throughput: %v", err)
		}

		// Start concurrent load and run merge
		controller, err := StartConcurrentLoad(db, bucket, config)
		if err != nil {
			b.Fatalf("start concurrent load: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		if err := db.Merge(); err != nil {
			b.Fatalf("merge: %v", err)
		}

		controller.Stop()
		metrics := controller.Metrics()

		b.StopTimer()

		// Calculate and report degradation
		duringMerge := CalculateThroughput(metrics)
		degradation := CalculateDegradation(baseline, duringMerge)

		b.Logf("Baseline: %.2f ops/sec, During merge: %.2f ops/sec, Degradation: %.2f%%",
			baseline.TotalOpsPerSec, duringMerge.TotalOpsPerSec, degradation.TotalDegradation)

		b.ReportMetric(baseline.TotalOpsPerSec, "baseline_ops_per_sec")
		b.ReportMetric(duringMerge.TotalOpsPerSec, "merge_ops_per_sec")
		b.ReportMetric(degradation.TotalDegradation, "degradation_pct")

		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeDir(dir)
	}
}
