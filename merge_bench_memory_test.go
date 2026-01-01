package nutsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const (
	memoryBudget16MB  = 16 * MB
	memoryBudget64MB  = 64 * MB
	memoryBudget256MB = 256 * MB
	memoryBudget1GB   = 1024 * MB
)

// BenchmarkMergeMemoryBudget benchmarks merge operations with different memory budgets
func BenchmarkMergeMemoryBudget(b *testing.B) {
	datasets := []mergeBenchDataset{
		{name: "small-100k", entries: 100_000, keySize: 16, valueSize: 128, segmentSize: 8 * MB},
		{name: "medium-500k", entries: 500_000, keySize: 16, valueSize: 128, segmentSize: 16 * MB},
		{name: "large-1m", entries: 1_000_000, keySize: 16, valueSize: 512, segmentSize: 32 * MB},
	}

	budgets := []struct {
		name   string
		budget int64
	}{
		{name: "16MB", budget: memoryBudget16MB},
		{name: "64MB", budget: memoryBudget64MB},
		{name: "256MB", budget: memoryBudget256MB},
		{name: "1GB", budget: memoryBudget1GB},
	}

	for _, dataset := range datasets {
		dataset := dataset
		for _, budget := range budgets {
			budget := budget
			name := fmt.Sprintf("%s/%s", dataset.name, budget.name)
			b.Run(name, func(b *testing.B) {
				benchmarkMergeMemoryBudget(b, dataset, budget.budget)
			})
		}
	}
}

func benchmarkMergeMemoryBudget(b *testing.B, dataset mergeBenchDataset, memoryBudget int64) {
	b.ReportAllocs()
	b.ResetTimer()

	var diagAgg mergeBenchDiagnosticsAgg
	var totalBytesProcessed int64

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-memory-%s-%d-%d", dataset.name, memoryBudget, i))
		removeDir(dir)

		opts := DefaultOptions
		opts.Dir = dir
		opts.SegmentSize = dataset.segmentSize
		opts.EnableHintFile = true
		opts.MergeLookupMemoryBudget = memoryBudget

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

		// Populate dataset with updates and deletes
		populateMemoryBenchData(b, db, bucket, dataset)

		// Ensure multiple data files for merge
		ensureMultipleDataFiles(b, db, dir, bucket, dataset)
		originalBytes, err := dataFileBytes(dir)
		if err != nil {
			b.Fatalf("measure original bytes: %v", err)
		}

		b.StartTimer()
		if err := db.Merge(); err != nil {
			b.Fatalf("merge: %v", err)
		}
		b.StopTimer()

		finalBytes, err := dataFileBytes(dir)
		if err != nil {
			b.Fatalf("measure final bytes: %v", err)
		}
		diagAgg.add(db.GetEnhancedMergeDiagnostics(), originalBytes, finalBytes)
		totalBytesProcessed += originalBytes

		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeDir(dir)
	}

	diagAgg.report(b)

	// Calculate and report memory efficiency (bytes processed per MB budget)
	if memoryBudget > 0 && diagAgg.count > 0 {
		avgBytesProcessed := float64(totalBytesProcessed) / float64(diagAgg.count)
		budgetMB := float64(memoryBudget) / (1024 * 1024)
		efficiency := avgBytesProcessed / budgetMB
		b.ReportMetric(efficiency, "bytes_per_mb_budget")
	}
}

func populateMemoryBenchData(b *testing.B, db *DB, bucket string, dataset mergeBenchDataset) {
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

// BenchmarkMergeMemoryOptimal identifies the optimal memory budget for each dataset
func BenchmarkMergeMemoryOptimal(b *testing.B) {
	datasets := []mergeBenchDataset{
		{name: "small-100k", entries: 100_000, keySize: 16, valueSize: 128, segmentSize: 8 * MB},
		{name: "medium-500k", entries: 500_000, keySize: 16, valueSize: 128, segmentSize: 16 * MB},
		{name: "large-1m", entries: 1_000_000, keySize: 16, valueSize: 512, segmentSize: 32 * MB},
	}

	for _, dataset := range datasets {
		dataset := dataset
		b.Run(dataset.name, func(b *testing.B) {
			benchmarkMergeMemoryOptimal(b, dataset)
		})
	}
}

func benchmarkMergeMemoryOptimal(b *testing.B, dataset mergeBenchDataset) {
	b.ReportAllocs()

	scenarios := []MemoryBudgetScenario{
		{Name: "16MB", Budget: memoryBudget16MB},
		{Name: "64MB", Budget: memoryBudget64MB},
		{Name: "256MB", Budget: memoryBudget256MB},
		{Name: "1GB", Budget: memoryBudget1GB},
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dirPrefix := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-optimal-%s-%d", dataset.name, i))
		removeDir(dirPrefix)

		opts := DefaultOptions
		opts.SegmentSize = dataset.segmentSize
		opts.EnableHintFile = true

		setup := func(db *DB, dir string) error {
			bucket := "bench"
			if err := db.Update(func(tx *Tx) error {
				return tx.NewBucket(DataStructureBTree, bucket)
			}); err != nil {
				return err
			}
			populateMemoryBenchData(b, db, bucket, dataset)
			ensureMultipleDataFiles(b, db, dir, bucket, dataset)
			return nil
		}

		b.StartTimer()
		results, err := RunMemoryBudgetScenarios(opts, dirPrefix, scenarios, setup)
		b.StopTimer()

		if err != nil {
			b.Fatalf("run memory budget scenarios: %v", err)
		}

		// Identify optimal budget
		optimal := IdentifyOptimalBudget(results)
		if optimal != nil {
			b.Logf("Optimal budget for %s: %s (duration: %v, batches: %d, efficiency: %.2f)",
				dataset.name, optimal.Scenario.Name, optimal.Duration, optimal.BatchCount, optimal.Efficiency)
		}

		removeDir(dirPrefix)
	}
}
