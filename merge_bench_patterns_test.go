package nutsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkMergePatterns benchmarks merge operations with different update patterns
func BenchmarkMergePatterns(b *testing.B) {
	datasets := []mergeBenchDataset{
		{name: "small-100k", entries: 100_000, keySize: 16, valueSize: 128, segmentSize: 8 * MB},
		{name: "medium-500k", entries: 500_000, keySize: 16, valueSize: 128, segmentSize: 16 * MB},
		{name: "large-1m", entries: 1_000_000, keySize: 16, valueSize: 512, segmentSize: 32 * MB},
	}

	patterns := []UpdatePattern{
		UpdatePatternOverwriteHeavy,
		UpdatePatternDeleteHeavy,
		UpdatePatternAppendOnly,
		UpdatePatternMixed,
	}

	hintModes := []struct {
		name    string
		enabled bool
	}{
		{name: "hint", enabled: true},
		{name: "nohint", enabled: false},
	}

	for _, dataset := range datasets {
		dataset := dataset
		for _, pattern := range patterns {
			pattern := pattern
			for _, hint := range hintModes {
				hint := hint
				name := fmt.Sprintf("%s/%s/%s", dataset.name, pattern, hint.name)
				b.Run(name, func(b *testing.B) {
					benchmarkMergePattern(b, dataset, pattern, hint.enabled)
				})
			}
		}
	}
}

func benchmarkMergePattern(b *testing.B, dataset mergeBenchDataset, pattern UpdatePattern, hintEnabled bool) {
	b.ReportAllocs()
	b.ResetTimer()

	var diagAgg mergeBenchDiagnosticsAgg

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-pattern-%s-%s-%t-%d", dataset.name, pattern, hintEnabled, i))
		removeDir(dir)

		opts := DefaultOptions
		opts.Dir = dir
		opts.SegmentSize = dataset.segmentSize
		opts.EnableHintFile = hintEnabled

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

		// Populate initial dataset
		populatePatternBenchData(b, db, bucket, dataset)

		// Apply update pattern
		config := NewUpdatePatternConfig(pattern)
		if err := GenerateUpdatePattern(db, bucket, dataset.entries, config, dataset.keySize, dataset.valueSize); err != nil {
			b.Fatalf("generate update pattern: %v", err)
		}

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

		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeDir(dir)
	}

	diagAgg.report(b)
}

func populatePatternBenchData(b *testing.B, db *DB, bucket string, dataset mergeBenchDataset) {
	const batchSize = 10_000
	value := bytes.Repeat([]byte("v"), dataset.valueSize)

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
}
