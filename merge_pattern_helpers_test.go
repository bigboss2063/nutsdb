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
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"time"
)

// UpdatePattern defines different update patterns for merge benchmarking
type UpdatePattern string

const (
	// UpdatePatternOverwriteHeavy represents a pattern with 60-80% overwrites
	UpdatePatternOverwriteHeavy UpdatePattern = "overwrite-heavy"

	// UpdatePatternDeleteHeavy represents a pattern with 40-60% deletes
	UpdatePatternDeleteHeavy UpdatePattern = "delete-heavy"

	// UpdatePatternAppendOnly represents a pattern with no updates or deletes
	UpdatePatternAppendOnly UpdatePattern = "append-only"

	// UpdatePatternMixed represents a pattern with 30% overwrites, 20% deletes, 50% appends
	UpdatePatternMixed UpdatePattern = "mixed"
)

// UpdatePatternConfig configures the distribution of operations in an update pattern
type UpdatePatternConfig struct {
	Pattern      UpdatePattern
	OverwritePct float64 // Percentage of keys to overwrite (0.0-1.0)
	DeletePct    float64 // Percentage of keys to delete (0.0-1.0)
	AppendPct    float64 // Percentage of new keys to append (0.0-1.0)
}

// NewUpdatePatternConfig creates a config for the specified pattern
func NewUpdatePatternConfig(pattern UpdatePattern) UpdatePatternConfig {
	switch pattern {
	case UpdatePatternOverwriteHeavy:
		return UpdatePatternConfig{
			Pattern:      pattern,
			OverwritePct: 0.7, // 70% overwrites (within 60-80% range)
			DeletePct:    0.0,
			AppendPct:    0.0,
		}
	case UpdatePatternDeleteHeavy:
		return UpdatePatternConfig{
			Pattern:      pattern,
			OverwritePct: 0.0,
			DeletePct:    0.5, // 50% deletes (within 40-60% range)
			AppendPct:    0.0,
		}
	case UpdatePatternAppendOnly:
		return UpdatePatternConfig{
			Pattern:      pattern,
			OverwritePct: 0.0,
			DeletePct:    0.0,
			AppendPct:    1.0, // 100% appends
		}
	case UpdatePatternMixed:
		return UpdatePatternConfig{
			Pattern:      pattern,
			OverwritePct: 0.3, // 30% overwrites
			DeletePct:    0.2, // 20% deletes
			AppendPct:    0.5, // 50% appends
		}
	default:
		return UpdatePatternConfig{
			Pattern:      UpdatePatternAppendOnly,
			OverwritePct: 0.0,
			DeletePct:    0.0,
			AppendPct:    1.0,
		}
	}
}

// GenerateUpdatePattern applies the specified update pattern to a dataset.
func GenerateUpdatePattern(db *DB, bucket string, baseEntries int, config UpdatePatternConfig, keySize int, valueSize int) error {
	if baseEntries <= 0 || keySize < 8 {
		return ErrBucket
	}

	overwriteValue := bytes.Repeat([]byte("o"), valueSize)
	appendValue := bytes.Repeat([]byte("a"), valueSize)

	// Calculate operation counts
	overwriteCount := int(float64(baseEntries) * config.OverwritePct)
	deleteCount := int(float64(baseEntries) * config.DeletePct)
	appendCount := int(float64(baseEntries) * config.AppendPct)

	// Apply overwrites
	if overwriteCount > 0 {
		if err := applyOverwrites(db, bucket, overwriteCount, keySize, overwriteValue); err != nil {
			return err
		}
	}

	// Apply deletes
	if deleteCount > 0 {
		if err := applyDeletes(db, bucket, baseEntries, deleteCount, keySize); err != nil {
			return err
		}
	}

	// Apply appends
	if appendCount > 0 {
		if err := applyAppends(db, bucket, baseEntries, appendCount, keySize, appendValue); err != nil {
			return err
		}
	}

	return nil
}

func applyOverwrites(db *DB, bucket string, count int, keySize int, overwriteValue []byte) error {
	const batchSize = 10_000

	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}

		if err := db.Update(func(tx *Tx) error {
			for i := start; i < end; i++ {
				key := makePatternKey(uint64(i), keySize)
				if err := tx.Put(bucket, key, overwriteValue, Persistent); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func applyDeletes(db *DB, bucket string, baseEntries, count int, keySize int) error {
	const batchSize = 10_000
	deleteStart := baseEntries - count

	for start := deleteStart; start < baseEntries; start += batchSize {
		end := start + batchSize
		if end > baseEntries {
			end = baseEntries
		}

		if err := db.Update(func(tx *Tx) error {
			for i := start; i < end; i++ {
				key := makePatternKey(uint64(i), keySize)
				if err := tx.Delete(bucket, key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func applyAppends(db *DB, bucket string, baseEntries, count int, keySize int, appendValue []byte) error {
	const batchSize = 10_000
	appendStart := baseEntries

	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}

		if err := db.Update(func(tx *Tx) error {
			for i := start; i < end; i++ {
				key := makePatternKey(uint64(appendStart+i), keySize)
				if err := tx.Put(bucket, key, appendValue, Persistent); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

// makePatternKey creates a benchmark key with the given ID and size
func makePatternKey(id uint64, keySize int) []byte {
	key := make([]byte, keySize)
	copy(key, "benchkey")
	binary.BigEndian.PutUint64(key[keySize-8:], id)
	return key
}

func ensureMultipleDataFilesForSetup(db *DB, dir, bucket string, dataset mergeBenchDataset) error {
	fillerVal := bytes.Repeat([]byte("x"), dataset.valueSize)
	const minFiles = 3
	for attempts := 0; attempts < 10; attempts++ {
		count, err := countDataFiles(dir)
		if err != nil {
			return fmt.Errorf("count data files: %w", err)
		}
		if count >= minFiles {
			return nil
		}
		key := makeBenchKey(uint64(dataset.entries+attempts), dataset.keySize)
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, Persistent)
		}); err != nil {
			return err
		}
	}
	return fmt.Errorf("expected at least %d data files for merge setup", minFiles)
}

// MemoryBudgetScenario defines a memory budget configuration for merge benchmarking
type MemoryBudgetScenario struct {
	Name   string
	Budget int64 // Memory budget in bytes
}

// MemoryBudgetResult captures the results of a merge operation with a specific memory budget
type MemoryBudgetResult struct {
	Scenario   MemoryBudgetScenario
	Duration   time.Duration
	PeakMemory int64
	BatchCount int
	Efficiency float64 // Bytes processed per MB budget
}

// RunMemoryBudgetScenarios executes merge with different memory budgets on fresh datasets.
func RunMemoryBudgetScenarios(opts Options, dirPrefix string, scenarios []MemoryBudgetScenario, setup func(db *DB, dir string) error) ([]MemoryBudgetResult, error) {
	if len(scenarios) == 0 {
		return nil, ErrBucket
	}

	results := make([]MemoryBudgetResult, 0, len(scenarios))

	for idx, scenario := range scenarios {
		dir := filepath.Join(dirPrefix, fmt.Sprintf("scenario-%s-%d", scenario.Name, idx))
		removeDir(dir)

		scenarioOpts := opts
		scenarioOpts.Dir = dir
		scenarioOpts.MergeLookupMemoryBudget = scenario.Budget

		db, err := Open(scenarioOpts)
		if err != nil {
			return nil, err
		}

		if setup != nil {
			if err := setup(db, dir); err != nil {
				_ = db.Close()
				removeDir(dir)
				return nil, err
			}
		}

		originalBytes, err := dataFileBytes(dir)
		if err != nil {
			_ = db.Close()
			removeDir(dir)
			return nil, err
		}

		startTime := time.Now()
		if err := db.Merge(); err != nil {
			_ = db.Close()
			removeDir(dir)
			return nil, err
		}
		duration := time.Since(startTime)

		diag := db.GetEnhancedMergeDiagnostics()
		peakMemory := diag.PeakLookupBytes
		batchCount := diag.Batches
		bytesProcessed := originalBytes

		efficiency := 0.0
		if scenario.Budget > 0 && bytesProcessed > 0 {
			budgetMB := float64(scenario.Budget) / (1024 * 1024)
			efficiency = float64(bytesProcessed) / budgetMB
		}

		results = append(results, MemoryBudgetResult{
			Scenario:   scenario,
			Duration:   duration,
			PeakMemory: peakMemory,
			BatchCount: batchCount,
			Efficiency: efficiency,
		})

		if err := db.Close(); err != nil {
			removeDir(dir)
			return nil, err
		}
		removeDir(dir)
	}

	return results, nil
}

// IdentifyOptimalBudget analyzes memory budget results and identifies the optimal configuration
func IdentifyOptimalBudget(results []MemoryBudgetResult) *MemoryBudgetResult {
	if len(results) == 0 {
		return nil
	}

	// Find the budget with the best balance of duration and memory usage
	var optimal *MemoryBudgetResult
	bestScore := float64(-1)

	for i := range results {
		result := &results[i]

		// Skip if no valid data
		if result.Duration == 0 {
			continue
		}

		// Calculate score: prioritize lower duration and fewer batches
		durationScore := 1.0 / result.Duration.Seconds()
		batchScore := 1.0
		if result.BatchCount > 0 {
			batchScore = 1.0 / float64(result.BatchCount)
		}

		// Combined score (weighted: 60% duration, 40% batch count)
		score := (durationScore * 0.6) + (batchScore * 0.4)

		if score > bestScore {
			bestScore = score
			optimal = result
		}
	}

	return optimal
}
