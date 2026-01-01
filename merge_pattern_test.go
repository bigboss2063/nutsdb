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
	"path/filepath"
	"testing"
)

func TestMemoryBudgetScenarios(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100 * KB
	keySize := 16
	valueSize := 32
	dirPrefix := filepath.Join(t.TempDir(), "memory-budget")

	// Test memory budget scenarios
	scenarios := []MemoryBudgetScenario{
		{Name: "16MB", Budget: 16 * MB},
		{Name: "64MB", Budget: 64 * MB},
	}

	setup := func(db *DB, dir string) error {
		bucket := "test-bucket"
		if err := db.Update(func(tx *Tx) error {
			return tx.NewBucket(DataStructureBTree, bucket)
		}); err != nil {
			return err
		}

		value := bytes.Repeat([]byte("v"), valueSize)
		if err := db.Update(func(tx *Tx) error {
			for i := 0; i < 10000; i++ {
				key := makePatternKey(uint64(i), keySize)
				if err := tx.Put(bucket, key, value, Persistent); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}

		config := NewUpdatePatternConfig(UpdatePatternOverwriteHeavy)
		if err := GenerateUpdatePattern(db, bucket, 10000, config, keySize, valueSize); err != nil {
			return err
		}

		dataset := mergeBenchDataset{
			name:        "test",
			entries:     10000,
			keySize:     keySize,
			valueSize:   valueSize,
			segmentSize: opts.SegmentSize,
		}
		return ensureMultipleDataFilesForSetup(db, dir, bucket, dataset)
	}

	results, err := RunMemoryBudgetScenarios(opts, dirPrefix, scenarios, setup)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != len(scenarios) {
		t.Fatalf("expected %d results, got %d", len(scenarios), len(results))
	}

	// Verify results have valid data
	for i, result := range results {
		if result.Duration == 0 {
			t.Errorf("result %d: duration is zero", i)
		}
		if result.Scenario.Name != scenarios[i].Name {
			t.Errorf("result %d: expected scenario name %s, got %s",
				i, scenarios[i].Name, result.Scenario.Name)
		}
	}

	// Test optimal budget identification
	optimal := IdentifyOptimalBudget(results)
	if optimal == nil {
		t.Fatal("expected optimal budget, got nil")
	}

	t.Logf("Optimal budget: %s (duration: %v, batches: %d)",
		optimal.Scenario.Name, optimal.Duration, optimal.BatchCount)
}

func TestIdentifyOptimalBudget_EmptyResults(t *testing.T) {
	results := []MemoryBudgetResult{}
	optimal := IdentifyOptimalBudget(results)
	if optimal != nil {
		t.Error("expected nil for empty results")
	}
}
