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
	"math"
	"os"
	"testing"
	"time"
)

func TestConcurrentLoadBasic(t *testing.T) {
	dir := "/tmp/test-concurrent-load"
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024 * 1024

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	bucket := "test-bucket"

	// Prepare test data
	hotKeys := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		hotKeys[i] = []byte("hotkey_" + string(rune('0'+i)))
	}

	// Insert initial data
	err = db.Update(func(tx *Tx) error {
		// Create bucket first
		if err := tx.NewBucket(DataStructureBTree, bucket); err != nil && err != ErrBucketAlreadyExist {
			return err
		}
		for _, key := range hotKeys {
			if err := tx.Put(bucket, key, []byte("initial_value"), Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Start concurrent load
	config := ConcurrentLoadConfig{
		Readers:    2,
		Writers:    1,
		HotKeys:    hotKeys,
		WriteValue: []byte("updated_value"),
		SampleRate: 10,
	}

	controller, err := StartConcurrentLoad(db, bucket, config)
	if err != nil {
		t.Fatalf("Failed to start concurrent load: %v", err)
	}

	// Let it run for a short time
	time.Sleep(100 * time.Millisecond)

	// Stop and collect metrics
	controller.Stop()
	metrics := controller.Metrics()

	// Verify metrics
	if metrics.ReadOps == 0 {
		t.Error("Expected some read operations")
	}

	if metrics.WriteOps == 0 {
		t.Error("Expected some write operations")
	}

	if metrics.Duration == 0 {
		t.Error("Expected non-zero duration")
	}

	t.Logf("Metrics: ReadOps=%d, WriteOps=%d, Duration=%v",
		metrics.ReadOps, metrics.WriteOps, metrics.Duration)
}

func TestLatencySampler(t *testing.T) {
	sampler := newLatencySampler(100)

	// Add samples
	for i := 0; i < 200; i++ {
		latency := time.Duration(i) * time.Millisecond
		sampler.sample(latency, 1)
	}

	// Should only have 100 samples due to buffer limit
	stats := sampler.calculateStats()
	if len(stats.Samples) != 100 {
		t.Errorf("Expected 100 samples, got %d", len(stats.Samples))
	}

	// Verify percentiles are calculated
	if stats.P50 == 0 {
		t.Error("Expected non-zero P50")
	}
	if stats.P95 == 0 {
		t.Error("Expected non-zero P95")
	}
	if stats.P99 == 0 {
		t.Error("Expected non-zero P99")
	}
	if stats.Max == 0 {
		t.Error("Expected non-zero Max")
	}

	// Verify ordering: P50 < P95 < P99 <= Max
	if stats.P50 >= stats.P95 {
		t.Errorf("Expected P50 < P95, got P50=%v, P95=%v", stats.P50, stats.P95)
	}
	if stats.P95 >= stats.P99 {
		t.Errorf("Expected P95 < P99, got P95=%v, P99=%v", stats.P95, stats.P99)
	}
	if stats.P99 > stats.Max {
		t.Errorf("Expected P99 <= Max, got P99=%v, Max=%v", stats.P99, stats.Max)
	}
}

func TestLatencySamplerWithSampleRate(t *testing.T) {
	sampler := newLatencySampler(1000)

	// Add 100 samples with sample rate of 10 (should record 10 samples)
	for i := 0; i < 100; i++ {
		latency := time.Duration(i) * time.Millisecond
		sampler.sample(latency, 10)
	}

	stats := sampler.calculateStats()
	if len(stats.Samples) != 10 {
		t.Errorf("Expected 10 samples with rate 10, got %d", len(stats.Samples))
	}
}

func TestCalculateThroughput(t *testing.T) {
	metrics := ConcurrentLoadMetrics{
		ReadOps:  1000,
		WriteOps: 500,
		Duration: 10 * time.Second,
	}

	throughput := CalculateThroughput(metrics)

	expectedReadOps := 100.0
	expectedWriteOps := 50.0
	expectedTotalOps := 150.0

	if throughput.ReadOpsPerSec != expectedReadOps {
		t.Errorf("Expected ReadOpsPerSec=%.2f, got %.2f", expectedReadOps, throughput.ReadOpsPerSec)
	}

	if throughput.WriteOpsPerSec != expectedWriteOps {
		t.Errorf("Expected WriteOpsPerSec=%.2f, got %.2f", expectedWriteOps, throughput.WriteOpsPerSec)
	}

	if throughput.TotalOpsPerSec != expectedTotalOps {
		t.Errorf("Expected TotalOpsPerSec=%.2f, got %.2f", expectedTotalOps, throughput.TotalOpsPerSec)
	}
}

func TestCalculateThroughputZeroDuration(t *testing.T) {
	metrics := ConcurrentLoadMetrics{
		ReadOps:  1000,
		WriteOps: 500,
		Duration: 0,
	}

	throughput := CalculateThroughput(metrics)

	if throughput.ReadOpsPerSec != 0 {
		t.Errorf("Expected ReadOpsPerSec=0 for zero duration, got %.2f", throughput.ReadOpsPerSec)
	}

	if throughput.WriteOpsPerSec != 0 {
		t.Errorf("Expected WriteOpsPerSec=0 for zero duration, got %.2f", throughput.WriteOpsPerSec)
	}

	if throughput.TotalOpsPerSec != 0 {
		t.Errorf("Expected TotalOpsPerSec=0 for zero duration, got %.2f", throughput.TotalOpsPerSec)
	}
}

func TestMeasureBaselineThroughput(t *testing.T) {
	dir := "/tmp/test-baseline-throughput"
	defer os.RemoveAll(dir)

	opts := DefaultOptions
	opts.Dir = dir
	opts.SegmentSize = 8 * 1024 * 1024

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	bucket := "test-bucket"

	// Prepare test data
	hotKeys := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		hotKeys[i] = []byte("hotkey_" + string(rune('0'+i)))
	}

	// Insert initial data
	err = db.Update(func(tx *Tx) error {
		// Create bucket first
		if err := tx.NewBucket(DataStructureBTree, bucket); err != nil && err != ErrBucketAlreadyExist {
			return err
		}
		for _, key := range hotKeys {
			if err := tx.Put(bucket, key, []byte("initial_value"), Persistent); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Measure baseline throughput
	config := ConcurrentLoadConfig{
		Readers:    2,
		Writers:    1,
		HotKeys:    hotKeys,
		WriteValue: []byte("updated_value"),
		SampleRate: 10,
	}

	throughput, err := MeasureBaselineThroughput(db, bucket, config, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to measure baseline throughput: %v", err)
	}

	// Verify throughput metrics are non-zero
	if throughput.ReadOpsPerSec == 0 {
		t.Error("Expected non-zero ReadOpsPerSec")
	}

	if throughput.WriteOpsPerSec == 0 {
		t.Error("Expected non-zero WriteOpsPerSec")
	}

	if throughput.TotalOpsPerSec == 0 {
		t.Error("Expected non-zero TotalOpsPerSec")
	}

	// Verify total ops is sum of read and write ops (within tolerance)
	expectedTotal := throughput.ReadOpsPerSec + throughput.WriteOpsPerSec
	if math.Abs(throughput.TotalOpsPerSec-expectedTotal) > 1e-6 {
		t.Errorf("Expected TotalOpsPerSec=%.2f (sum of read+write), got %.2f",
			expectedTotal, throughput.TotalOpsPerSec)
	}

	t.Logf("Baseline Throughput: ReadOps=%.2f/s, WriteOps=%.2f/s, Total=%.2f/s",
		throughput.ReadOpsPerSec, throughput.WriteOpsPerSec, throughput.TotalOpsPerSec)
}

func TestCalculateDegradation(t *testing.T) {
	baseline := ThroughputMetrics{
		ReadOpsPerSec:  1000.0,
		WriteOpsPerSec: 500.0,
		TotalOpsPerSec: 1500.0,
	}

	duringMerge := ThroughputMetrics{
		ReadOpsPerSec:  800.0,
		WriteOpsPerSec: 400.0,
		TotalOpsPerSec: 1200.0,
	}

	degradation := CalculateDegradation(baseline, duringMerge)

	// Expected degradation: (1000 - 800) / 1000 * 100 = 20%
	expectedReadDeg := 20.0
	if degradation.ReadDegradation != expectedReadDeg {
		t.Errorf("Expected ReadDegradation=%.2f%%, got %.2f%%", expectedReadDeg, degradation.ReadDegradation)
	}

	// Expected degradation: (500 - 400) / 500 * 100 = 20%
	expectedWriteDeg := 20.0
	if degradation.WriteDegradation != expectedWriteDeg {
		t.Errorf("Expected WriteDegradation=%.2f%%, got %.2f%%", expectedWriteDeg, degradation.WriteDegradation)
	}

	// Expected degradation: (1500 - 1200) / 1500 * 100 = 20%
	expectedTotalDeg := 20.0
	if degradation.TotalDegradation != expectedTotalDeg {
		t.Errorf("Expected TotalDegradation=%.2f%%, got %.2f%%", expectedTotalDeg, degradation.TotalDegradation)
	}

	// Verify baseline and duringMerge are stored
	if degradation.Baseline.TotalOpsPerSec != baseline.TotalOpsPerSec {
		t.Error("Baseline metrics not stored correctly")
	}

	if degradation.DuringMerge.TotalOpsPerSec != duringMerge.TotalOpsPerSec {
		t.Error("DuringMerge metrics not stored correctly")
	}
}

func TestCalculateDegradationZeroBaseline(t *testing.T) {
	baseline := ThroughputMetrics{
		ReadOpsPerSec:  0,
		WriteOpsPerSec: 0,
		TotalOpsPerSec: 0,
	}

	duringMerge := ThroughputMetrics{
		ReadOpsPerSec:  100.0,
		WriteOpsPerSec: 50.0,
		TotalOpsPerSec: 150.0,
	}

	degradation := CalculateDegradation(baseline, duringMerge)

	// With zero baseline, degradation should be 0
	if degradation.ReadDegradation != 0 {
		t.Errorf("Expected ReadDegradation=0 for zero baseline, got %.2f%%", degradation.ReadDegradation)
	}

	if degradation.WriteDegradation != 0 {
		t.Errorf("Expected WriteDegradation=0 for zero baseline, got %.2f%%", degradation.WriteDegradation)
	}

	if degradation.TotalDegradation != 0 {
		t.Errorf("Expected TotalDegradation=0 for zero baseline, got %.2f%%", degradation.TotalDegradation)
	}
}

func TestCalculateDegradationImprovement(t *testing.T) {
	baseline := ThroughputMetrics{
		ReadOpsPerSec:  1000.0,
		WriteOpsPerSec: 500.0,
		TotalOpsPerSec: 1500.0,
	}

	// During merge is better than baseline (negative degradation)
	duringMerge := ThroughputMetrics{
		ReadOpsPerSec:  1200.0,
		WriteOpsPerSec: 600.0,
		TotalOpsPerSec: 1800.0,
	}

	degradation := CalculateDegradation(baseline, duringMerge)

	// Expected degradation: (1000 - 1200) / 1000 * 100 = -20% (improvement)
	expectedReadDeg := -20.0
	if degradation.ReadDegradation != expectedReadDeg {
		t.Errorf("Expected ReadDegradation=%.2f%%, got %.2f%%", expectedReadDeg, degradation.ReadDegradation)
	}

	// Expected degradation: (500 - 600) / 500 * 100 = -20% (improvement)
	expectedWriteDeg := -20.0
	if degradation.WriteDegradation != expectedWriteDeg {
		t.Errorf("Expected WriteDegradation=%.2f%%, got %.2f%%", expectedWriteDeg, degradation.WriteDegradation)
	}

	// Expected degradation: (1500 - 1800) / 1500 * 100 = -20% (improvement)
	expectedTotalDeg := -20.0
	if degradation.TotalDegradation != expectedTotalDeg {
		t.Errorf("Expected TotalDegradation=%.2f%%, got %.2f%%", expectedTotalDeg, degradation.TotalDegradation)
	}
}
