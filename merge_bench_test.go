package nutsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	mergeBenchLatencySampleRate = 20
	mergeBenchLatencySampleSize = 4096
	mergeBenchLoadStartTimeout  = 2 * time.Second
)

type mergeBenchDataset struct {
	name        string
	entries     int
	keySize     int
	valueSize   int
	segmentSize int64
}

type mergeBenchScenario struct {
	name    string
	readers int
	writers int
}

type mergeBenchDiagnosticsAgg struct {
	count       int
	peakSum     int64
	lockHoldSum time.Duration
	batchSum    int
	originalSum int64
	finalSum    int64
}

func (agg *mergeBenchDiagnosticsAgg) add(diag EnhancedMergeDiagnostics, originalBytes, finalBytes int64) {
	agg.count++
	agg.peakSum += diag.PeakLookupBytes
	agg.lockHoldSum += diag.MaxLockHold
	agg.batchSum += diag.Batches
	agg.originalSum += originalBytes
	agg.finalSum += finalBytes
}

func (agg *mergeBenchDiagnosticsAgg) report(b *testing.B) {
	if agg.count == 0 {
		return
	}
	b.ReportMetric(float64(agg.peakSum)/float64(agg.count), "peak_lookup_bytes")
	b.ReportMetric(float64(agg.lockHoldSum.Microseconds())/float64(agg.count), "max_lock_hold_us")
	b.ReportMetric(float64(agg.batchSum)/float64(agg.count), "batch_count")

	// Calculate and report reduction ratio as percentage
	if agg.originalSum > 0 {
		reductionRatio := float64(agg.originalSum-agg.finalSum) / float64(agg.originalSum) * 100.0
		b.ReportMetric(reductionRatio, "reduction_ratio_pct")
	}
}

type mergeBenchLoadMetrics struct {
	readOps        uint64
	writeOps       uint64
	readMaxUs      int64
	writeMaxUs     int64
	readSamples    []int64
	writeSamples   []int64
	readSampleIdx  uint64
	writeSampleIdx uint64
}

func newMergeBenchLoadMetrics() *mergeBenchLoadMetrics {
	return &mergeBenchLoadMetrics{
		readSamples:  make([]int64, mergeBenchLatencySampleSize),
		writeSamples: make([]int64, mergeBenchLatencySampleSize),
	}
}

func (m *mergeBenchLoadMetrics) recordReadSample(d time.Duration) {
	latUs := d.Microseconds()
	updateMaxInt64(&m.readMaxUs, latUs)
	idx := atomic.AddUint64(&m.readSampleIdx, 1) - 1
	if idx < uint64(len(m.readSamples)) {
		m.readSamples[idx] = latUs
	}
}

func (m *mergeBenchLoadMetrics) recordWriteSample(d time.Duration) {
	latUs := d.Microseconds()
	updateMaxInt64(&m.writeMaxUs, latUs)
	idx := atomic.AddUint64(&m.writeSampleIdx, 1) - 1
	if idx < uint64(len(m.writeSamples)) {
		m.writeSamples[idx] = latUs
	}
}

func (m *mergeBenchLoadMetrics) readSampleSlice() []int64 {
	n := atomic.LoadUint64(&m.readSampleIdx)
	if n > uint64(len(m.readSamples)) {
		n = uint64(len(m.readSamples))
	}
	return append([]int64(nil), m.readSamples[:n]...)
}

func (m *mergeBenchLoadMetrics) writeSampleSlice() []int64 {
	n := atomic.LoadUint64(&m.writeSampleIdx)
	if n > uint64(len(m.writeSamples)) {
		n = uint64(len(m.writeSamples))
	}
	return append([]int64(nil), m.writeSamples[:n]...)
}

type mergeBenchLoadAgg struct {
	readOps       uint64
	writeOps      uint64
	readMaxUs     int64
	writeMaxUs    int64
	readSamples   []int64
	writeSamples  []int64
	totalDuration time.Duration
}

func (agg *mergeBenchLoadAgg) add(metrics *mergeBenchLoadMetrics, duration time.Duration) {
	agg.totalDuration += duration
	agg.readOps += atomic.LoadUint64(&metrics.readOps)
	agg.writeOps += atomic.LoadUint64(&metrics.writeOps)

	if max := atomic.LoadInt64(&metrics.readMaxUs); max > agg.readMaxUs {
		agg.readMaxUs = max
	}
	if max := atomic.LoadInt64(&metrics.writeMaxUs); max > agg.writeMaxUs {
		agg.writeMaxUs = max
	}

	agg.readSamples = append(agg.readSamples, metrics.readSampleSlice()...)
	agg.writeSamples = append(agg.writeSamples, metrics.writeSampleSlice()...)
}

func (agg *mergeBenchLoadAgg) report(b *testing.B) {
	if agg.totalDuration <= 0 {
		return
	}
	if agg.readOps > 0 {
		b.ReportMetric(float64(agg.readOps)/agg.totalDuration.Seconds(), "read_ops_per_sec")
		if p95 := percentile(agg.readSamples, 0.95); p95 > 0 {
			b.ReportMetric(float64(p95), "read_p95_us")
		}
		b.ReportMetric(float64(agg.readMaxUs), "read_max_us")
	}
	if agg.writeOps > 0 {
		b.ReportMetric(float64(agg.writeOps)/agg.totalDuration.Seconds(), "write_ops_per_sec")
		if p95 := percentile(agg.writeSamples, 0.95); p95 > 0 {
			b.ReportMetric(float64(p95), "write_p95_us")
		}
		b.ReportMetric(float64(agg.writeMaxUs), "write_max_us")
	}
}

func BenchmarkMerge(b *testing.B) {
	datasets := []mergeBenchDataset{
		{name: "small-100k-128b", entries: 100_000, keySize: 16, valueSize: 128, segmentSize: 8 * MB},
		{name: "medium-500k-128b", entries: 500_000, keySize: 16, valueSize: 128, segmentSize: 16 * MB},
		{name: "large-1m-512b", entries: 1_000_000, keySize: 16, valueSize: 512, segmentSize: 32 * MB},
	}

	scenarios := []mergeBenchScenario{
		{name: "baseline", readers: 0, writers: 0},
		{name: "read", readers: 2, writers: 0},
		{name: "write", readers: 0, writers: 1},
		{name: "readwrite", readers: 2, writers: 1},
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
		for _, hint := range hintModes {
			hint := hint
			for _, scenario := range scenarios {
				scenario := scenario
				name := fmt.Sprintf("%s/%s/%s", dataset.name, hint.name, scenario.name)
				b.Run(name, func(b *testing.B) {
					benchmarkMergeScenario(b, dataset, scenario, hint.enabled)
				})
			}
		}
	}
}

func benchmarkMergeScenario(b *testing.B, dataset mergeBenchDataset, scenario mergeBenchScenario, hintEnabled bool) {
	b.ReportAllocs()
	b.ResetTimer()

	var diagAgg mergeBenchDiagnosticsAgg
	var loadAgg mergeBenchLoadAgg

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-bench-%s-%s-%t-%d", dataset.name, scenario.name, hintEnabled, i))
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

		populateMergeBenchData(b, db, bucket, dataset)
		ensureMultipleDataFiles(b, db, dir, bucket, dataset)
		originalBytes, err := dataFileBytes(dir)
		if err != nil {
			b.Fatalf("measure original bytes: %v", err)
		}

		hotKeys := buildMergeBenchHotKeys(dataset)
		writeValue := bytes.Repeat([]byte("w"), dataset.valueSize)

		stopCh := make(chan struct{})
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		var loadStarted uint32
		loadMetrics := newMergeBenchLoadMetrics()
		startMergeBenchLoad(db, bucket, scenario, hotKeys, writeValue, stopCh, startCh, &wg, &loadStarted, loadMetrics)
		waitForMergeBenchLoadStart(scenario.readers+scenario.writers, &loadStarted)

		b.StartTimer()
		close(startCh)
		mergeStart := time.Now()
		if err := db.Merge(); err != nil {
			b.Fatalf("merge: %v", err)
		}
		mergeDuration := time.Since(mergeStart)
		b.StopTimer()

		finalBytes, err := dataFileBytes(dir)
		if err != nil {
			b.Fatalf("measure final bytes: %v", err)
		}
		diagAgg.add(db.GetEnhancedMergeDiagnostics(), originalBytes, finalBytes)
		loadAgg.add(loadMetrics, mergeDuration)

		close(stopCh)
		wg.Wait()

		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeDir(dir)
	}

	diagAgg.report(b)
	loadAgg.report(b)
}

func populateMergeBenchData(b *testing.B, db *DB, bucket string, dataset mergeBenchDataset) {
	const batchSize = 10_000
	value := bytes.Repeat([]byte("v"), dataset.valueSize)
	updateValue := bytes.Repeat([]byte("u"), dataset.valueSize)

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

	updateCount := mergeBenchUpdateCount(dataset.entries)
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

	deleteStart := updateCount
	deleteEnd := updateCount + mergeBenchDeleteCount(dataset.entries)
	if deleteEnd > dataset.entries {
		deleteEnd = dataset.entries
	}
	for start := deleteStart; start < deleteEnd; start += batchSize {
		end := start + batchSize
		if end > deleteEnd {
			end = deleteEnd
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

func ensureMultipleDataFiles(b *testing.B, db *DB, dir, bucket string, dataset mergeBenchDataset) {
	fillerVal := bytes.Repeat([]byte("x"), dataset.valueSize)
	minFiles := 3
	for attempts := 0; attempts < 10; attempts++ {
		count, err := countDataFiles(dir)
		if err != nil {
			b.Fatalf("count data files: %v", err)
		}
		if count >= minFiles {
			return
		}
		key := makeBenchKey(uint64(dataset.entries+attempts), dataset.keySize)
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, Persistent)
		}); err != nil {
			b.Fatalf("add extra entry: %v", err)
		}
	}
	b.Fatalf("expected at least %d data files for merge benchmark", minFiles)
}

func startMergeBenchLoad(db *DB, bucket string, scenario mergeBenchScenario, hotKeys [][]byte, writeValue []byte, stopCh <-chan struct{}, startCh <-chan struct{}, wg *sync.WaitGroup, started *uint32, metrics *mergeBenchLoadMetrics) {
	if len(hotKeys) == 0 {
		return
	}

	for i := 0; i < scenario.readers; i++ {
		seed := int64(100 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if started != nil {
				atomic.AddUint32(started, 1)
			}
			if startCh != nil {
				select {
				case <-startCh:
				case <-stopCh:
					return
				}
			}
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				ops := atomic.AddUint64(&metrics.readOps, 1)
				key := hotKeys[rng.Intn(len(hotKeys))]
				if ops%mergeBenchLatencySampleRate == 0 {
					start := time.Now()
					_ = db.View(func(tx *Tx) error {
						_, _ = tx.Get(bucket, key)
						return nil
					})
					metrics.recordReadSample(time.Since(start))
					continue
				}
				_ = db.View(func(tx *Tx) error {
					_, _ = tx.Get(bucket, key)
					return nil
				})
			}
		}()
	}

	for i := 0; i < scenario.writers; i++ {
		seed := int64(200 + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if started != nil {
				atomic.AddUint32(started, 1)
			}
			if startCh != nil {
				select {
				case <-startCh:
				case <-stopCh:
					return
				}
			}
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				ops := atomic.AddUint64(&metrics.writeOps, 1)
				key := hotKeys[rng.Intn(len(hotKeys))]
				if ops%mergeBenchLatencySampleRate == 0 {
					start := time.Now()
					_ = db.Update(func(tx *Tx) error {
						return tx.Put(bucket, key, writeValue, Persistent)
					})
					metrics.recordWriteSample(time.Since(start))
					continue
				}
				_ = db.Update(func(tx *Tx) error {
					return tx.Put(bucket, key, writeValue, Persistent)
				})
			}
		}()
	}
}

func waitForMergeBenchLoadStart(total int, started *uint32) {
	if total <= 0 || started == nil {
		return
	}

	deadline := time.Now().Add(mergeBenchLoadStartTimeout)
	for {
		if int(atomic.LoadUint32(started)) >= total {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func buildMergeBenchHotKeys(dataset mergeBenchDataset) [][]byte {
	size := mergeBenchHotsetSize(dataset.entries)
	keys := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = makeBenchKey(uint64(i), dataset.keySize)
	}
	return keys
}

func mergeBenchUpdateCount(entries int) int {
	count := entries / 3
	if count < 1 {
		return 1
	}
	return count
}

func mergeBenchDeleteCount(entries int) int {
	count := entries / 10
	if count < 1 {
		return 1
	}
	return count
}

func mergeBenchHotsetSize(entries int) int {
	size := entries / 100
	if size < 1 {
		size = 1
	}
	if size > 10_000 {
		size = 10_000
	}
	return size
}

func percentile(samples []int64, p float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	if p <= 0 {
		return samples[0]
	}
	if p >= 1 {
		return samples[len(samples)-1]
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	idx := int(math.Ceil(p*float64(len(samples)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return samples[idx]
}

func updateMaxInt64(target *int64, value int64) {
	for {
		cur := atomic.LoadInt64(target)
		if value <= cur {
			return
		}
		if atomic.CompareAndSwapInt64(target, cur, value) {
			return
		}
	}
}

func makeBenchKey(id uint64, keySize int) []byte {
	key := make([]byte, keySize)
	copy(key, "benchkey")
	binary.BigEndian.PutUint64(key[keySize-8:], id)
	return key
}
