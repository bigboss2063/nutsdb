package nutsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

type mergeBenchScenario struct {
	name    string
	readers int
	writers int
}

func BenchmarkMerge(b *testing.B) {
	scenarios := []mergeBenchScenario{
		{name: "baseline", readers: 0, writers: 0},
		{name: "read", readers: 2, writers: 0},
		{name: "write", readers: 0, writers: 1},
		{name: "readwrite", readers: 2, writers: 1},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkMergeScenario(b, scenario)
		})
	}
}

func benchmarkMergeScenario(b *testing.B, scenario mergeBenchScenario) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-bench-%s-%d", scenario.name, i))
		removeDir(dir)

		opts := DefaultOptions
		opts.Dir = dir
		opts.SegmentSize = 32 * KB
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

		populateMergeBenchData(b, db, bucket)
		ensureMultipleDataFiles(b, db, dir, bucket)

		stopCh := make(chan struct{})
		var wg sync.WaitGroup
		startMergeBenchLoad(db, bucket, scenario, stopCh, &wg)

		b.StartTimer()
		if err := db.Merge(); err != nil {
			b.Fatalf("merge: %v", err)
		}
		b.StopTimer()

		close(stopCh)
		wg.Wait()

		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		removeDir(dir)
	}
}

func populateMergeBenchData(b *testing.B, db *DB, bucket string) {
	entries := 2000
	if err := db.Update(func(tx *Tx) error {
		for j := 0; j < entries; j++ {
			key := []byte(fmt.Sprintf("key-%06d", j))
			value := []byte(fmt.Sprintf("value-%06d", j))
			if err := tx.Put(bucket, key, value, Persistent); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("populate data: %v", err)
	}

	if err := db.Update(func(tx *Tx) error {
		for j := 0; j < entries/2; j++ {
			key := []byte(fmt.Sprintf("key-%06d", j))
			if err := tx.Delete(bucket, key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("delete data: %v", err)
	}
}

func ensureMultipleDataFiles(b *testing.B, db *DB, dir, bucket string) {
	fillerVal := bytes.Repeat([]byte("x"), 512)
	for attempts := 0; attempts < 10; attempts++ {
		userIDs, _, err := enumerateDataFileIDs(dir)
		if err != nil {
			b.Fatalf("enumerate data files: %v", err)
		}
		if len(userIDs) >= 2 {
			return
		}
		key := []byte(fmt.Sprintf("extra-%06d", attempts))
		if err := db.Update(func(tx *Tx) error {
			return tx.Put(bucket, key, fillerVal, Persistent)
		}); err != nil {
			b.Fatalf("add extra entry: %v", err)
		}
	}
	b.Fatalf("expected multiple data files for merge benchmark")
}

func startMergeBenchLoad(db *DB, bucket string, scenario mergeBenchScenario, stopCh <-chan struct{}, wg *sync.WaitGroup) {
	readKey := []byte("key-001500")
	for i := 0; i < scenario.readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				_ = db.View(func(tx *Tx) error {
					_, _ = tx.Get(bucket, readKey)
					return nil
				})
			}
		}()
	}

	var counter uint64
	for i := 0; i < scenario.writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				id := atomic.AddUint64(&counter, 1)
				key := make([]byte, 0, 32)
				key = append(key, "concurrent-"...)
				key = strconv.AppendUint(key, id, 10)
				_ = db.Update(func(tx *Tx) error {
					return tx.Put(bucket, key, []byte("value"), Persistent)
				})
			}
		}()
	}
}
