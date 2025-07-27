// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	db  *DB
	opt Options
	err error
)

const NutsDBTestDirPath = "/tmp/nutsdb-test"

func assertErr(t *testing.T, err error, expectErr error) {
	if expectErr != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func runNutsDBTest(t *testing.T, opts *Options, test func(t *testing.T, db *DB)) {
	if opts == nil {
		opts = &DefaultOptions
	}
	if opts.Dir == "" {
		opts.Dir = NutsDBTestDirPath
	}
	defer removeDir(opts.Dir)
	db, err := Open(*opts)
	require.NoError(t, err)

	test(t, db)
	t.Cleanup(func() {
		if !db.IsClose() {
			require.NoError(t, db.Close())
		}
	})
}

func txPut(t *testing.T, db *DB, bucket string, key, value []byte, ttl uint32, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err = tx.Put(bucket, key, value, ttl)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txGet(t *testing.T, db *DB, bucket string, key []byte, expectVal []byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.Get(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			require.EqualValuesf(t, expectVal, value, "err Tx Get. got %s want %s", string(value), string(expectVal))
		}
		return nil
	})
	require.NoError(t, err)
}

func txGetAll(t *testing.T, db *DB, bucket string, expectKeys [][]byte, expectValues [][]byte, expectErr error) {
	require.NoError(t, db.View(func(tx *Tx) error {
		keys, values, err := tx.GetAll(bucket)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			n := len(keys)
			for i := 0; i < n; i++ {
				require.Equal(t, expectKeys[i], keys[i])
				require.Equal(t, expectValues[i], values[i])
			}
		}
		return nil
	}))
}

func txDel(t *testing.T, db *DB, bucket string, key []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Delete(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txGetMaxOrMinKey(t *testing.T, db *DB, bucket string, isMax bool, expectVal []byte, expectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.getMaxOrMinKey(bucket, isMax)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
			require.EqualValuesf(t, expectVal, value, "err Tx Get. got %s want %s", string(value), string(expectVal))
		}
		return nil
	})
	require.NoError(t, err)
}

func txDeleteBucket(t *testing.T, db *DB, ds uint16, bucket string, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.DeleteBucket(ds, bucket)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func txCreateBucket(t *testing.T, db *DB, ds uint16, bucket string, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.NewBucket(ds, bucket)
		assertErr(t, err, expectErr)
		return nil
	})
	require.NoError(t, err)
}

func InitOpt(fileDir string, isRemoveFiles bool) {
	if fileDir == "" {
		fileDir = "/tmp/nutsdbtest"
	}
	if isRemoveFiles {
		files, _ := ioutil.ReadDir(fileDir)
		for _, f := range files {
			name := f.Name()
			if name != "" {
				err := os.RemoveAll(fileDir + "/" + name)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	opt = DefaultOptions
	opt.Dir = fileDir
	opt.SegmentSize = 8 * 1024
	opt.CleanFdsCacheThreshold = 0.5
	opt.MaxFdNumsInCache = 1024
}

func TestDB_Basic(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key0 := GetTestBytes(0)
		val0 := GetRandomBytes(24)

		// put
		txPut(t, db, bucket, key0, val0, Persistent, nil, nil)
		txGet(t, db, bucket, key0, val0, nil)

		val1 := GetRandomBytes(24)

		// update
		txPut(t, db, bucket, key0, val1, Persistent, nil, nil)
		txGet(t, db, bucket, key0, val1, nil)

		// del
		txDel(t, db, bucket, key0, nil)
		txGet(t, db, bucket, key0, val1, ErrKeyNotFound)
	})
}

func TestDB_Flock(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		db2, err := Open(db.opt)
		require.Nil(t, db2)
		require.Equal(t, ErrDirLocked, err)

		err = db.Close()
		require.NoError(t, err)

		db2, err = Open(db.opt)
		require.NoError(t, err)
		require.NotNil(t, db2)

		err = db2.flock.Unlock()
		require.NoError(t, err)
		require.False(t, db2.flock.Locked())

		err = db2.Close()
		require.Error(t, err)
		require.Equal(t, ErrDirUnlocked, err)
	})
}

func TestDB_DeleteANonExistKey(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		testBucket := "test_bucket"
		txCreateBucket(t, db, DataStructureBTree, testBucket, nil)

		txDel(t, db, testBucket, GetTestBytes(0), ErrKeyNotFound)
		txPut(t, db, testBucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil, nil)
		txDel(t, db, testBucket, GetTestBytes(0), ErrKeyNotFound)
	})
}

func txIterateBuckets(t *testing.T, db *DB, ds uint16, pattern string, f func(key string) bool, expectErr error, containsKey ...string) {
	err := db.View(func(tx *Tx) error {
		var elements []string
		err := tx.IterateBuckets(ds, pattern, func(key string) bool {
			if f != nil && !f(key) {
				return false
			}
			elements = append(elements, key)
			return true
		})
		if err != nil {
			assert.Equal(t, expectErr, err)
		} else {
			assert.NoError(t, err)
			for _, key := range containsKey {
				assert.Contains(t, elements, key)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDB_GetKeyNotFound(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
		txPut(t, db, bucket, GetTestBytes(1), GetRandomBytes(24), Persistent, nil, nil)
		txGet(t, db, bucket, GetTestBytes(0), nil, ErrKeyNotFound)
	})
}

func TestDB_Backup(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		backUpDir := "/tmp/nutsdb-backup"
		require.NoError(t, db.Backup(backUpDir))
	})
}

func TestDB_BackupTarGZ(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		backUpFile := "/tmp/nutsdb-backup/backup.tar.gz"
		f, err := os.Create(backUpFile)
		require.NoError(t, err)
		require.NoError(t, db.BackupTarGZ(f))
	})
}

func TestDB_Close(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Close())
		require.Equal(t, ErrDBClosed, db.Close())
	})
}

func TestDB_ErrThenReadWrite(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "testForDeadLock"
		err = db.View(
			func(tx *Tx) error {
				return fmt.Errorf("err happened")
			})
		require.NotNil(t, err)

		err = db.View(
			func(tx *Tx) error {
				key := []byte("key1")
				_, err := tx.Get(bucket, key)
				if err != nil {
					return err
				}

				return nil
			})
		require.NotNil(t, err)

		notice := make(chan struct{})
		go func() {
			err = db.Update(
				func(tx *Tx) error {
					notice <- struct{}{}

					return nil
				})
			require.NoError(t, err)
		}()

		select {
		case <-notice:
		case <-time.After(1 * time.Second):
			t.Fatalf("exist deadlock")
		}
	})
}

func TestDB_ErrorHandler(t *testing.T) {
	opts := DefaultOptions
	handleErrCalled := false
	opts.ErrorHandler = ErrorHandlerFunc(func(err error) {
		handleErrCalled = true
	})

	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		err = db.View(
			func(tx *Tx) error {
				return fmt.Errorf("err happened")
			})
		require.NotNil(t, err)
		require.Equal(t, handleErrCalled, true)
	})
}

func TestDB_CommitBuffer(t *testing.T) {
	bucket := "bucket"

	opts := DefaultOptions
	opts.CommitBufferSize = 8 * MB
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		require.Equal(t, int64(8*MB), db.opt.CommitBufferSize)
		// When the database starts, the commit buffer should be allocated with the size of CommitBufferSize.
		require.Equal(t, 0, db.commitBuffer.Len())
		require.Equal(t, db.opt.CommitBufferSize, int64(db.commitBuffer.Cap()))
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, GetTestBytes(0), GetRandomBytes(24), Persistent, nil, nil)

		// When tx is committed, content of commit buffer should be empty, but do not release memory
		require.Equal(t, 0, db.commitBuffer.Len())
		require.Equal(t, db.opt.CommitBufferSize, int64(db.commitBuffer.Cap()))
	})

	opts = DefaultOptions
	opts.CommitBufferSize = 1 * KB
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		require.Equal(t, int64(1*KB), db.opt.CommitBufferSize)

		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		err := db.Update(func(tx *Tx) error {
			// making this tx big enough, it should not use the commit buffer
			for i := 0; i < 1000; i++ {
				err := tx.Put(bucket, GetTestBytes(i), GetRandomBytes(1024), Persistent)
				require.NoError(t, err)
			}
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, 0, db.commitBuffer.Len())
		require.Equal(t, db.opt.CommitBufferSize, int64(db.commitBuffer.Cap()))
	})
}

func TestDB_DeleteBucket(t *testing.T) {
	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key := GetTestBytes(0)
		val := GetTestBytes(0)
		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)

		txDeleteBucket(t, db, DataStructureBTree, bucket, nil)
		txPut(t, db, bucket, key, val, Persistent, ErrorBucketNotExist, nil)
	})
}

func withDBOption(t *testing.T, opt Options, fn func(t *testing.T, db *DB)) {
	db, err := Open(opt)
	require.NoError(t, err)

	defer func() {
		os.RemoveAll(db.opt.Dir)
		db.Close()
	}()

	fn(t, db)
}

func withDefaultDB(t *testing.T, fn func(t *testing.T, db *DB)) {
	tmpdir, _ := os.MkdirTemp("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.SegmentSize = 8 * 1024

	withDBOption(t, opt, fn)
}

func withRAMIdxDB(t *testing.T, fn func(t *testing.T, db *DB)) {
	tmpdir, _ := os.MkdirTemp("", "nutsdb")
	opt := DefaultOptions
	opt.Dir = tmpdir
	opt.EntryIdxMode = HintKeyAndRAMIdxMode

	withDBOption(t, opt, fn)
}

func TestDB_HintKeyValAndRAMIdxMode_RestartDB(t *testing.T) {
	opts := DefaultOptions
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		key := GetTestBytes(0)
		val := GetTestBytes(0)

		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)

		db.Close()
		// restart db with HintKeyValAndRAMIdxMode EntryIdxMode
		db, err := Open(db.opt)
		require.NoError(t, err)
		txGet(t, db, bucket, key, val, nil)
	})
}

func TestDB_HintKeyAndRAMIdxMode_RestartDB(t *testing.T) {
	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		key := GetTestBytes(0)
		val := GetTestBytes(0)

		txPut(t, db, bucket, key, val, Persistent, nil, nil)
		txGet(t, db, bucket, key, val, nil)
		db.Close()

		// restart db with HintKeyAndRAMIdxMode EntryIdxMode
		db, err := Open(db.opt)
		require.NoError(t, err)
		txGet(t, db, bucket, key, val, nil)
	})
}

func TestDB_HintKeyAndRAMIdxMode_LruCache(t *testing.T) {
	opts := DefaultOptions
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	lruCacheSizes := []int{0, 5000, 10000, 20000}

	for _, lruCacheSize := range lruCacheSizes {
		opts.HintKeyAndRAMIdxCacheSize = lruCacheSize
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			bucket := "bucket"
			txCreateBucket(t, db, DataStructureBTree, bucket, nil)
			for i := 0; i < 10000; i++ {
				key := []byte(fmt.Sprintf("%10d", i))
				val := []byte(fmt.Sprintf("%10d", i))
				txPut(t, db, bucket, key, val, Persistent, nil, nil)
				txGet(t, db, bucket, key, val, nil)
				txGet(t, db, bucket, key, val, nil)
			}
			db.Close()
		})
	}
}

func TestTx_SmallFile(t *testing.T) {
	opts := DefaultOptions
	opts.SegmentSize = 100
	opts.EntryIdxMode = HintKeyAndRAMIdxMode
	runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
		bucket := "bucket"
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		err := db.Update(func(tx *Tx) error {
			for i := 0; i < 100; i++ {
				err := tx.Put(bucket, GetTestBytes(i), GetTestBytes(i), Persistent)
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.Nil(t, err)
		require.NoError(t, db.Close())
		db, _ = Open(opts)

		txGet(t, db, bucket, GetTestBytes(10), GetTestBytes(10), nil)
	})
}

func TestDB_DataStructureBTreeWriteRecordLimit(t *testing.T) {
	opts := DefaultOptions
	limitCount := int64(1000)
	opts.MaxWriteRecordCount = limitCount
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	// Iterate over different EntryIdxModes
	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		runNutsDBTest(t, &opts, func(t *testing.T, db *DB) {
			txCreateBucket(t, db, DataStructureBTree, bucket1, nil)
			txCreateBucket(t, db, DataStructureBTree, bucket2, nil)

			// Add limitCount records
			err := db.Update(func(tx *Tx) error {
				for i := 0; i < int(limitCount); i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.Put(bucket1, key, value, Persistent)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Trigger the limit
			txPut(t, db, bucket1, []byte("key1"), []byte("value1"), Persistent, nil, ErrTxnExceedWriteLimit)
			// Add a key that is within the limit
			txPut(t, db, bucket1, []byte("0"), []byte("000"), Persistent, nil, nil)
			// Delete and add one item
			txDel(t, db, bucket1, []byte("0"), nil)
			txPut(t, db, bucket1, []byte("key1"), []byte("value1"), Persistent, nil, nil)
			// Add an item to another bucket
			txPut(t, db, bucket2, []byte("key2"), []byte("value2"), Persistent, nil, ErrTxnExceedWriteLimit)
			// Delete bucket1
			txDeleteBucket(t, db, DataStructureBTree, bucket1, nil)
			// Add data to bucket2
			err = db.Update(func(tx *Tx) error {
				for i := 0; i < (int(limitCount) - 1); i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(strconv.Itoa(i))
					err = tx.Put(bucket2, key, value, Persistent)
					assertErr(t, err, nil)
				}
				return nil
			})
			require.NoError(t, err)
			// Add items to bucket2
			txPut(t, db, bucket2, []byte("key1"), []byte("value1"), Persistent, nil, nil)
			txPut(t, db, bucket2, []byte("key2"), []byte("value2"), Persistent, nil, ErrTxnExceedWriteLimit)
		})
	}
}

func txIncrement(t *testing.T, db *DB, bucket string, key []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Incr(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txDecrement(t *testing.T, db *DB, bucket string, key []byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Decr(bucket, key)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txIncrementBy(t *testing.T, db *DB, bucket string, key []byte, value int64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.IncrBy(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txDecrementBy(t *testing.T, db *DB, bucket string, key []byte, value int64, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.DecrBy(bucket, key, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txPutIfNotExists(t *testing.T, db *DB, bucket string, key, value []byte, expectedErr, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.PutIfNotExists(bucket, key, value, Persistent)
		assertErr(t, err, expectedErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txPutIfExists(t *testing.T, db *DB, bucket string, key, value []byte, expectedErr, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.PutIfExists(bucket, key, value, Persistent)
		assertErr(t, err, expectedErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txValueLen(t *testing.T, db *DB, bucket string, key []byte, expectLength int, expectErr error) {
	err := db.View(func(tx *Tx) error {
		length, err := tx.ValueLen(bucket, key)
		if expectErr != nil {
			require.Equal(t, expectErr, err)
		} else {
			require.NoError(t, err)
		}
		require.EqualValuesf(t, expectLength, length, "err Tx ValueLen. got %s want %s", length, expectLength)
		return nil
	})
	require.NoError(t, err)
}

func txGetSet(t *testing.T, db *DB, bucket string, key, value []byte, expectOldValue []byte, expectErr error) {
	err := db.Update(func(tx *Tx) error {
		oldValue, err := tx.GetSet(bucket, key, value)
		assertErr(t, err, expectErr)
		require.EqualValuesf(t, oldValue, expectOldValue, "err Tx GetSet. got %s want %s", string(oldValue), string(expectOldValue))
		return nil
	})
	require.NoError(t, err)
}

func txGetBit(t *testing.T, db *DB, bucket string, key []byte, offset int, expectVal byte, expectErr error, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.GetBit(bucket, key, offset)
		assertErr(t, err, expectErr)
		require.Equal(t, expectVal, value)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txSetBit(t *testing.T, db *DB, bucket string, key []byte, offset int, value byte, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.SetBit(bucket, key, offset, value)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txGetTTL(t *testing.T, db *DB, bucket string, key []byte, expectedTTL int64, expectedErr error) {
	err := db.View(func(tx *Tx) error {
		ttl, err := tx.GetTTL(bucket, key)
		assertErr(t, err, expectedErr)

		// If diff between expectedTTL and realTTL lesser than 1s, We'll consider as equal
		diff := int(math.Abs(float64(ttl - expectedTTL)))
		assert.LessOrEqual(t, diff, 1)
		return nil
	})
	require.NoError(t, err)
}

func txPersist(t *testing.T, db *DB, bucket string, key []byte, expectedErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Persist(bucket, key)
		assertErr(t, err, expectedErr)
		return nil
	})
	require.NoError(t, err)
}

func txMSet(t *testing.T, db *DB, bucket string, args [][]byte, ttl uint32, expectErr error, finalExpectErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.MSet(bucket, ttl, args...)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txMGet(t *testing.T, db *DB, bucket string, keys [][]byte, expectValues [][]byte, expectErr error, finalExpectErr error) {
	err := db.View(func(tx *Tx) error {
		values, err := tx.MGet(bucket, keys...)
		assertErr(t, err, expectErr)
		require.EqualValues(t, expectValues, values)
		return nil
	})
	assertErr(t, err, finalExpectErr)
}

func txAppend(t *testing.T, db *DB, bucket string, key, appendage []byte, expectErr error, expectFinalErr error) {
	err := db.Update(func(tx *Tx) error {
		err := tx.Append(bucket, key, appendage)
		assertErr(t, err, expectErr)
		return nil
	})
	assertErr(t, err, expectFinalErr)
}

func txGetRange(t *testing.T, db *DB, bucket string, key []byte, start, end int, expectVal []byte, expectErr error, expectFinalErr error) {
	err := db.View(func(tx *Tx) error {
		value, err := tx.GetRange(bucket, key, start, end)
		assertErr(t, err, expectErr)
		require.EqualValues(t, expectVal, value)
		return nil
	})
	assertErr(t, err, expectFinalErr)
}
