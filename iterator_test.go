// Copyright 2022 The nutsdb Author. All rights reserved.
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
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/testutils"
	"github.com/nutsdb/nutsdb/internal/ttl"
	"github.com/stretchr/testify/require"
)

func TestIterator_Forward(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{})
			defer it.Close()
			require.NoError(t, it.Err())

			i := 0
			var buf []byte
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NotNil(t, item)
				require.Equal(t, testutils.GetTestBytes(i), item.Key())

				buf, err := item.ValueCopy(buf[:0])
				require.NoError(t, err)
				require.Equal(t, testutils.GetTestBytes(i), buf)

				i++
			}
			require.Equal(t, 100, i)
			return nil
		}))
	})
}

func TestIterator_Reverse(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
			defer it.Close()
			require.NoError(t, it.Err())

			i := 99
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				require.NotNil(t, item)

				val, err := item.ValueCopy(nil)
				require.NoError(t, err)
				require.Equal(t, testutils.GetTestBytes(i), val)
				i--
			}
			require.Equal(t, -1, i)
			return nil
		}))
	})
}

func TestIterator_Seek(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		for i := 0; i < 100; i++ {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{})
			defer it.Close()
			require.NoError(t, it.Err())

			it.Seek(testutils.GetTestBytes(40))
			require.True(t, it.Valid())
			require.Equal(t, testutils.GetTestBytes(40), it.Item().Key())
			return nil
		}))
	})
}

func TestIterator_SeekReverse_NonExistent(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		// Insert keys 0, 2, 4, 6, 8 (even numbers only)
		for i := 0; i < 10; i += 2 {
			txPut(t, db, bucket, testutils.GetTestBytes(i), testutils.GetTestBytes(i), Persistent, nil, nil)
		}

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{Reverse: true})
			defer it.Close()
			require.NoError(t, it.Err())

			// Seek to key 3 (doesn't exist, should position at 2 in reverse)
			it.Seek(testutils.GetTestBytes(3))
			require.True(t, it.Valid())
			require.Equal(t, testutils.GetTestBytes(2), it.Item().Key())
			return nil
		}))
	})
}

func TestIterator_Prefix_Forward(t *testing.T) {
	bucket := "bucket"
	prefix := []byte("user:")

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txPut(t, db, bucket, []byte("foo:1"), []byte("x"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("user:1"), []byte("a"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("user:2"), []byte("b"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("zzz"), []byte("y"), Persistent, nil, nil)

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{Prefix: prefix})
			defer it.Close()
			require.NoError(t, it.Err())

			var keys [][]byte
			it.ForEach(func(item *IteratorItem) bool {
				keys = append(keys, item.KeyCopy(nil))
				return true
			})

			require.Equal(t, [][]byte{[]byte("user:1"), []byte("user:2")}, keys)
			return nil
		}))
	})
}

func TestIterator_Prefix_Reverse(t *testing.T) {
	bucket := "bucket"
	prefix := []byte("user:")

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txPut(t, db, bucket, []byte("foo:1"), []byte("x"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("user:1"), []byte("a"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("user:2"), []byte("b"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("zzz"), []byte("y"), Persistent, nil, nil)

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{Reverse: true, Prefix: prefix})
			defer it.Close()
			require.NoError(t, it.Err())

			var keys [][]byte
			it.ForEach(func(item *IteratorItem) bool {
				keys = append(keys, item.KeyCopy(nil))
				return true
			})

			require.Equal(t, [][]byte{[]byte("user:2"), []byte("user:1")}, keys)
			return nil
		}))
	})
}

func TestIterator_Range(t *testing.T) {
	bucket := "bucket"

	runNutsDBTest(t, nil, func(t *testing.T, db *DB) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txPut(t, db, bucket, []byte("a"), []byte("a"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("b"), []byte("b"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("c"), []byte("c"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("d"), []byte("d"), Persistent, nil, nil)

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{Start: []byte("b"), End: []byte("c")})
			defer it.Close()
			require.NoError(t, it.Err())

			var keys [][]byte
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			require.Equal(t, [][]byte{[]byte("b"), []byte("c")}, keys)
			return nil
		}))
	})
}

func TestIterator_TTL_DefaultExcludesExpired(t *testing.T) {
	bucket := "bucket"

	runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txPut(t, db, bucket, []byte("alive"), []byte("1"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("dead"), []byte("2"), 1, nil, nil) // TTL=1s

		mc.AdvanceTime(2 * time.Second)

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{})
			defer it.Close()
			require.NoError(t, it.Err())

			var keys [][]byte
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			require.Equal(t, [][]byte{[]byte("alive")}, keys)
			return nil
		}))
	})
}

func TestIterator_TTL_IncludeExpired(t *testing.T) {
	bucket := "bucket"

	runNutsDBTestWithMockClock(t, nil, func(t *testing.T, db *DB, mc ttl.Clock) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		txPut(t, db, bucket, []byte("alive"), []byte("1"), Persistent, nil, nil)
		txPut(t, db, bucket, []byte("dead"), []byte("2"), 1, nil, nil) // TTL=1s

		mc.AdvanceTime(2 * time.Second)

		require.NoError(t, db.View(func(tx *Tx) error {
			it := NewIterator(tx, bucket, IteratorOptions{IncludeExpired: true})
			defer it.Close()
			require.NoError(t, it.Err())

			var keys [][]byte
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			require.Equal(t, [][]byte{[]byte("alive"), []byte("dead")}, keys)
			return nil
		}))
	})
}
