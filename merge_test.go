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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_MergeForString(t *testing.T) {
	bucket := "bucket"
	opts := DefaultOptions
	opts.SegmentSize = KB
	opts.Dir = "/tmp/test-string-merge/"

	// Clean the test directory at the start
	removeDir(opts.Dir)

	for _, idxMode := range []EntryIdxMode{HintKeyValAndRAMIdxMode, HintKeyAndRAMIdxMode} {
		opts.EntryIdxMode = idxMode
		db, err := Open(opts)
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)
		require.NoError(t, err)

		// Merge is not needed
		err = db.Merge()
		require.Equal(t, ErrDontNeedMerge, err)

		// Add some data
		n := 1000
		for i := 0; i < n; i++ {
			txPut(t, db, bucket, GetTestBytes(i), GetTestBytes(i), Persistent, nil, nil)
		}

		// Delete some data
		for i := 0; i < n/2; i++ {
			txDel(t, db, bucket, GetTestBytes(i), nil)
		}

		// Merge and check the result
		require.NoError(t, db.Merge())

		dbCnt, err := db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// Check the deleted data is deleted
		for i := 0; i < n/2; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Check the added data is added
		for i := n / 2; i < n; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}

		// Close and reopen the db
		require.NoError(t, db.Close())

		db, err = Open(opts)
		require.NoError(t, err)

		dbCnt, err = db.getRecordCount()
		require.NoError(t, err)
		require.Equal(t, int64(n/2), dbCnt)

		// Check the deleted data is deleted
		for i := 0; i < n/2; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), ErrKeyNotFound)
		}

		// Check the added data is added
		for i := n / 2; i < n; i++ {
			txGet(t, db, bucket, GetTestBytes(i), GetTestBytes(i), nil)
		}

		require.NoError(t, db.Close())
		removeDir(opts.Dir)
	}
}
