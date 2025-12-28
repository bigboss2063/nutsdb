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
	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/data"
)

type IteratorOptions struct {
	Reverse        bool
	Prefix         []byte
	Start          []byte
	End            []byte
	IncludeExpired bool

	// Reserved for future use (2.0+). The initial implementation keeps value
	// fetching lazy; these options may be used to implement read-ahead.
	PrefetchValues bool
	PrefetchSize   int
}

// Iterator provides a KV (BTree) iterator with TTL/range/prefix filtering.
//
// Iterator is not safe for concurrent use.
//
// 2.0 API notes:
// - NewIterator never returns nil; check it.Err() for construction errors.
// - Use `for it.Rewind(); it.Valid(); it.Next() { ... }` or `it.ForEach(...)` to avoid manual breaks.
type Iterator struct {
	tx   *Tx
	opt  IteratorOptions
	iter *data.BTreeIterator

	item IteratorItem
	err  error
}

// IteratorItem represents the current key/value pair during iteration.
// The returned slices are only valid until the iterator is repositioned (Next/Seek/Rewind)
// and while the owning transaction is open.
type IteratorItem struct {
	it     *Iterator
	key    []byte
	record *core.Record
}

func (item *IteratorItem) Key() []byte {
	return item.key
}

func (item *IteratorItem) KeyCopy(dst []byte) []byte {
	if item.key == nil {
		return nil
	}
	if cap(dst) < len(item.key) {
		dst = make([]byte, len(item.key))
	}
	dst = dst[:len(item.key)]
	copy(dst, item.key)
	return dst
}

func (item *IteratorItem) Value(fn func(val []byte) error) error {
	if item.it == nil || item.it.tx == nil {
		return ErrIteratorInvalid
	}
	if item.it.err != nil {
		return item.it.err
	}
	if item.record == nil {
		return ErrIteratorInvalid
	}
	val, err := item.it.tx.db.getValueByRecord(item.record)
	if err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	return fn(val)
}

func (item *IteratorItem) ValueCopy(dst []byte) ([]byte, error) {
	var val []byte
	err := item.Value(func(v []byte) error {
		val = v
		return nil
	})
	if err != nil {
		return nil, err
	}
	if cap(dst) < len(val) {
		dst = make([]byte, len(val))
	}
	dst = dst[:len(val)]
	copy(dst, val)
	return dst, nil
}

// NewIterator returns a new KV iterator for the given bucket.
// It never returns nil. Callers MUST check Err() after construction.
func NewIterator(tx *Tx, bucket string, opt IteratorOptions) *Iterator {
	it := &Iterator{tx: tx, opt: opt}
	it.item.it = it

	if tx == nil {
		it.err = ErrTxClosed
		return it
	}
	if err := tx.checkTxIsClosed(); err != nil {
		it.err = err
		return it
	}

	b, err := tx.db.bucketMgr.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		it.err = ErrBucketNotFound
		return it
	}

	bt := tx.db.Index.BTree.GetWithDefault(b.Id)
	it.iter = data.NewBTreeIterator(bt, data.BTreeIteratorOptions{
		Reverse:        opt.Reverse,
		Prefix:         opt.Prefix,
		Start:          opt.Start,
		End:            opt.End,
		IncludeExpired: opt.IncludeExpired,
	})
	it.refreshItem()
	return it
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Valid() bool {
	return it.err == nil && it.iter != nil && it.iter.Valid()
}

func (it *Iterator) Item() *IteratorItem {
	if !it.Valid() {
		return nil
	}
	return &it.item
}

func (it *Iterator) Rewind() {
	if it.err != nil || it.iter == nil {
		return
	}
	it.iter.Rewind()
	it.refreshItem()
}

func (it *Iterator) Seek(key []byte) {
	if it.err != nil || it.iter == nil {
		return
	}
	it.iter.Seek(key)
	it.refreshItem()
}

func (it *Iterator) Next() {
	if it.err != nil || it.iter == nil {
		return
	}
	it.iter.Next()
	it.refreshItem()
}

// ForEach iterates from the beginning (Rewind) and stops when fn returns false.
func (it *Iterator) ForEach(fn func(item *IteratorItem) bool) {
	if it.err != nil || it.iter == nil || fn == nil {
		return
	}
	for it.Rewind(); it.Valid(); it.Next() {
		if !fn(&it.item) {
			return
		}
	}
}

func (it *Iterator) Close() {
	if it.iter != nil {
		it.iter.Close()
	}
	it.iter = nil
	it.item.key = nil
	it.item.record = nil
	if it.err == nil {
		it.err = ErrIteratorInvalid
	}
}

func (it *Iterator) refreshItem() {
	if !it.Valid() {
		it.item.key = nil
		it.item.record = nil
		return
	}
	cur := it.iter.Item()
	if cur == nil || cur.Record == nil {
		it.item.key = nil
		it.item.record = nil
		return
	}
	it.item.key = cur.Key
	it.item.record = cur.Record
}
