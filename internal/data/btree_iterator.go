package data

import (
	"bytes"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/tidwall/btree"
)

type BTreeIteratorOptions struct {
	Reverse        bool
	Prefix         []byte
	Start          []byte
	End            []byte
	IncludeExpired bool
}

// BTreeIterator provides a stateful iterator over a BTree, applying the same TTL/range/prefix
// filtering semantics as BTreeScanner.
//
// It is KV-only and internal to NutsDB; callers outside this module should use the public
// iterator wrapper in package nutsdb.
type BTreeIterator struct {
	iter  btree.IterG[*core.Item[core.Record]]
	item  *core.Item[core.Record]
	valid bool
	opt   BTreeIteratorOptions

	filter btreeFilter
}

func NewBTreeIterator(bt *BTree, opt BTreeIteratorOptions) *BTreeIterator {
	it := &BTreeIterator{
		iter: bt.index.Iter(),
		opt:  opt,
		filter: btreeFilter{
			ttlChecker:     bt.ttlChecker,
			bucketId:       bt.bucketId,
			ds:             core.DataStructureBTree,
			direction:      Forward,
			startKey:       opt.Start,
			endKey:         opt.End,
			prefix:         opt.Prefix,
			includeExpired: opt.IncludeExpired,
		},
	}
	if opt.Reverse {
		it.filter.direction = Reverse
	}
	it.Rewind()
	return it
}

func (it *BTreeIterator) Valid() bool {
	return it.valid
}

func (it *BTreeIterator) Item() *core.Item[core.Record] {
	if !it.valid {
		return nil
	}
	return it.item
}

func (it *BTreeIterator) Next() {
	if !it.valid {
		return
	}
	if it.opt.Reverse {
		it.valid = it.iter.Prev()
	} else {
		it.valid = it.iter.Next()
	}
	it.refreshAndAdvance()
}

func (it *BTreeIterator) Seek(key []byte) {
	if !it.seekWithinBounds(key) {
		it.valid = false
		it.item = nil
		return
	}

	if it.opt.Reverse {
		it.seekReverse(key)
	} else {
		it.seekForward(key)
	}
	it.refreshAndAdvance()
}

func (it *BTreeIterator) Rewind() {
	it.Seek(nil)
}

func (it *BTreeIterator) Close() {
	it.iter.Release()
	it.item = nil
	it.valid = false
}

func (it *BTreeIterator) seekForward(key []byte) {
	seekKey := it.defaultSeekKey(key)
	if seekKey == nil {
		it.valid = it.iter.First()
		return
	}
	it.valid = it.iter.Seek(&core.Item[core.Record]{Key: seekKey})
}

func (it *BTreeIterator) seekReverse(key []byte) {
	seekKey := it.defaultSeekKey(key)
	if seekKey == nil {
		it.valid = it.iter.Last()
		return
	}

	it.valid = it.iter.Seek(&core.Item[core.Record]{Key: seekKey})
	if !it.valid {
		// Seek past the end -> position at the last key (<= seekKey).
		it.valid = it.iter.Last()
		return
	}

	cur := it.iter.Item()
	if cur != nil && bytes.Compare(cur.Key, seekKey) > 0 {
		it.valid = it.iter.Prev()
	}
}

func (it *BTreeIterator) refreshAndAdvance() {
	if !it.valid {
		it.item = nil
		return
	}
	it.item = it.iter.Item()
	it.advanceToMatch()
}

func (it *BTreeIterator) advanceToMatch() {
	for it.valid {
		it.item = it.iter.Item()
		switch it.filter.check(it.item) {
		case iterMatch:
			return
		case iterStop:
			it.valid = false
			it.item = nil
			return
		case iterContinue:
			if it.opt.Reverse {
				it.valid = it.iter.Prev()
			} else {
				it.valid = it.iter.Next()
			}
		}
	}
	it.item = nil
}

func (it *BTreeIterator) seekWithinBounds(key []byte) bool {
	// Default seek is the iterator start; always allowed.
	if key == nil {
		return true
	}

	// Forward: if seeking beyond End, nothing can match.
	if !it.opt.Reverse && it.opt.End != nil && bytes.Compare(key, it.opt.End) > 0 {
		return false
	}
	// Reverse: if seeking below End (lower bound), nothing can match.
	if it.opt.Reverse && it.opt.End != nil && bytes.Compare(key, it.opt.End) < 0 {
		return false
	}
	return true
}

func (it *BTreeIterator) defaultSeekKey(key []byte) []byte {
	if key == nil {
		if it.opt.Reverse {
			if it.opt.Start != nil {
				return it.opt.Start
			}
			if it.opt.Prefix != nil {
				return prefixUpperBound(it.opt.Prefix)
			}
			return nil
		}
		// Forward.
		if it.opt.Start != nil {
			return it.opt.Start
		}
		if it.opt.Prefix != nil {
			return it.opt.Prefix
		}
		return nil
	}

	// Clamp to Start for forward, or to Start as upper bound for reverse.
	if it.opt.Start != nil {
		if !it.opt.Reverse && bytes.Compare(key, it.opt.Start) < 0 {
			return it.opt.Start
		}
		if it.opt.Reverse && bytes.Compare(key, it.opt.Start) > 0 {
			return it.opt.Start
		}
	}

	return key
}

// prefixUpperBound returns the smallest key which is strictly greater than all keys with the
// given prefix. If no such key exists (e.g., prefix is all 0xFF), it returns nil.
func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := make([]byte, len(prefix))
	copy(out, prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
