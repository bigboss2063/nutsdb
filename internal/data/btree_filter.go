package data

import (
	"bytes"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/ttl"
)

// btreeFilter holds the shared filtering semantics between BTreeScanner and BTreeIterator.
// It intentionally covers only TTL + range + prefix behavior.
type btreeFilter struct {
	ttlChecker     *ttl.Checker
	bucketId       uint64
	ds             uint16
	direction      ScanDirection
	startKey       []byte
	endKey         []byte
	prefix         []byte
	includeExpired bool
}

func (f *btreeFilter) isValid(item *core.Item[core.Record]) bool {
	if f.ttlChecker == nil || item.Record == nil {
		return true
	}
	return f.ttlChecker.FilterExpiredRecord(f.bucketId, item.Key, item.Record, f.ds)
}

func (f *btreeFilter) check(item *core.Item[core.Record]) iterResult {
	if item == nil || item.Record == nil {
		return iterContinue
	}

	// TTL check.
	if !f.includeExpired && !f.isValid(item) {
		return iterContinue
	}

	// Range check (inclusive) with direction-aware stop/continue behavior.
	if f.direction == Forward {
		if f.startKey != nil && bytes.Compare(item.Key, f.startKey) < 0 {
			return iterContinue
		}
		if f.endKey != nil && bytes.Compare(item.Key, f.endKey) > 0 {
			return iterStop
		}
	} else {
		// Reverse.
		if f.startKey != nil && bytes.Compare(item.Key, f.startKey) > 0 {
			return iterContinue
		}
		if f.endKey != nil && bytes.Compare(item.Key, f.endKey) < 0 {
			return iterStop
		}
	}

	// Prefix check.
	if f.prefix != nil && !bytes.HasPrefix(item.Key, f.prefix) {
		if f.direction == Forward {
			// Once we've reached keys beyond the prefix range, we can stop.
			// If we're still before the prefix (possible with explicit Seek), continue.
			if bytes.Compare(item.Key, f.prefix) < 0 {
				return iterContinue
			}
			return iterStop
		}

		// Reverse: skip keys above the prefix range; stop when we're below it.
		if bytes.Compare(item.Key, f.prefix) < 0 {
			return iterStop
		}
		return iterContinue
	}

	return iterMatch
}
