package nutsdb

import (
	"bytes"
	"regexp"
	"sort"

	"github.com/nutsdb/nutsdb/internal/utils"
)

// EntryStatus represents the Entry status in the current Tx
type EntryStatus = uint8

const (
	// NotFoundEntry means there is no changes for this entry in current Tx
	NotFoundEntry EntryStatus = 0
	// EntryDeleted means this Entry has been deleted in the current Tx
	EntryDeleted EntryStatus = 1
	// EntryUpdated means this Entry has been updated in the current Tx
	EntryUpdated EntryStatus = 2
)

// BucketStatus represents the current status of bucket in current Tx
type BucketStatus = uint8

const (
	// BucketStatusExistAlready means this bucket already exists
	BucketStatusExistAlready = 1
	// BucketStatusDeleted means this bucket is already deleted
	BucketStatusDeleted = 2
	// BucketStatusNew means this bucket is created in current Tx
	BucketStatusNew = 3
	// BucketStatusUpdated means this bucket is updated in current Tx
	BucketStatusUpdated = 4
	// BucketStatusUnknown means this bucket doesn't exist
	BucketStatusUnknown = 5
)

// pendingBucketList the uncommitted bucket changes in this Tx
type pendingBucketList map[Ds]map[BucketName]*Bucket

// pendingEntriesInBTree means the changes Entries in DataStructureBTree in the Tx
type pendingEntriesInBTree map[BucketName]map[string]*Entry

// pendingEntryList the uncommitted Entry changes in this Tx
type pendingEntryList struct {
	entriesInBTree pendingEntriesInBTree
	entries        map[Ds]map[BucketName][]*Entry
	size           int
}

// newPendingEntriesList create a new pendingEntryList object for a Tx
func newPendingEntriesList() *pendingEntryList {
	pending := &pendingEntryList{
		entriesInBTree: map[BucketName]map[string]*Entry{},
		entries:        map[Ds]map[BucketName][]*Entry{},
		size:           0,
	}
	return pending
}

// submitEntry submit an entry into pendingEntryList
func (pending *pendingEntryList) submitEntry(ds Ds, bucket string, e *Entry) {
	switch ds {
	case DataStructureBTree:
		if _, exist := pending.entriesInBTree[bucket]; !exist {
			pending.entriesInBTree[bucket] = map[string]*Entry{}
		}
		if _, exist := pending.entriesInBTree[bucket][string(e.Key)]; !exist {
			pending.size++
		}
		pending.entriesInBTree[bucket][string(e.Key)] = e
	default:
		if _, exist := pending.entries[ds]; !exist {
			pending.entries[ds] = map[BucketName][]*Entry{}
		}
		entries := pending.entries[ds][bucket]
		entries = append(entries, e)
		pending.entries[ds][bucket] = entries
		pending.size++
	}
}

func (pending *pendingEntryList) Get(ds Ds, bucket string, key []byte) (entry *Entry, err error) {
	switch ds {
	case DataStructureBTree:
		if _, exist := pending.entriesInBTree[bucket]; exist {
			if rec, ok := pending.entriesInBTree[bucket][string(key)]; ok {
				return rec, nil
			} else {
				return nil, ErrKeyNotFound
			}
		}
		return nil, ErrBucketNotFound
	default:
		if _, exist := pending.entries[ds]; exist {
			if entries, ok := pending.entries[ds][bucket]; ok {
				for _, e := range entries {
					if bytes.Equal(key, e.Key) {
						return e, nil
					}
				}
				return nil, ErrKeyNotFound
			} else {
				return nil, ErrKeyNotFound
			}
		}
		return nil, ErrBucketNotFound
	}
}

func (pending *pendingEntryList) GetTTL(ds Ds, bucket string, key []byte) (ttl int64, err error) {
	rec, err := pending.Get(ds, bucket, key)
	if err != nil {
		return 0, err
	}
	if rec.Meta.TTL == Persistent {
		return -1, nil
	}
	return int64(expireTime(rec.Meta.Timestamp, rec.Meta.TTL).Seconds()), nil
}

// getDataByRange returns keys and values in the range [start, end] from pending writes.
// It excludes deleted entries (entries with DataDeleteFlag).
func (pending *pendingEntryList) getDataByRange(
	start, end []byte, bucketName BucketName,
) (keys, values [][]byte) {

	mp, ok := pending.entriesInBTree[bucketName]
	if !ok {
		return nil, nil
	}

	// Pre-allocate with estimated capacity
	estimatedSize := len(mp)
	keys = make([][]byte, 0, estimatedSize)
	values = make([][]byte, 0, estimatedSize)

	for _, v := range mp {
		// Skip deleted entries
		if v.Meta != nil && v.Meta.Flag == DataDeleteFlag {
			continue
		}
		if bytes.Compare(start, v.Key) <= 0 && bytes.Compare(v.Key, end) <= 0 {
			keys = append(keys, v.Key)
			values = append(values, v.Value)
		}
	}
	sort.Sort(&sortkv{
		k: keys,
		v: values,
	})
	return
}

// getAllOrKeysOrValues returns keys and/or values based on type
// typ: getAllType(0), getKeysType(1), getValuesType(2)
// It excludes deleted entries (entries with DataDeleteFlag)
func (pending *pendingEntryList) getAllOrKeysOrValues(bucket string, includeKeys, includeValues bool) (keys, values [][]byte) {
	mp, ok := pending.entriesInBTree[bucket]
	if !ok {
		return nil, nil
	}

	// Collect non-deleted entries
	var activeEntries []*Entry
	for _, entry := range mp {
		// Skip deleted entries (Meta.Flag == DataDeleteFlag)
		if entry.Meta != nil && entry.Meta.Flag == DataDeleteFlag {
			continue
		}
		activeEntries = append(activeEntries, entry)
	}

	if len(activeEntries) == 0 {
		return nil, nil
	}

	// Sort entries by key
	sort.Slice(activeEntries, func(i, j int) bool {
		return bytes.Compare(activeEntries[i].Key, activeEntries[j].Key) < 0
	})

	// Extract keys and values
	if includeKeys {
		keys = make([][]byte, 0, len(activeEntries))
	}
	if includeValues {
		values = make([][]byte, 0, len(activeEntries))
	}

	for _, entry := range activeEntries {
		if includeKeys {
			keys = append(keys, entry.Key)
		}
		if includeValues {
			values = append(values, entry.Value)
		}
	}

	return
}

// prefixScanEntries returns keys and/or values with given prefix from pending writes
// It returns active (non-deleted) entries and deleted keys separately.
// If reg is provided, only entries matching the regex pattern are returned.
func (pending *pendingEntryList) prefixScanEntries(bucket string, prefix []byte, reg string, includeKeys, includeValues bool) (keys, values [][]byte, err error) {
	mp, ok := pending.entriesInBTree[bucket]
	if !ok {
		return nil, nil, nil
	}

	// Collect entries matching the prefix (excluding deleted entries)
	var matchedEntries []*Entry
	for _, entry := range mp {
		if bytes.HasPrefix(entry.Key, prefix) {
			// Skip deleted entries (Meta.Flag == DataDeleteFlag)
			if entry.Meta != nil && entry.Meta.Flag == DataDeleteFlag {
				continue
			}
			matchedEntries = append(matchedEntries, entry)
		}
	}

	if len(matchedEntries) == 0 {
		return nil, nil, nil
	}

	// Apply regex filter if provided
	if reg != "" {
		regex, err := regexp.Compile(reg)
		if err != nil {
			return nil, nil, err
		}
		var filtered []*Entry
		for _, entry := range matchedEntries {
			// Match against the key suffix after removing the prefix (same as DB behavior)
			keySuffix := bytes.TrimPrefix(entry.Key, prefix)
			if regex.Match(keySuffix) {
				filtered = append(filtered, entry)
			}
		}
		matchedEntries = filtered
	}

	// Sort entries by key
	sort.Slice(matchedEntries, func(i, j int) bool {
		return bytes.Compare(matchedEntries[i].Key, matchedEntries[j].Key) < 0
	})

	// Extract keys and values
	if includeKeys {
		keys = make([][]byte, 0, len(matchedEntries))
	}
	if includeValues {
		values = make([][]byte, 0, len(matchedEntries))
	}

	for _, entry := range matchedEntries {
		if includeKeys {
			keys = append(keys, entry.Key)
		}
		if includeValues {
			values = append(values, entry.Value)
		}
	}

	return
}

// getDeletedKeys returns a set of deleted keys with given prefix from pending writes.
//
// Parameters:
//   - bucket: the bucket name to search in
//   - prefix: optional prefix filter; if nil or empty, returns all deleted keys in the bucket
//
// Returns a map where keys are string representations of deleted key bytes.
func (pending *pendingEntryList) getDeletedKeys(bucket string, prefix []byte) map[string]bool {
	mp, ok := pending.entriesInBTree[bucket]
	if !ok {
		return nil
	}

	deletedKeys := make(map[string]bool)
	for _, entry := range mp {
		if entry.Meta != nil && entry.Meta.Flag == DataDeleteFlag {
			// If prefix is provided, filter by prefix; otherwise include all deleted keys
			if len(prefix) == 0 || bytes.HasPrefix(entry.Key, prefix) {
				deletedKeys[string(entry.Key)] = true
			}
		}
	}

	return deletedKeys
}

// rangeBucket input a range handler function f and call it with every bucket in pendingBucketList
func (p pendingBucketList) rangeBucket(f func(bucket *Bucket) error) error {
	for _, bucketsInDs := range p {
		for _, bucket := range bucketsInDs {
			err := f(bucket)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// toList collect all the entries in pendingEntryList to a list.
func (pending *pendingEntryList) toList() []*Entry {
	list := make([]*Entry, 0, pending.size)
	for _, entriesInBucket := range pending.entriesInBTree {
		for _, entry := range entriesInBucket {
			list = append(list, entry)
		}
	}
	for _, entriesInDS := range pending.entries {
		for _, entries := range entriesInDS {
			list = append(list, entries...)
		}
	}
	return list
}

func (pending *pendingEntryList) rangeEntries(_ Ds, bucketName BucketName, rangeFunc func(entry *Entry) bool) {
	pendingWriteEntries := pending.entriesInBTree
	if pendingWriteEntries == nil {
		return
	}
	entries := pendingWriteEntries[bucketName]
	for _, entry := range entries {
		ok := rangeFunc(entry)
		if !ok {
			break
		}
	}
}

func (pending *pendingEntryList) MaxOrMinKey(bucketName string, isMax bool) (key []byte, found bool) {
	var (
		maxKey       []byte = nil
		minKey       []byte = nil
		pendingFound        = false
	)

	pending.rangeEntries(
		DataStructureBTree,
		bucketName,
		func(entry *Entry) bool {
			// Skip deleted entries in pending writes
			if entry.Meta != nil && entry.Meta.Flag == DataDeleteFlag {
				return true
			}
			// Also skip expired entries in pending writes
			if entry.Meta != nil && utils.IsExpired(entry.Meta.TTL, entry.Meta.Timestamp) {
				return true
			}
			maxKey = compareAndReturn(maxKey, entry.Key, 1)
			minKey = compareAndReturn(minKey, entry.Key, -1)
			pendingFound = true
			return true

		})

	if !pendingFound {
		return nil, false
	}

	if isMax {
		return maxKey, true
	}

	return minKey, true
}

// isBucketNotFoundStatus return true for bucket is not found,
// false for other status.
func isBucketNotFoundStatus(status BucketStatus) bool {
	return status == BucketStatusDeleted || status == BucketStatusUnknown
}
