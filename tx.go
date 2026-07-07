package nutsdb

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/store"
)

// Tx is the public transaction facade. Implementations are created by DB.View
// and DB.Update and are only valid inside the callback.
type Tx interface {
	Get(ctx context.Context, bucket string, key []byte) ([]byte, error)
	Put(ctx context.Context, bucket string, key, value []byte) error
	Delete(ctx context.Context, bucket string, key []byte) error
	Scan(ctx context.Context, bucket string, r Range, fn func(context.Context, Item) error) error
	Bucket(name string) Bucket
}

type tx struct {
	db       *DB
	writable bool
	closed   bool
	pending  map[string]mutation
	order    []string
}

type mutation struct {
	record  *core.Record
	deleted bool
}

func newTx(db *DB, writable bool) *tx {
	return &tx{
		db:       db,
		writable: writable,
		pending:  make(map[string]mutation),
	}
}

func (tx *tx) Bucket(name string) Bucket {
	return &bucket{tx: tx, name: name}
}

func (tx *tx) Get(ctx context.Context, bucket string, key []byte) ([]byte, error) {
	if err := tx.ensureOpen(ctx); err != nil {
		return nil, err
	}
	physicalKey, err := tx.db.codec.encodeUserKey(bucket, key)
	if err != nil {
		return nil, err
	}
	if m, ok := tx.pending[string(physicalKey)]; ok {
		if m.deleted {
			return nil, ErrNotFound
		}
		return tx.db.codec.decodeValue(m.record.Value)
	}
	rec, err := tx.db.store.Get(ctx, physicalKey)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return tx.db.codec.decodeValue(rec.Value)
}

func (tx *tx) Put(ctx context.Context, bucket string, key, value []byte) error {
	if err := tx.ensureWritable(ctx); err != nil {
		return err
	}
	physicalKey, err := tx.db.codec.encodeUserKey(bucket, key)
	if err != nil {
		return err
	}
	physicalValue := tx.db.codec.encodeValue(value)
	rec := core.NewRecord().WithValue(physicalValue).WithValueSize(uint32(len(physicalValue)))
	tx.putMutation(physicalKey, rec, false)
	return nil
}

func (tx *tx) Delete(ctx context.Context, bucket string, key []byte) error {
	if err := tx.ensureWritable(ctx); err != nil {
		return err
	}
	physicalKey, err := tx.db.codec.encodeUserKey(bucket, key)
	if err != nil {
		return err
	}
	tx.putMutation(physicalKey, nil, true)
	return nil
}

func (tx *tx) Scan(ctx context.Context, bucket string, r Range, fn func(context.Context, Item) error) error {
	if err := tx.ensureOpen(ctx); err != nil {
		return err
	}
	prefix, err := tx.db.codec.bucketPrefix(bucket)
	if err != nil {
		return err
	}
	itemsByPhysicalKey := make(map[string]Item)
	var scanErr error
	err = tx.db.store.Iterate(ctx, func(key []byte, rec *core.Record) bool {
		match, userKey, err := physicalKeyInRange(key, prefix, r, tx.db.codec)
		if err != nil {
			scanErr = err
			return false
		}
		if !match {
			return true
		}
		value, err := tx.db.codec.decodeValue(rec.Value)
		if err != nil {
			scanErr = err
			return false
		}
		itemsByPhysicalKey[string(key)] = Item{Key: userKey, Value: value}
		return true
	})
	if err != nil {
		return err
	}
	if scanErr != nil {
		return scanErr
	}
	for _, key := range tx.order {
		m := tx.pending[key]
		match, userKey, err := physicalKeyInRange([]byte(key), prefix, r, tx.db.codec)
		if err != nil {
			return err
		}
		if !match {
			continue
		}
		if m.deleted {
			delete(itemsByPhysicalKey, key)
			continue
		}
		value, err := tx.db.codec.decodeValue(m.record.Value)
		if err != nil {
			return err
		}
		itemsByPhysicalKey[key] = Item{Key: userKey, Value: value}
	}
	items := make([]Item, 0, len(itemsByPhysicalKey))
	for _, item := range itemsByPhysicalKey {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		cmp := bytes.Compare(items[i].Key, items[j].Key)
		if r.Reverse {
			return cmp > 0
		}
		return cmp < 0
	})
	if r.Limit > 0 && len(items) > r.Limit {
		items = items[:r.Limit]
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(ctx, Item{Key: cloneBytes(item.Key), Value: cloneBytes(item.Value)}); err != nil {
			return err
		}
	}
	return nil
}

func (tx *tx) putMutation(physicalKey []byte, record *core.Record, deleted bool) {
	key := string(physicalKey)
	if _, ok := tx.pending[key]; !ok {
		tx.order = append(tx.order, key)
	}
	record = cloneCoreRecord(record)
	tx.pending[key] = mutation{record: record, deleted: deleted}
}

func (tx *tx) commit(ctx context.Context) error {
	if err := tx.ensureWritable(ctx); err != nil {
		return err
	}
	if len(tx.order) == 0 {
		return nil
	}
	puts := make([]struct {
		Key   []byte
		Value *core.Record
	}, 0, len(tx.order))
	deletes := make([][]byte, 0)
	for _, key := range tx.order {
		m := tx.pending[key]
		if m.deleted {
			deletes = append(deletes, cloneBytes([]byte(key)))
			continue
		}
		puts = append(puts, struct {
			Key   []byte
			Value *core.Record
		}{Key: cloneBytes([]byte(key)), Value: cloneCoreRecord(m.record)})
	}
	if len(puts) > 0 {
		if err := tx.db.store.BatchPut(ctx, puts); err != nil {
			return err
		}
	}
	if len(deletes) > 0 {
		if err := tx.db.store.BatchDelete(ctx, deletes); err != nil {
			return err
		}
	}
	return nil
}

func (tx *tx) close() {
	tx.closed = true
}

func (tx *tx) ensureOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	if tx.db == nil || tx.db.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (tx *tx) ensureWritable(ctx context.Context) error {
	if err := tx.ensureOpen(ctx); err != nil {
		return err
	}
	if !tx.writable {
		return ErrReadOnly
	}
	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func cloneCoreRecord(r *core.Record) *core.Record {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Value = cloneBytes(r.Value)
	return &cp
}
