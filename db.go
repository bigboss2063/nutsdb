package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nutsdb/nutsdb/internal/store"
)

// DB is the public control-plane entry point. User data operations are exposed
// through View/Update transactions, not directly on DB.
type DB struct {
	store  store.StoreManager
	mu     sync.RWMutex
	closed atomic.Bool
	codec  codec
}

func Open(ctx context.Context, opt Options) (*DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sm := opt.Store
	if sm == nil {
		return nil, ErrStoreRequired
	}
	return &DB{store: sm, codec: codec{}}, nil
}

func (db *DB) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil || db.closed.Load() {
		return ErrClosed
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	return db.store.Close()
}

func (db *DB) View(ctx context.Context, fn func(context.Context, Tx) error) error {
	if err := db.begin(ctx); err != nil {
		return err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed.Load() {
		return ErrClosed
	}
	tx := newTx(db, false)
	defer tx.close()
	return fn(ctx, tx)
}

func (db *DB) Update(ctx context.Context, fn func(context.Context, Tx) error) error {
	if err := db.begin(ctx); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed.Load() {
		return ErrClosed
	}
	tx := newTx(db, true)
	defer tx.close()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.commit(ctx)
}

func (db *DB) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil || db.closed.Load() {
		return ErrClosed
	}
	return nil
}
