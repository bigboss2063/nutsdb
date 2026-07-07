package nutsdb

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/store"
	"github.com/stretchr/testify/require"
)

func TestDBUpdateViewAndBucketFacade(t *testing.T) {
	ctx := context.Background()
	db, err := openTestDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx Tx) error {
		b := tx.Bucket("default")
		require.Equal(t, "default", b.Name())
		require.NoError(t, b.Put(ctx, []byte("k"), []byte("v")))
		got, err := b.Get(ctx, []byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), got)
		return nil
	}))

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx Tx) error {
		got, err := tx.Bucket("default").Get(ctx, []byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), got)
		return nil
	}))
}

func TestTxRollbackOnCallbackError(t *testing.T) {
	ctx := context.Background()
	db, err := openTestDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	boom := errors.New("boom")
	err = db.Update(ctx, func(ctx context.Context, tx Tx) error {
		require.NoError(t, tx.Put(ctx, "default", []byte("k"), []byte("v")))
		return boom
	})
	require.ErrorIs(t, err, boom)

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx Tx) error {
		_, err := tx.Get(ctx, "default", []byte("k"))
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))
}

func TestReadOnlyTxRejectsWrites(t *testing.T) {
	ctx := context.Background()
	db, err := openTestDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx Tx) error {
		require.ErrorIs(t, tx.Put(ctx, "default", []byte("k"), []byte("v")), ErrReadOnly)
		require.ErrorIs(t, tx.Delete(ctx, "default", []byte("k")), ErrReadOnly)
		return nil
	}))
}

func TestBucketsAreIsolated(t *testing.T) {
	ctx := context.Background()
	db, err := openTestDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx Tx) error {
		require.NoError(t, tx.Put(ctx, "a", []byte("k"), []byte("va")))
		require.NoError(t, tx.Put(ctx, "b", []byte("k"), []byte("vb")))
		return nil
	}))

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx Tx) error {
		gotA, err := tx.Get(ctx, "a", []byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("va"), gotA)
		gotB, err := tx.Get(ctx, "b", []byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("vb"), gotB)
		return nil
	}))
}

func TestTxScanSeesPendingOverlay(t *testing.T) {
	ctx := context.Background()
	db, err := openTestDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx Tx) error {
		require.NoError(t, tx.Put(ctx, "default", []byte("a"), []byte("old-a")))
		require.NoError(t, tx.Put(ctx, "default", []byte("b"), []byte("old-b")))
		require.NoError(t, tx.Put(ctx, "other", []byte("a"), []byte("other-a")))
		return nil
	}))

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx Tx) error {
		require.NoError(t, tx.Put(ctx, "default", []byte("a"), []byte("new-a")))
		require.NoError(t, tx.Delete(ctx, "default", []byte("b")))
		require.NoError(t, tx.Put(ctx, "default", []byte("c"), []byte("new-c")))

		var got []Item
		err := tx.Scan(ctx, "default", Range{}, func(ctx context.Context, item Item) error {
			got = append(got, item)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, []Item{
			{Key: []byte("a"), Value: []byte("new-a")},
			{Key: []byte("c"), Value: []byte("new-c")},
		}, got)
		return nil
	}))
}

func openTestDB(ctx context.Context) (*DB, error) {
	return Open(ctx, Options{Store: newTestStore()})
}

type testStore struct {
	mu     sync.RWMutex
	closed bool
	data   map[string]*core.Record
}

func newTestStore() *testStore {
	return &testStore{data: make(map[string]*core.Record)}
}

func (s *testStore) Get(ctx context.Context, key []byte) (*core.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rec, ok := s.data[string(key)]; ok {
		return cloneCoreRecord(rec), nil
	}
	return nil, store.ErrKeyNotFound
}

func (s *testStore) Put(ctx context.Context, key []byte, value *core.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[string(key)] = cloneCoreRecord(value).WithKey(cloneBytes(key))
	return nil
}

func (s *testStore) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
	return nil
}

func (s *testStore) Iterate(ctx context.Context, callback func(key []byte, value *core.Record) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !callback([]byte(key), cloneCoreRecord(s.data[key])) {
			break
		}
	}
	return nil
}

func (s *testStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *testStore) BatchPut(ctx context.Context, records []struct {
	Key   []byte
	Value *core.Record
}) error {
	for _, rec := range records {
		if err := s.Put(ctx, rec.Key, rec.Value); err != nil {
			return err
		}
	}
	return nil
}

func (s *testStore) BatchDelete(ctx context.Context, keys [][]byte) error {
	for _, key := range keys {
		if err := s.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *testStore) BatchGet(ctx context.Context, keys [][]byte) ([]struct {
	Key   []byte
	Value *core.Record
}, error) {
	results := make([]struct {
		Key   []byte
		Value *core.Record
	}, 0, len(keys))
	for _, key := range keys {
		rec, err := s.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		results = append(results, struct {
			Key   []byte
			Value *core.Record
		}{Key: cloneBytes(key), Value: rec})
	}
	return results, nil
}
