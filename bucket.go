package nutsdb

import "context"

// Bucket is a tx-scoped facade over one logical bucket namespace. It does not
// represent persisted bucket metadata.
type Bucket interface {
	Name() string
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Scan(ctx context.Context, r Range, fn func(context.Context, Item) error) error
}

type bucket struct {
	tx   Tx
	name string
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) Get(ctx context.Context, key []byte) ([]byte, error) {
	return b.tx.Get(ctx, b.name, key)
}

func (b *bucket) Put(ctx context.Context, key, value []byte) error {
	return b.tx.Put(ctx, b.name, key, value)
}

func (b *bucket) Delete(ctx context.Context, key []byte) error {
	return b.tx.Delete(ctx, b.name, key)
}

func (b *bucket) Scan(ctx context.Context, r Range, fn func(context.Context, Item) error) error {
	return b.tx.Scan(ctx, b.name, r, fn)
}
