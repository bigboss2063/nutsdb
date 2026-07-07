package nutsdb

import "github.com/nutsdb/nutsdb/internal/store"

// Options configures DB.Open. The next control plane currently defaults to an
// in-memory StoreManager; Dir is reserved for the future disk-backed data plane.
type Options struct {
	Dir   string
	Store store.StoreManager
}
