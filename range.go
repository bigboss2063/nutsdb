package nutsdb

// Range describes a logical user-key range inside one bucket.
type Range struct {
	Start        []byte
	End          []byte
	Prefix       []byte
	IncludeStart bool
	IncludeEnd   bool
	Limit        int
	Reverse      bool
}

// Item is returned by Tx.Scan/Bucket.Scan with logical user key and value.
type Item struct {
	Key   []byte
	Value []byte
}
