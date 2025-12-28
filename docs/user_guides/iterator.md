# Iterator

NutsDB stores its keys in byte-sorted order within a bucket. This makes sequential iteration over these keys extremely fast.

### Prefix scans

To iterate over a key prefix, we can use `PrefixScanEntries` function, and the parameters `offsetNum` and `limitNum` constrain the number of entries returned :

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        prefix := []byte("user_")
        bucket := "user_list"
        // Constrain 100 entries returned 
        if keys, values, err := tx.PrefixScanEntries(bucket, prefix, "", 25, 100, true, true); err != nil {
            return err
        } else {
            for k := range keys {
                fmt.Println(string(keys[k]), string(values[k]))
            }
        }
        return nil
    }); err != nil {
        log.Fatal(err)
}
```

### Prefix search scans

To iterate over a key prefix with search by regular expression on a second part of key without prefix, we can use `PrefixScanEntries` function when using the reg parameter,the parameters `offsetNum`, `limitNum` constrain the number of entries returned :

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        prefix := []byte("user_")
        reg := "username"
        bucket := "user_list"
        // Constrain 100 entries returned 
        if keys, values, err := tx.PrefixScanEntries(bucket, prefix, reg, 25, 100, true, true); err != nil {
            return err
        } else {
            for k := range keys {
                fmt.Println(string(keys[k]), string(values[k]))
            }
        }
        return nil
    }); err != nil {
        log.Fatal(err)
}
```

### Range scans

To scan over a range, we can use `RangeScanEntries` function. For example：

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        // Assume key from user_0000000 to user_9999999.
        // Query a specific user key range like this.
        start := []byte("user_0010001")
        end := []byte("user_0010010")
        bucket := "user_list"
        if keys, values, err := tx.RangeScanEntries(bucket, start, end, true, true); err != nil {
            return err
        } else {
            for k := range keys {
                fmt.Println(string(keys[k]), string(values[k]))
            }
        }
        return nil
    }); err != nil {
    log.Fatal(err)
}
```

### Get all

To scan all keys and values of the bucket stored, we can use `GetAll` function. For example:

```go
if err := db.View(
    func(tx *nutsdb.Tx) error {
        bucket := "user_list"
        entries, err := tx.GetAll(bucket)
        if err != nil {
            return err
        }

        for _, entry := range entries {
            fmt.Println(string(entry.Key),string(entry.Value))
        }

        return nil
    }); err != nil {
    log.Println(err)
}
```

### Iterator

NutsDB provides a KV iterator (BTree bucket) with streaming prefix/range constraints and TTL filtering.

Key points:
- `NewIterator` never returns `nil`. Check `it.Err()` after creation.
- Iterate without manual `break` using `for it.Rewind(); it.Valid(); it.Next() { ... }` or `it.ForEach(...)`.
- `it.Item()` returns an `*IteratorItem` whose `Key()` / `Value()` results are only valid until the next `Next/Seek/Rewind` and while the transaction is open. Use `KeyCopy/ValueCopy` for stable copies.

#### Forward iteration

```go
_ = db.View(func(tx *nutsdb.Tx) error {
    it := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{})
    defer it.Close()
    if err := it.Err(); err != nil {
        return err
    }

    for it.Rewind(); it.Valid(); it.Next() {
        item := it.Item()
        k := item.Key()
        v, _ := item.ValueCopy(nil)
        fmt.Println(string(k), string(v))
    }
    return nil
})
```

#### Reverse iteration

```go
_ = db.View(func(tx *nutsdb.Tx) error {
    it := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{Reverse: true})
    defer it.Close()
    if err := it.Err(); err != nil {
        return err
    }

    for it.Rewind(); it.Valid(); it.Next() {
        item := it.Item()
        k := item.Key()
        v, _ := item.ValueCopy(nil)
        fmt.Println(string(k), string(v))
    }
    return nil
})
```

#### Prefix scan (streaming)

```go
_ = db.View(func(tx *nutsdb.Tx) error {
    it := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{Prefix: []byte(\"user:\")})
    defer it.Close()
    if err := it.Err(); err != nil {
        return err
    }

    it.ForEach(func(item *nutsdb.IteratorItem) bool {
        fmt.Println(string(item.Key()))
        return true
    })
    return nil
})
```

#### Range scan (inclusive)

```go
_ = db.View(func(tx *nutsdb.Tx) error {
    it := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{
        Start: []byte(\"user:0010\"),
        End:   []byte(\"user:0020\"),
    })
    defer it.Close()
    if err := it.Err(); err != nil {
        return err
    }

    for it.Rewind(); it.Valid(); it.Next() {
        fmt.Println(string(it.Item().Key()))
    }
    return nil
})
```

#### TTL (expired keys)

By default, expired keys are excluded. Use `IncludeExpired: true` to include them in the iterator output.
