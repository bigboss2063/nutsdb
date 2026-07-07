package nutsdb

import (
	"bytes"
	"encoding/binary"
)

const (
	keyNamespaceNormal byte = 0x01
	keyNamespacePlugin byte = 0x02
	keyFormatVersion   byte = 0x00
)

type codec struct{}

func (codec) encodeUserKey(bucket string, key []byte) ([]byte, error) {
	if bucket == "" {
		return nil, ErrBucket
	}
	if len(key) == 0 {
		return nil, ErrKeyEmpty
	}
	bucketBytes := []byte(bucket)
	buf := make([]byte, 0, 2+binary.MaxVarintLen64+len(bucketBytes)+len(key))
	buf = append(buf, keyNamespaceNormal, keyFormatVersion)
	buf = binary.AppendUvarint(buf, uint64(len(bucketBytes)))
	buf = append(buf, bucketBytes...)
	buf = append(buf, key...)
	return buf, nil
}

func (codec) bucketPrefix(bucket string) ([]byte, error) {
	if bucket == "" {
		return nil, ErrBucket
	}
	bucketBytes := []byte(bucket)
	buf := make([]byte, 0, 2+binary.MaxVarintLen64+len(bucketBytes))
	buf = append(buf, keyNamespaceNormal, keyFormatVersion)
	buf = binary.AppendUvarint(buf, uint64(len(bucketBytes)))
	buf = append(buf, bucketBytes...)
	return buf, nil
}

func (codec) decodeUserKey(physicalKey []byte) (bucket string, key []byte, err error) {
	if len(physicalKey) < 3 || physicalKey[0] != keyNamespaceNormal || physicalKey[1] != keyFormatVersion {
		return "", nil, ErrCorrupted
	}
	bucketLen, n := binary.Uvarint(physicalKey[2:])
	if n <= 0 {
		return "", nil, ErrCorrupted
	}
	bucketStart := 2 + n
	bucketEnd := bucketStart + int(bucketLen)
	if bucketEnd > len(physicalKey) {
		return "", nil, ErrCorrupted
	}
	return string(physicalKey[bucketStart:bucketEnd]), cloneBytes(physicalKey[bucketEnd:]), nil
}

func physicalKeyInRange(key []byte, prefix []byte, r Range, c codec) (bool, []byte, error) {
	if !bytes.HasPrefix(key, prefix) {
		return false, nil, nil
	}
	_, userKey, err := c.decodeUserKey(key)
	if err != nil {
		return false, nil, err
	}
	if len(r.Prefix) > 0 && !bytes.HasPrefix(userKey, r.Prefix) {
		return false, nil, nil
	}
	if len(r.Start) > 0 {
		cmp := bytes.Compare(userKey, r.Start)
		if cmp < 0 || (cmp == 0 && !r.IncludeStart) {
			return false, nil, nil
		}
	}
	if len(r.End) > 0 {
		cmp := bytes.Compare(userKey, r.End)
		if cmp > 0 || (cmp == 0 && !r.IncludeEnd) {
			return false, nil, nil
		}
	}
	return true, userKey, nil
}
