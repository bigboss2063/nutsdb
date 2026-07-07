package nutsdb

const (
	valueFormatVersion byte = 0x00
	valueFlagsNone     byte = 0x00
)

func (codec) encodeValue(value []byte) []byte {
	buf := make([]byte, 0, 2+len(value))
	buf = append(buf, valueFormatVersion, valueFlagsNone)
	buf = append(buf, value...)
	return buf
}

func (codec) decodeValue(physicalValue []byte) ([]byte, error) {
	if len(physicalValue) < 2 || physicalValue[0] != valueFormatVersion {
		return nil, ErrCorrupted
	}
	return cloneBytes(physicalValue[2:]), nil
}
