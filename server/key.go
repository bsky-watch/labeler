package server

import (
	"fmt"
	"math/big"
)

// We're using length-prefixed integers.
// First byte indicates length `n`, the next `n` are the number
// encoded as big-endian bytes.
// To maintain lexicographic ordering, each number must be encoded using
// minimal possible number of bytes.

func encodeKey(n int64) []byte {
	v := big.NewInt(n)
	b := v.Bytes()
	return append([]byte{byte(len(b))}, b...)
}

func decodeKey(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty key")
	}
	l := int(b[0])
	if len(b) != l+1 {
		return 0, fmt.Errorf("mismatching key length")
	}
	if l == 0 {
		return 0, fmt.Errorf("zero length key")
	}
	if b[1] == 0 {
		return 0, fmt.Errorf("non-minimal key encoding")
	}
	v := big.NewInt(0).SetBytes(b[1:])
	if !v.IsInt64() {
		return 0, fmt.Errorf("key does not fit into int64")
	}
	return v.Int64(), nil
}
