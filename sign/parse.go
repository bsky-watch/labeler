package sign

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multicodec"
	"gitlab.com/yawning/secp256k1-voi/secec"
)

// ParsePrivateKey parses hex-encoded string into a private key.
func ParsePrivateKey(s string) (*secec.PrivateKey, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return secec.NewPrivateKey(b)
}

// GetPublicKey returns a string representation of the public key
// that corresponds to the given private key.
func GetPublicKey(private *secec.PrivateKey) (string, error) {
	b := binary.AppendUvarint(nil, uint64(multicodec.Secp256k1Pub))
	b = append(b, private.PublicKey().CompressedBytes()...)
	return multibase.Encode(multibase.Base58BTC, b)
}
