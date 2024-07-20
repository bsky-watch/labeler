package sign

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	"gitlab.com/yawning/secp256k1-voi/secec"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/lex/util"
)

// Sign adds a signature to the entry.
func Sign(ctx context.Context, key *secec.PrivateKey, entry *comatproto.LabelDefs_Label) error {
	entry.Sig = nil
	buf := bytes.NewBuffer(nil)
	if err := entry.MarshalCBOR(buf); err != nil {
		return err
	}
	h := sha256.Sum256(buf.Bytes())
	signature, err := key.Sign(nil, h[:], &secec.ECDSAOptions{Encoding: secec.EncodingCompact})
	if err != nil {
		return fmt.Errorf("failed to generate signature: %w", err)
	}
	entry.Sig = util.LexBytes(signature)
	return nil
}
