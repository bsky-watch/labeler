package account

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/imax9000/errors"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"

	"bsky.watch/labeler/config"
)

// UpdateLabelDefs checks if labeler policy of the account that `client` is logged in with
// is the same as `defs`. If it doesn't - it will try to update it.
func UpdateLabelDefs(ctx context.Context, client *xrpc.Client, defs *bsky.LabelerDefs_LabelerPolicies) error {
	cfg := &config.Config{Labels: *defs}
	cfg.UpdateLabelValues()
	defs = &cfg.Labels

	session, err := comatproto.ServerGetSession(ctx, client)
	if err != nil {
		return fmt.Errorf("com.atproto.server.getSession: %w", err)
	}

	resp, err := comatproto.RepoGetRecord(ctx, client, "", "app.bsky.labeler.service", session.Did, "self")
	if err != nil {
		missingRecord := false
		if err, ok := errors.As[*xrpc.XRPCError](err); ok {
			if strings.HasPrefix(err.Message, "Could not locate record: ") {
				missingRecord = true
				resp = &comatproto.RepoGetRecord_Output{
					Value: &util.LexiconTypeDecoder{
						Val: &bsky.LabelerService{
							LexiconTypeID: "app.bsky.labeler.service",
							CreatedAt:     time.Now().Format(time.RFC3339),
						},
					},
				}
			}
		}
		if !missingRecord {
			return fmt.Errorf("com.atproto.repo.getRecord: %w", err)
		}
	}

	current, ok := resp.Value.Val.(*bsky.LabelerService)
	if !ok {
		return fmt.Errorf("unexpected record type %T", resp.Value.Val)
	}

	if reflect.DeepEqual(current.Policies, defs) {
		// No changes needed.
		return nil
	}

	current.Policies = defs
	_, err = comatproto.RepoPutRecord(ctx, client, &comatproto.RepoPutRecord_Input{
		Collection: "app.bsky.labeler.service",
		Repo:       session.Did,
		Rkey:       "self",
		Record:     &util.LexiconTypeDecoder{Val: current},
		SwapRecord: resp.Cid,
	})
	if err != nil {
		return fmt.Errorf("com.atproto.repo.putRecord: %w", err)
	}
	return nil
}
