package account

import (
	"context"
	"fmt"
	"reflect"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

func UpdateLabelDefs(ctx context.Context, client *xrpc.Client, defs *bsky.LabelerDefs_LabelerPolicies) error {
	session, err := comatproto.ServerGetSession(ctx, client)
	if err != nil {
		return fmt.Errorf("com.atproto.server.getSession: %w", err)
	}

	resp, err := comatproto.RepoGetRecord(ctx, client, "", "app.bsky.labeler.service", session.Did, "self")
	if err != nil {
		return fmt.Errorf("com.atproto.repo.getRecord: %w", err)
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
	})
	if err != nil {
		return fmt.Errorf("com.atproto.repo.putRecord: %w", err)
	}
	return nil
}
