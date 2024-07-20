package account

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"

	"bsky.watch/utils/plc"
)

// UpdateSigningKeyAndEndpoint updates labeler's public key and (optionally) endpoint,
// if the current values in PLC are different.
func UpdateSigningKeyAndEndpoint(ctx context.Context, client *xrpc.Client, token string, publicKey string, endpoint string) error {
	session, err := comatproto.ServerGetSession(ctx, client)
	if err != nil {
		return fmt.Errorf("com.atproto.server.getSession: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("https://plc.directory/%s/data", session.Did), nil)
	if err != nil {
		return fmt.Errorf("creating request object: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetching PLC data: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("PLC returned %s", resp.Status)
	}

	data := &plc.Op{}
	if err := json.NewDecoder(resp.Body).Decode(data); err != nil {
		resp.Body.Close()
		return fmt.Errorf("failed to parse PLC response: %w", err)
	}
	resp.Body.Close()

	update := map[string]any{}
	publicKey = "did:key:" + publicKey
	if data.VerificationMethods == nil || data.VerificationMethods["atproto_label"] != publicKey {
		methods := data.VerificationMethods
		if methods == nil {
			methods = map[string]string{}
		}
		methods["atproto_label"] = publicKey
		update["verificationMethods"] = methods
	}
	if endpoint != "" && (data.Services == nil || data.Services["atproto_labeler"].Endpoint != endpoint) {
		services := data.Services
		if services == nil {
			services = map[string]plc.Service{}
		}
		services["atproto_labeler"] = plc.Service{
			Endpoint: endpoint,
			Type:     "AtprotoLabeler",
		}
	}
	if len(update) == 0 {
		// No changes needed.
		return nil
	}

	if token == "" {
		err := comatproto.IdentityRequestPlcOperationSignature(ctx, client)
		if err != nil {
			return fmt.Errorf("failed to request signature from PDS: %w", err)
		}
		return fmt.Errorf("please check your email for a message with a token, required to update information in PLC")
	}

	update["token"] = token
	var signedOp struct {
		Operation plc.Op `json:"operation"`
	}
	err = client.Do(ctx, xrpc.Procedure, "application/json",
		"com.atproto.identity.signPlcOperation", nil, update, &signedOp)
	if err != nil {
		return fmt.Errorf("failed to get a signature for the PLC operation from PDS: %w", err)
	}
	err = client.Do(ctx, xrpc.Procedure, "application/json",
		"com.atproto.identity.submitPlcOperation", nil,
		signedOp, nil)
	if err != nil {
		return fmt.Errorf("failed to update rotation keys in PLC via PDS: %w", err)
	}
	return nil
}
