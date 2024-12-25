package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Jille/convreq"
	"github.com/Jille/convreq/respond"
	"github.com/imax9000/errors"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"bsky.watch/labeler/sign"
)

type queryRequestGet struct {
	UriPatterns []string `schema:"uriPatterns"`
	Sources     []string `schema:"sources"`
	// Ignoring `limit` and `cursor`
}

type errUnsupportedPattern string

func (s errUnsupportedPattern) Error() string {
	return fmt.Sprintf("unsupported pattern %q", string(s))
}

func (err errUnsupportedPattern) Respond(w http.ResponseWriter, r *http.Request) error {
	http.Error(w, fmt.Sprintf("I received your request but I decided to ignore you.\n\n%s", err), 448)
	return nil
}

func (q *queryRequestGet) Validate() error {
	if len(q.UriPatterns) == 0 {
		return fmt.Errorf("need at least one pattern")
	}
	for _, p := range q.UriPatterns {
		switch {
		case strings.HasPrefix(p, "did:"):
			if strings.Contains(p, "*") {
				return errUnsupportedPattern(p)
			}
		case strings.HasPrefix(p, "at://"):
			// We don't support wildcards yet. Even if only the rkey is wildcarded,
			// the query becomes too broad.
			if strings.Contains(p, "*") {
				return errUnsupportedPattern(p)
			}
		default:
			return fmt.Errorf("invalid pattern %q", p)
		}
	}
	return nil
}

func (s *Server) query(ctx context.Context, get queryRequestGet) ([]Entry, error) {
	var entries []Entry
	var err error
	if len(get.Sources) == 0 {
		err = s.db.Model(&entries).
			Where("uri in ?", get.UriPatterns).
			Order("seq asc").
			Find(&entries).Error
	} else {
		err = s.db.Model(&entries).
			Where("uri in ? and src in ?", get.UriPatterns, get.Sources).
			Order("seq asc").
			Find(&entries).Error
	}
	if err != nil {
		return nil, err
	}

	return dedupeAndNegateEntries(entries), nil
}

// Query returns HTTP handler that implements [com.atproto.label.queryLabels](https://docs.bsky.app/docs/api/com-atproto-label-query-labels) XRPC method.
func (s *Server) Query() http.Handler {
	return convreq.Wrap(func(ctx context.Context, get queryRequestGet) convreq.HttpResponse {
		if err := get.Validate(); err != nil {
			if err, ok := errors.As[errUnsupportedPattern](err); ok {
				return err
			}
			return respond.BadRequest(err.Error())
		}

		result, err := s.query(ctx, get)
		if err != nil {
			return respond.InternalServerError("failed to query labels")
		}

		var r []comatproto.LabelDefs_Label
		for i := range result {
			l := result[i].ToLabel()
			if err := sign.Sign(ctx, s.privateKey, &l); err != nil {
				return respond.InternalServerError("failed to sign the labels")
			}
			r = append(r, l)
		}

		return respond.JSON(map[string]any{"labels": r})
	})
}
