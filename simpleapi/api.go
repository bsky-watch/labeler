// Package simpleapi implements a very bare-bones API for mutating labeler's state.
//
// Handler accepts a POST request, with a partially populated label
// as JSON in the request body. It doesn't provide any authentication
// whatsoever, so make sure you're limiting who can access it.
package simpleapi

import (
	"context"
	"net/http"

	"github.com/Jille/convreq"
	"github.com/Jille/convreq/respond"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"bsky.watch/labeler/server"
)

type Handler struct {
	server  *server.Server
	handler http.Handler
}

// New returns HTTP handler to serve requests.
func New(server *server.Server) *Handler {
	h := &Handler{server: server}
	h.handler = convreq.Wrap(h.serve)
	return h
}

type label_JSON comatproto.LabelDefs_Label

func (h *Handler) serve(ctx context.Context, post label_JSON) convreq.HttpResponse {
	changed, err := h.server.AddLabel(comatproto.LabelDefs_Label(post))
	if err != nil {
		return respond.BadRequest(err.Error())
	}
	if changed {
		return respond.Created("OK")
	}
	return respond.String("OK")
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.handler.ServeHTTP(w, req)
}
