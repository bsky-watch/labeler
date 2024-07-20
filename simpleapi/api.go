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

func New(server *server.Server) *Handler {
	h := &Handler{server: server}
	h.handler = convreq.Wrap(h.serve)
	return h
}

func (h *Handler) serve(ctx context.Context, postJSON *comatproto.LabelDefs_Label) convreq.HttpResponse {
	changed, err := h.server.AddLabel(*postJSON)
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
