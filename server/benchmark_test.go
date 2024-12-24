package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/bluesky-social/indigo/api/atproto"
)

const benchmarkDBSize = 100000

func BenchmarkQuerySingleResult(b *testing.B) {
	ctx := context.Background()
	server, err := NewTestServer(ctx)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < benchmarkDBSize/2; i++ {
		server.AddLabel(atproto.LabelDefs_Label{Val: fmt.Sprintf("a%d", i), Uri: testDID})
	}
	server.AddLabel(atproto.LabelDefs_Label{Val: "label", Uri: otherDID})
	for i := 0; i < benchmarkDBSize/2; i++ {
		server.AddLabel(atproto.LabelDefs_Label{Val: fmt.Sprintf("b%d", i), Uri: testDID})
	}

	b.ResetTimer()
	for range b.N {
		if _, err := server.query(ctx, queryRequestGet{UriPatterns: []string{otherDID}}); err != nil {
			b.Error(err)
		}
	}
}
