package server

import (
	"context"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	bolt "go.etcd.io/bbolt"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"bsky.watch/labeler/sign"
)

const labelerDID = "did:example"
const testDID = "did:foo"
const otherDID = "did:bar"
const privateKey = "c6d40ec53c689ca905036e41d8c73560777e5746d1d228fd6f9db56efed8ecaf"

func NewTestServer(ctx context.Context) (*Server, error) {
	key, err := sign.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, err
	}

	f, err := os.CreateTemp("", "labeler_test.*.bolt")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	f.Close()

	// Delete the file after it is opened by New().
	defer func() { os.Remove(path) }()

	return New(ctx, path, labelerDID, key)
}

func TestBasic(t *testing.T) {
	ctx := context.Background()

	type testQuery struct {
		Query          []string
		ExpectedLabels []Entry
	}
	type testCase struct {
		Name           string
		Labels         []comatproto.LabelDefs_Label
		ExpectedLabels []Entry
	}

	cases := []testCase{
		{
			Name: "One label",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
			},
			ExpectedLabels: []Entry{{Val: "a"}},
		},
		{
			Name: "Multiple labels",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "b"},
				{Val: "c"},
			},
			ExpectedLabels: []Entry{{Val: "a"}, {Val: "b"}, {Val: "c"}},
		},
		{
			Name: "No duplicates",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a"},
				{Val: "a"},
			},
			ExpectedLabels: []Entry{{Val: "a"}},
		},
		{
			Name: "Negation",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a", Neg: ptr(true)},
			},
			ExpectedLabels: []Entry{},
		},
		{
			Name: "Added after negation",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a", Neg: ptr(true)},
				{Val: "a"},
			},
			ExpectedLabels: []Entry{{Val: "a"}},
		},
		{
			Name: "CID creates a new label",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a", Cid: ptr("a")},
			},
			ExpectedLabels: []Entry{{Val: "a"}, {Val: "a", Cid: ptr("a")}},
		},

		// The following were derived from reading Ozone's source code, i.e.,
		// not actually verified empirically. And it goes without saying that
		// the spec doesn't define these cases.
		{
			Name: "Expiration updates the label",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a", Exp: ptr("a")},
				{Val: "b", Cid: ptr("b")},
				{Val: "b", Cid: ptr("b"), Exp: ptr("b")},
				{Val: "c", Exp: ptr("c")},
				{Val: "c"},
				{Val: "d", Cid: ptr("d"), Exp: ptr("d")},
				{Val: "d", Cid: ptr("d")},
			},
			ExpectedLabels: []Entry{
				{Val: "a", Exp: ptr("a")},
				{Val: "b", Cid: ptr("b"), Exp: ptr("b")},
				{Val: "c"},
				{Val: "d", Cid: ptr("d")},
			},
		},
		{
			Name: "Negation and CID",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a", Cid: ptr("a")},
				{Val: "b"},
				{Val: "b", Cid: ptr("b")},
				{Val: "c"},
				{Val: "d", Cid: ptr("d")},

				{Val: "a", Neg: ptr(true)},
				{Val: "b", Cid: ptr("b"), Neg: ptr(true)},
				{Val: "c", Cid: ptr("c"), Neg: ptr(true)}, // no-op
				{Val: "d", Neg: ptr(true)},                // no-op
			},
			ExpectedLabels: []Entry{
				{Val: "a", Cid: ptr("a")},
				{Val: "b"},
				{Val: "c"},
				{Val: "d", Cid: ptr("d")},
			},
		},
		{
			Name: "Negating label with expiration",
			Labels: []comatproto.LabelDefs_Label{
				{Val: "a"},
				{Val: "a", Exp: ptr("a")},
				{Val: "b", Cid: ptr("b")},
				{Val: "b", Cid: ptr("b"), Exp: ptr("b")},

				{Val: "a", Neg: ptr(true)},
				{Val: "b", Cid: ptr("b"), Neg: ptr(true)},
			},
			ExpectedLabels: []Entry{},
		},
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreFields(Entry{}, "Cts", "Ver", "Src"),
		cmpopts.SortSlices(func(a Entry, b Entry) bool {
			if a.Val != b.Val {
				return a.Val < b.Val
			}
			if a.Cid != nil || b.Cid != nil {
				if a.Cid == nil {
					return true
				}
				if b.Cid == nil {
					return false
				}
				if *a.Cid < *b.Cid {
					return true
				}
			}
			if a.Exp != nil || b.Exp != nil {
				if a.Exp == nil {
					return true
				}
				if b.Exp == nil {
					return false
				}
				if *a.Exp < *b.Exp {
					return true
				}
			}
			return false
		}),
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			server, err := NewTestServer(ctx)
			if err != nil {
				t.Fatal(err)
			}

			for _, l := range tc.Labels {
				if l.Uri == "" {
					l.Uri = testDID
				}
				if _, err := server.AddLabel(l); err != nil {
					t.Fatal(err)
				}
			}

			entries, err := server.query(ctx, queryRequestGet{UriPatterns: []string{testDID}})
			if err != nil {
				t.Fatal(err)
			}

			expected := []Entry{}
			for _, l := range tc.ExpectedLabels {
				l.Uri = testDID
				expected = append(expected, l)
			}
			if diff := cmp.Diff(expected, entries, cmpOpts...); diff != "" {
				t.Errorf(diff)

				server.db.View(func(tx *bolt.Tx) error {
					return tx.Bucket([]byte(bucketName)).ForEach(func(k, v []byte) error {
						t.Log(string(v))
						return nil
					})
				})
			}
		})
	}
}
