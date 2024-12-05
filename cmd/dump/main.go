package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

var (
	endpoint = flag.String("from", "", "URL of the labeler to copy the labels from")
)

func runMain(ctx context.Context) error {
	if *endpoint == "" {
		return fmt.Errorf("--from is required")
	}

	u, err := url.Parse(*endpoint)
	if err != nil {
		return err
	}
	u.Scheme = "wss"
	u.Path = "/xrpc/com.atproto.label.subscribeLabels"
	u.RawQuery = "cursor=0"

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", u.String(), err)
	}

	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return fmt.Errorf("setting read deadline: %w", err)
		}
		_, b, err := conn.ReadMessage()
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) || strings.HasSuffix(err.Error(), os.ErrDeadlineExceeded.Error()) {
				break
			}
			return fmt.Errorf("reading from websocket: %w", err)
		}

		if !bytes.HasPrefix(b, []byte("\xa2atg#labelsbop\x01")) {
			fmt.Printf("Unexpected prefix: %q", string(b))
			continue
		}
		labels := &comatproto.LabelSubscribeLabels_Labels{}
		err = labels.UnmarshalCBOR(bytes.NewBuffer(bytes.TrimPrefix(b, []byte("\xa2atg#labelsbop\x01"))))
		if err != nil {
			return fmt.Errorf("unmarshaling labels: %w", err)
		}
		if len(labels.Labels) > 1 {
			return fmt.Errorf("unsupported: seq %d has more than one label", labels.Seq)
		}
		for _, label := range labels.Labels {
			op := "+"
			if label.Neg != nil && *label.Neg {
				op = "-"
			}
			fmt.Printf("%s %d\t%s\t%s\t%s\n", op, labels.Seq, label.Uri, label.Val, label.Cts)
		}
	}
	conn.Close()
	return nil
}

func main() {
	flag.Parse()

	if err := runMain(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
