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
	"gopkg.in/yaml.v3"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"bsky.watch/labeler/config"
	"bsky.watch/labeler/server"
	"bsky.watch/labeler/sign"
)

var (
	configFile = flag.String("config", "config.yaml", "Path to the config file")
	endpoint   = flag.String("from", "", "URL of the labeler to copy the labels from")
)

func runMain(ctx context.Context) error {
	if *endpoint == "" {
		return fmt.Errorf("--from is required")
	}

	b, err := os.ReadFile(*configFile)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	config := &config.Config{}
	if err := yaml.Unmarshal(b, config); err != nil {
		return fmt.Errorf("parsing config file: %w", err)
	}

	// Technically we don't need the key to write labels, but it's easier
	// to pass it here, than modify the server to allow nil key.
	key, err := sign.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("parsing private key: %w", err)
	}

	server, err := server.New(ctx, config.DBFile, config.DID, key)
	if err != nil {
		return fmt.Errorf("instantiating a server: %w", err)
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
		for _, label := range labels.Labels {
			op := "+"
			if label.Neg != nil && *label.Neg {
				op = "-"
			}
			fmt.Printf("%s %d\t%s\t%s\n", op, labels.Seq, label.Uri, label.Val)
			changed, err := server.AddLabel(*label)
			if err != nil {
				return fmt.Errorf("writing label: %w", err)
			}
			if !changed {
				fmt.Printf("The above label had no effect.\n")
			}
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
