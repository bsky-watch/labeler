package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"

	"bsky.watch/utils/xrpcauth"

	"bsky.watch/labeler/account"
	"bsky.watch/labeler/config"
	"bsky.watch/labeler/logging"
	"bsky.watch/labeler/server"
)

var (
	listenAddr   = flag.String("listen-addr", ":8081", "IP:port to listen on")
	logFile      = flag.String("log-file", "", "File to write the logs to. Will use stderr if not set")
	logFormat    = flag.String("log-format", "text", "Log entry format, 'text' or 'json'.")
	logLevel     = flag.Int("log-level", 0, "Log level. 0 - debug, 1 - info, 3 - error")
	testDuration = flag.Duration("test-duration", 10*time.Second, "The desired duration of a stress test")
	numWriters   = flag.Int("writers", 3, "Number of concurrent writers")
	postgresUrl  = flag.String("postgres-url", "", "URL of the DB to use. (if empty - will use in-memory SQLite)")
)

var cfg = &config.Config{
	DID:        "did:example:test",
	PrivateKey: "c6d40ec53c689ca905036e41d8c73560777e5746d1d228fd6f9db56efed8ecaf",
}

func runMain(ctx context.Context) error {
	log := zerolog.Ctx(ctx)

	if *postgresUrl != "" {
		cfg.PostgresURL = *postgresUrl
	} else {
		cfg.SQLiteDB = "file:testdb0?mode=memory&cache=shared"
	}

	server, err := server.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("instantiating a server: %w", err)
	}

	if cfg.Password != "" && len(cfg.Labels.LabelValueDefinitions) > 0 {
		client := xrpcauth.NewClientWithTokenSource(ctx, xrpcauth.PasswordAuth(cfg.DID, cfg.Password))
		err := account.UpdateLabelDefs(ctx, client, &cfg.Labels)
		if err != nil {
			return fmt.Errorf("updating label definitions: %w", err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/xrpc/com.atproto.label.subscribeLabels", server.Subscribe())
	mux.Handle("/xrpc/com.atproto.label.queryLabels", server.Query())

	ch := make(chan struct{})
	go func() {
		close(ch)
		log.Info().Msgf("Starting HTTP listener on %s...", *listenAddr)
		if err := http.ListenAndServe(*listenAddr, mux); err != nil {
			log.Fatal().Err(err).Msgf("Failed to start HTTP server: %s", err)
		}
	}()
	<-ch // Force the above goroutine to start
	time.Sleep(time.Second)

	subCtx, cancel := context.WithTimeout(ctx, *testDuration)
	defer cancel()

	var wg sync.WaitGroup
	for range *numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			labelWriter(subCtx, server)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := subscriber(subCtx, *listenAddr)
		if err != nil {
			log.Fatal().Err(err).Msgf("subscriber failed: %s", err)
		}
	}()

	wg.Wait()

	fmt.Println()
	fmt.Printf("Labels written:\t%d\n", counters.LabelsWritten.Load())
	fmt.Printf("Labels skipped:\t%d\n", counters.LabelsSkipped.Load())
	fmt.Printf("Write errors:\t%d\n", counters.WriteErrors.Load())
	fmt.Printf("Cursor rollbacks:\t%d\n", counters.CursorRollbacks.Load())
	fmt.Printf("Unexpected messages:\t%d\n", counters.BadMessages.Load())
	fmt.Printf("Connects:\t%d\n", counters.Connects.Load())

	if n := counters.WriteErrors.Load(); n > 0 {
		return fmt.Errorf("write errors count: %d", n)
	}
	if n := counters.CursorRollbacks.Load(); n > 0 {
		return fmt.Errorf("cursor rollbacks: %d", n)
	}
	if n := counters.BadMessages.Load(); n > 0 {
		return fmt.Errorf("unexpected messages: %d", n)
	}
	return nil
}

var counters struct {
	LabelsWritten   atomic.Int64
	LabelsSkipped   atomic.Int64
	WriteErrors     atomic.Int64
	CursorRollbacks atomic.Int64
	BadMessages     atomic.Int64
	Connects        atomic.Int64
}

func labelWriter(ctx context.Context, labeler *server.Server) {
	log := zerolog.Ctx(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		subject := fmt.Sprintf("at://did:example:subject/collection/%d", rand.Int())
		added, err := labeler.AddLabel(ctx, atproto.LabelDefs_Label{
			Uri: subject,
			Val: "test",
		})
		if err != nil {
			log.Error().Err(err).Msgf("Write failed: %s", err)
			counters.WriteErrors.Add(1)
		} else {
			if added {
				counters.LabelsWritten.Add(1)
			} else {
				counters.LabelsSkipped.Add(1)
			}
		}
	}
}

func subscriber(ctx context.Context, addr string) error {
	log := zerolog.Ctx(ctx)

	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	u, err := url.Parse("http://" + addr)
	if err != nil {
		return fmt.Errorf("failed to parse %q: %w", addr, err)
	}

	cursor := int64(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		u.Scheme = "ws"
		u.Path = "/xrpc/com.atproto.label.subscribeLabels"
		u.RawQuery = fmt.Sprintf("cursor=%d", cursor)

		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", u.String(), err)
		}
		counters.Connects.Add(1)

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
		conn.Close()

		if !bytes.HasPrefix(b, []byte("\xa2atg#labelsbop\x01")) {
			fmt.Printf("Unexpected prefix: %q", string(b))
			continue
		}
		labels := &atproto.LabelSubscribeLabels_Labels{}
		err = labels.UnmarshalCBOR(bytes.NewBuffer(bytes.TrimPrefix(b, []byte("\xa2atg#labelsbop\x01"))))
		if err != nil {
			return fmt.Errorf("unmarshaling labels: %w", err)
		}
		if len(labels.Labels) > 1 {
			return fmt.Errorf("unsupported: seq %d has more than one label", labels.Seq)
		}
		if cursor >= labels.Seq {
			counters.CursorRollbacks.Add(1)
			log.Error().Msgf("Bad seq change %d -> %d", cursor, labels.Seq)
		}
		cursor = labels.Seq
	}
	return nil
}

func main() {
	flag.Parse()

	ctx := logging.Setup(context.Background(), *logFile, *logFormat, zerolog.Level(*logLevel))
	log := zerolog.Ctx(ctx)

	if err := runMain(ctx); err != nil {
		log.Fatal().Err(err).Msgf("%s", err)
	}
}
