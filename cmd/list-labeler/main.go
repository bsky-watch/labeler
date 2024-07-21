package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"

	"bsky.watch/utils/didset"
	"bsky.watch/utils/xrpcauth"

	"bsky.watch/labeler/account"
	"bsky.watch/labeler/config"
	"bsky.watch/labeler/logging"
	"bsky.watch/labeler/server"
	"bsky.watch/labeler/sign"
)

var (
	configFile     = flag.String("config", "config.yaml", "Path to the config file")
	listenAddr     = flag.String("listen-addr", ":8081", "IP:port to listen on")
	logFile        = flag.String("log-file", "", "File to write the logs to. Will use stderr if not set")
	logFormat      = flag.String("log-format", "text", "Log entry format, 'text' or 'json'.")
	logLevel       = flag.Int("log-level", 1, "Log level. 0 - debug, 1 - info, 3 - error")
	updateInterval = flag.Duration("update-interval", time.Hour, "Interval between updates")
)

type Config struct {
	config.Config `yaml:",inline"`

	Lists map[string]string `yaml:"lists"`
}

func runMain(ctx context.Context) error {
	log := zerolog.Ctx(ctx)

	b, err := os.ReadFile(*configFile)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(b, config); err != nil {
		return fmt.Errorf("parsing config file: %w", err)
	}
	config.UpdateLabelValues()

	key, err := sign.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("parsing private key: %w", err)
	}

	server, err := server.New(ctx, config.DBFile, config.DID, key)
	if err != nil {
		return fmt.Errorf("instantiating a server: %w", err)
	}
	server.SetAllowedLabels(config.LabelValues())

	if config.Password == "" {
		return fmt.Errorf("no password provided in the config file")
	}

	client := xrpcauth.NewClientWithTokenSource(ctx, xrpcauth.PasswordAuth(config.DID, config.Password))

	if config.Password != "" && len(config.Labels.LabelValueDefinitions) > 0 {
		err := account.UpdateLabelDefs(ctx, client, &config.Labels)
		if err != nil {
			return fmt.Errorf("updating label definitions: %w", err)
		}
	}

	startListUpdates(ctx, client, config, server, *updateInterval)

	mux := http.NewServeMux()
	mux.Handle("/xrpc/com.atproto.label.subscribeLabels", server.Subscribe())
	mux.Handle("/xrpc/com.atproto.label.queryLabels", server.Query())

	log.Info().Msgf("Starting HTTP listener...")
	return http.ListenAndServe(*listenAddr, mux)
}

func main() {
	flag.Parse()

	ctx := logging.Setup(context.Background(), *logFile, *logFormat, zerolog.Level(*logLevel))
	log := zerolog.Ctx(ctx)

	if err := runMain(ctx); err != nil {
		log.Fatal().Err(err).Msgf("%s", err)
	}
}

func startListUpdates(ctx context.Context, client *xrpc.Client, config *Config, server *server.Server, updateInterval time.Duration) {
	log := zerolog.Ctx(ctx)

	if err := updateOnce(ctx, client, config, server); err != nil {
		log.Error().Err(err).Msgf("Update failed: %s", err)
	}
	ticker := time.NewTicker(updateInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("context cancelled, exiting")
			return
		case <-ticker.C:
			if err := updateOnce(ctx, client, config, server); err != nil {
				log.Error().Err(err).Msgf("Update failed: %s", err)
			}
		}
	}
}

func updateOnce(ctx context.Context, client *xrpc.Client, config *Config, server *server.Server) error {
	log := zerolog.Ctx(ctx)

	for label, list := range config.Lists {
		if err := updateFromList(ctx, client, server, label, list); err != nil {
			log.Error().Err(err).Str("label", label).Msgf("Failed to update label entries: %s", err)
		}
	}
	return nil
}

func updateFromList(ctx context.Context, client *xrpc.Client, server *server.Server, label string, listUri string) error {
	log := zerolog.Ctx(ctx).With().Str("label", label).Logger()
	ctx = log.WithContext(ctx)

	entries, err := server.LabelEntries(ctx, label)
	if err != nil {
		return fmt.Errorf("getting existing label entries: %w", err)
	}
	labeledDids := didset.StringSet{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Uri, "did:") {
			continue
		}
		labeledDids[entry.Uri] = true
	}
	log.Debug().Msgf("Currently labeled accounts: %d", len(labeledDids))

	// Note: This uses `app.bsky.graph.getList` method, which filters out accounts that have blocked you.
	list, err := didset.MuteList(client, listUri).GetDIDs(ctx)
	if err != nil {
		return fmt.Errorf("getting list content: %w", err)
	}
	log.Debug().Msgf("Number of list members: %d", len(list))

	toAdd, _ := didset.Difference(list, labeledDids).GetDIDs(ctx)
	toRemove, _ := didset.Difference(labeledDids, list).GetDIDs(ctx)
	if len(toAdd)+len(toRemove) == 0 {
		return nil
	}
	log.Debug().Msgf("Adding %d and removing %d labels", len(toAdd), len(toRemove))

	for did := range toAdd {
		_, err := server.AddLabel(atproto.LabelDefs_Label{
			Uri: did,
			Val: label,
		})
		if err != nil {
			return err
		}
		log.Debug().Msgf("Added %s", did)
	}
	for did := range toRemove {
		neg := true
		_, err := server.AddLabel(atproto.LabelDefs_Label{
			Uri: did,
			Val: label,
			Neg: &neg,
		})
		if err != nil {
			return err
		}
		log.Debug().Msgf("Removed %s", did)
	}
	return nil
}
