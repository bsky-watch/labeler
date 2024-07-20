package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"bsky.watch/labeler/account"
	"bsky.watch/labeler/config"
	"bsky.watch/labeler/sign"
	"bsky.watch/utils/xrpcauth"
)

var (
	configFile = flag.String("config", "config.yaml", "Path to the config file")
	token      = flag.String("token", "", "Token that PDS requires to sign PLC operations")
)

func runMain(ctx context.Context) error {
	b, err := os.ReadFile(*configFile)
	if err != nil {
		return fmt.Errorf("reading config file: %w", err)
	}

	config := &config.Config{}
	if err := yaml.Unmarshal(b, config); err != nil {
		return fmt.Errorf("parsing config file: %w", err)
	}

	if config.Password == "" {
		return fmt.Errorf("password is not specified in the config")
	}

	key, err := sign.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("parsing private key: %w", err)
	}
	publicKey, err := sign.GetPublicKey(key)
	if err != nil {
		return fmt.Errorf("failed to get the public key: %w", err)
	}

	client := xrpcauth.NewClientWithTokenSource(ctx, xrpcauth.PasswordAuth(config.DID, config.Password))

	err = account.UpdateSigningKeyAndEndpoint(ctx, client, *token, publicKey, config.Endpoint)
	if err != nil {
		if *token == "" {
			fmt.Fprintln(os.Stderr, "If you need to provide a token, re-run this command with --token=YOUR-TOKEN flag")
		}
		return err
	}

	return nil
}

func main() {
	flag.Parse()

	if err := runMain(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
