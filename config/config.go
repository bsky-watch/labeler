package config

import "github.com/bluesky-social/indigo/api/bsky"

type Config struct {
	DBFile     string                           `yaml:"db_file"`
	DID        string                           `yaml:"did"`
	PrivateKey string                           `yaml:"private_key"`
	Password   string                           `yaml:"password"`
	Endpoint   string                           `yaml:"endpoint"`
	Labels     bsky.LabelerDefs_LabelerPolicies `yaml:"labels"`
}
