package config

import "github.com/bluesky-social/indigo/api/bsky"

type Config struct {
	DBFile      string                           `yaml:"db_file"`
	SQLiteDB    string                           `yaml:"sqlite_db"`
	PostgresURL string                           `yaml:"postgres_url"`
	DID         string                           `yaml:"did"`
	PrivateKey  string                           `yaml:"private_key"`
	Password    string                           `yaml:"password"`
	Endpoint    string                           `yaml:"endpoint"`
	Labels      bsky.LabelerDefs_LabelerPolicies `yaml:"labels"`
}

// UpdateLabelValues ensures that all labels defined in c.Labels.LabelValueDefinitions
// are also listed in c.Labels.LabelValues.
func (c *Config) UpdateLabelValues() {
	labels := map[string]bool{}
	for _, l := range c.Labels.LabelValues {
		labels[*l] = true
	}
	for _, def := range c.Labels.LabelValueDefinitions {
		if labels[def.Identifier] {
			continue
		}
		c.Labels.LabelValues = append(c.Labels.LabelValues, &def.Identifier)
	}
}

// LabelValues returns the list of labels specified in the config.
func (c *Config) LabelValues() []string {
	r := []string{}
	for _, l := range c.Labels.LabelValues {
		if l == nil {
			continue
		}
		r = append(r, *l)
	}
	return r
}
