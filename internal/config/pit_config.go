package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// ValidArtifacts is the set of valid keep_artifacts values.
var ValidArtifacts = map[string]bool{
	"logs":    true,
	"project": true,
	"data":    true,
}

// DefaultKeepArtifacts is the default set — keep everything.
var DefaultKeepArtifacts = []string{"logs", "project", "data"}

// DefaultDBTDriver is the default ODBC driver for dbt profiles.
const DefaultDBTDriver = "ODBC Driver 17 for SQL Server"

// PitConfig holds workspace-level settings from pit_config.toml.
type PitConfig struct {
	SecretsDir    string   `toml:"secrets_dir"`
	RunsDir       string   `toml:"runs_dir"`
	DBTDriver     string   `toml:"dbt_driver"`
	KeepArtifacts []string `toml:"keep_artifacts"`
}

// LoadPitConfig loads pit_config.toml from rootDir.
// Returns nil, nil if the file doesn't exist (config is optional).
func LoadPitConfig(rootDir string) (*PitConfig, error) {
	path := filepath.Join(rootDir, "pit_config.toml")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading %q: %w", path, err)
	}

	var cfg PitConfig
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %q: %w", path, err)
	}

	// Make relative paths absolute relative to rootDir
	if cfg.SecretsDir != "" && !filepath.IsAbs(cfg.SecretsDir) {
		cfg.SecretsDir = filepath.Join(rootDir, cfg.SecretsDir)
	}
	if cfg.RunsDir != "" && !filepath.IsAbs(cfg.RunsDir) {
		cfg.RunsDir = filepath.Join(rootDir, cfg.RunsDir)
	}

	// Validate keep_artifacts entries
	for _, a := range cfg.KeepArtifacts {
		if !ValidArtifacts[a] {
			return nil, fmt.Errorf("invalid keep_artifacts value %q (must be logs, project, or data)", a)
		}
	}

	return &cfg, nil
}
