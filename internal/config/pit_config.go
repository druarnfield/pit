package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// PitConfig holds workspace-level settings from pit_config.toml.
type PitConfig struct {
	SecretsDir string `toml:"secrets_dir"`
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

	// Make relative secrets_dir absolute relative to rootDir
	if cfg.SecretsDir != "" && !filepath.IsAbs(cfg.SecretsDir) {
		cfg.SecretsDir = filepath.Join(rootDir, cfg.SecretsDir)
	}

	return &cfg, nil
}
