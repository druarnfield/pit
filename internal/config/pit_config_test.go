package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPitConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		dir := t.TempDir()
		content := `secrets_dir = "secrets/secrets.toml"` + "\n"
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}
		if cfg == nil {
			t.Fatal("LoadPitConfig() returned nil, want config")
		}

		// Should be made absolute
		want := filepath.Join(dir, "secrets", "secrets.toml")
		if cfg.SecretsDir != want {
			t.Errorf("SecretsDir = %q, want %q", cfg.SecretsDir, want)
		}
	})

	t.Run("absolute secrets_dir unchanged", func(t *testing.T) {
		dir := t.TempDir()
		content := `secrets_dir = "/etc/pit/secrets.toml"` + "\n"
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}
		if cfg.SecretsDir != "/etc/pit/secrets.toml" {
			t.Errorf("SecretsDir = %q, want %q", cfg.SecretsDir, "/etc/pit/secrets.toml")
		}
	})

	t.Run("missing file returns nil nil", func(t *testing.T) {
		dir := t.TempDir()
		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}
		if cfg != nil {
			t.Errorf("LoadPitConfig() = %+v, want nil for missing file", cfg)
		}
	})

	t.Run("invalid TOML", func(t *testing.T) {
		dir := t.TempDir()
		content := `this is not valid TOML [[[`
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := LoadPitConfig(dir)
		if err == nil {
			t.Error("LoadPitConfig() expected error for invalid TOML, got nil")
		}
	})

	t.Run("empty secrets_dir", func(t *testing.T) {
		dir := t.TempDir()
		content := `secrets_dir = ""` + "\n"
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}
		if cfg.SecretsDir != "" {
			t.Errorf("SecretsDir = %q, want empty", cfg.SecretsDir)
		}
	})
}
