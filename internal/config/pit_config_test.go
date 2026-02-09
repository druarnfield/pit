package config

import (
	"os"
	"path/filepath"
	"strings"
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

	t.Run("full config", func(t *testing.T) {
		dir := t.TempDir()
		content := `
secrets_dir = "secrets/secrets.toml"
runs_dir = "output/runs"
dbt_driver = "ODBC Driver 17 for SQL Server"
keep_artifacts = ["logs", "data"]
`
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}

		wantRuns := filepath.Join(dir, "output", "runs")
		if cfg.RunsDir != wantRuns {
			t.Errorf("RunsDir = %q, want %q", cfg.RunsDir, wantRuns)
		}
		if cfg.DBTDriver != "ODBC Driver 17 for SQL Server" {
			t.Errorf("DBTDriver = %q, want %q", cfg.DBTDriver, "ODBC Driver 17 for SQL Server")
		}
		if len(cfg.KeepArtifacts) != 2 {
			t.Fatalf("len(KeepArtifacts) = %d, want 2", len(cfg.KeepArtifacts))
		}
		if cfg.KeepArtifacts[0] != "logs" || cfg.KeepArtifacts[1] != "data" {
			t.Errorf("KeepArtifacts = %v, want [logs data]", cfg.KeepArtifacts)
		}
	})

	t.Run("runs_dir absolute unchanged", func(t *testing.T) {
		dir := t.TempDir()
		content := `runs_dir = "/var/pit/runs"` + "\n"
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}
		if cfg.RunsDir != "/var/pit/runs" {
			t.Errorf("RunsDir = %q, want %q", cfg.RunsDir, "/var/pit/runs")
		}
	})

	t.Run("invalid keep_artifacts", func(t *testing.T) {
		dir := t.TempDir()
		content := `keep_artifacts = ["logs", "snapshots"]` + "\n"
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		_, err := LoadPitConfig(dir)
		if err == nil {
			t.Fatal("LoadPitConfig() expected error for invalid keep_artifacts, got nil")
		}
		if !strings.Contains(err.Error(), "snapshots") {
			t.Errorf("error = %q, want it to mention invalid value", err)
		}
	})

	t.Run("empty keep_artifacts", func(t *testing.T) {
		dir := t.TempDir()
		content := `keep_artifacts = []` + "\n"
		if err := os.WriteFile(filepath.Join(dir, "pit_config.toml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadPitConfig(dir)
		if err != nil {
			t.Fatalf("LoadPitConfig() error: %v", err)
		}
		if len(cfg.KeepArtifacts) != 0 {
			t.Errorf("KeepArtifacts = %v, want empty", cfg.KeepArtifacts)
		}
	})
}
