package transform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseModelConfigs_Simple(t *testing.T) {
	configs, err := ParseModelConfigs("testdata/simple")
	if err != nil {
		t.Fatalf("ParseModelConfigs() error: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("got %d configs, want 1", len(configs))
	}
	cfg, ok := configs["stg_orders"]
	if !ok {
		t.Fatalf("missing config for stg_orders, got keys: %v", configKeys(configs))
	}
	if cfg.Materialization != "view" {
		t.Errorf("materialization = %q, want %q", cfg.Materialization, "view")
	}
	if cfg.Schema != "staging" {
		t.Errorf("schema = %q, want %q", cfg.Schema, "staging")
	}
}

func TestParseModelConfigs_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	configs, err := ParseModelConfigs(dir)
	if err != nil {
		t.Fatalf("ParseModelConfigs() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("got %d configs, want 0", len(configs))
	}
}

func TestParseModelConfigs_NestedDefaults(t *testing.T) {
	configs, err := ParseModelConfigs("testdata/nested")
	if err != nil {
		t.Fatalf("ParseModelConfigs() error: %v", err)
	}

	tests := []struct {
		name         string
		wantMat      string
		wantSchema   string
		wantStrategy string
	}{
		// staging/stg_orders: root defaults (view) + staging override (schema=staging)
		{"stg_orders", "view", "staging", ""},
		// marts/dim_customers: per-model override (view) + marts defaults (schema=analytics)
		{"dim_customers", "view", "analytics", ""},
		// marts/incremental/fact_daily: incremental defaults override marts table default
		{"fact_daily", "incremental", "analytics", "merge"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, ok := configs[tt.name]
			if !ok {
				t.Fatalf("missing config for %s", tt.name)
			}
			if cfg.Materialization != tt.wantMat {
				t.Errorf("materialization = %q, want %q", cfg.Materialization, tt.wantMat)
			}
			if cfg.Schema != tt.wantSchema {
				t.Errorf("schema = %q, want %q", cfg.Schema, tt.wantSchema)
			}
			if cfg.Strategy != tt.wantStrategy {
				t.Errorf("strategy = %q, want %q", cfg.Strategy, tt.wantStrategy)
			}
		})
	}
}

func TestParseModelConfigs_DuplicateModelName(t *testing.T) {
	dir := t.TempDir()
	// Create two .sql files with the same name in different subdirs
	os.MkdirAll(filepath.Join(dir, "a"), 0o755)
	os.MkdirAll(filepath.Join(dir, "b"), 0o755)
	os.WriteFile(filepath.Join(dir, "a", "dup.sql"), []byte("SELECT 1"), 0o644)
	os.WriteFile(filepath.Join(dir, "b", "dup.sql"), []byte("SELECT 2"), 0o644)

	_, err := ParseModelConfigs(dir)
	if err == nil {
		t.Errorf("expected error for duplicate model name, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate model name") {
		t.Errorf("error = %q, want it to contain %q", err, "duplicate model name")
	}
}

func configKeys(m map[string]*ModelConfig) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
