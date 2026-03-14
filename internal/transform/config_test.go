package transform

import (
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

func configKeys(m map[string]*ModelConfig) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
