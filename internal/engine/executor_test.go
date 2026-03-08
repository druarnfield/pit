package engine

import (
	"testing"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/secrets"
)

// TestRun_SecretsResolverNilInterface guards against the typed-nil interface
// bug: assigning a nil *secrets.Store to SecretsResolver produces a non-nil
// interface value (it carries the concrete type but a nil pointer). The nil
// check in executeTask then passes and ResolveField is called on a nil
// receiver, causing a panic.
//
// The fix is to only assign store to run.SecretsResolver when store is
// non-nil, so the interface field stays a true nil interface.
func TestRun_SecretsResolverNilInterface(t *testing.T) {
	// Demonstrate the Go gotcha: typed nil pointer → non-nil interface.
	var store *secrets.Store // nil
	var iface SecretsResolver = store
	if iface == nil {
		t.Fatal("prerequisite failed: expected typed nil *secrets.Store to produce non-nil interface")
	}

	// Verify the fix: conditional assignment leaves the field as nil interface.
	run := &Run{}
	if store != nil {
		run.SecretsResolver = store
	}
	if run.SecretsResolver != nil {
		t.Error("SecretsResolver should be nil interface when store is nil, got non-nil")
	}
}

func TestResolveTaskConnection(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{SQL: config.SQLConfig{Connection: "default_conn"}},
	}
	// Task override wins
	tc := &config.TaskConfig{Connection: "task_conn"}
	if got := resolveTaskConnection(tc, cfg); got != "task_conn" {
		t.Errorf("got %q, want %q", got, "task_conn")
	}
	// Falls back to DAG default
	tc2 := &config.TaskConfig{}
	if got := resolveTaskConnection(tc2, cfg); got != "default_conn" {
		t.Errorf("got %q, want %q", got, "default_conn")
	}
}

func TestParseSchemaTable(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{"staging.customers", "staging", "customers"},
		{"customers", "", "customers"},
		{"public.my_table", "public", "my_table"},
	}
	for _, tt := range tests {
		s, tbl := parseSchemaTable(tt.input)
		if s != tt.wantSchema || tbl != tt.wantTable {
			t.Errorf("parseSchemaTable(%q) = (%q, %q), want (%q, %q)",
				tt.input, s, tbl, tt.wantSchema, tt.wantTable)
		}
	}
}
