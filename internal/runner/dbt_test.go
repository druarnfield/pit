package runner

import (
	"strings"
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func TestDBTRunner_BuildArgs(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *config.DBTConfig
		dbtCommand string
		wantArgs   []string
	}{
		{
			name: "basic run",
			cfg: &config.DBTConfig{
				Version: "1.9.1",
				Adapter: "dbt-sqlserver",
			},
			dbtCommand: "run",
			wantArgs: []string{
				"--from", "dbt-core==1.9.1",
				"--with", "dbt-sqlserver",
				"dbt", "run",
				"--log-format", "json",
			},
		},
		{
			name: "run with select",
			cfg: &config.DBTConfig{
				Version: "1.9.1",
				Adapter: "dbt-sqlserver",
			},
			dbtCommand: "run --select staging",
			wantArgs: []string{
				"--from", "dbt-core==1.9.1",
				"--with", "dbt-sqlserver",
				"dbt", "run", "--select", "staging",
				"--log-format", "json",
			},
		},
		{
			name: "with extra deps",
			cfg: &config.DBTConfig{
				Version:   "1.9.1",
				Adapter:   "dbt-sqlserver",
				ExtraDeps: []string{"dbt-utils", "dbt-expectations"},
			},
			dbtCommand: "test",
			wantArgs: []string{
				"--from", "dbt-core==1.9.1",
				"--with", "dbt-sqlserver",
				"--with", "dbt-utils",
				"--with", "dbt-expectations",
				"dbt", "test",
				"--log-format", "json",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewDBTRunner(tt.cfg, "/tmp/profiles")
			got := r.BuildArgs(tt.dbtCommand)
			if len(got) != len(tt.wantArgs) {
				t.Fatalf("BuildArgs() returned %d args, want %d\n  got:  %v\n  want: %v",
					len(got), len(tt.wantArgs), got, tt.wantArgs)
			}
			for i := range got {
				if got[i] != tt.wantArgs[i] {
					t.Errorf("BuildArgs()[%d] = %q, want %q", i, got[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestDBTRunner_InvalidConfig(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *config.DBTConfig
		errContain string
	}{
		{
			name:       "nil config",
			cfg:        nil,
			errContain: "config is nil",
		},
		{
			name:       "missing version",
			cfg:        &config.DBTConfig{Adapter: "dbt-sqlserver"},
			errContain: "version is required",
		},
		{
			name:       "missing adapter",
			cfg:        &config.DBTConfig{Version: "1.9.1"},
			errContain: "adapter is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &DBTRunner{Config: tt.cfg, ProfilesDir: "/tmp"}
			rc := RunContext{ScriptPath: "run", SnapshotDir: "/tmp"}
			err := r.Run(t.Context(), rc, nil)
			if err == nil {
				t.Fatal("Run() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContain) {
				t.Errorf("error = %q, want it to contain %q", err, tt.errContain)
			}
		})
	}
}

func TestResolve_DBT(t *testing.T) {
	_, err := Resolve("dbt", "run --select staging")
	if err == nil {
		t.Fatal("Resolve('dbt', ...) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "executor") {
		t.Errorf("error = %q, want it to mention executor", err)
	}
}
