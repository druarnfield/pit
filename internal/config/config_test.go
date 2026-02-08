package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDuration_UnmarshalText(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{name: "minutes", input: "5m", want: 5 * time.Minute},
		{name: "hours", input: "1h", want: time.Hour},
		{name: "seconds", input: "30s", want: 30 * time.Second},
		{name: "compound", input: "1h30m", want: 90 * time.Minute},
		{name: "invalid", input: "nope", wantErr: true},
		{name: "empty", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Duration
			err := d.UnmarshalText([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Errorf("UnmarshalText(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalText(%q) unexpected error: %v", tt.input, err)
			}
			if d.Duration != tt.want {
				t.Errorf("UnmarshalText(%q) = %v, want %v", tt.input, d.Duration, tt.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Run("valid minimal", func(t *testing.T) {
		cfg, err := Load(filepath.Join("testdata", "valid_minimal.toml"))
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.DAG.Name != "minimal" {
			t.Errorf("DAG.Name = %q, want %q", cfg.DAG.Name, "minimal")
		}
		if len(cfg.Tasks) != 1 {
			t.Fatalf("len(Tasks) = %d, want 1", len(cfg.Tasks))
		}
		if cfg.Tasks[0].Name != "hello" {
			t.Errorf("Tasks[0].Name = %q, want %q", cfg.Tasks[0].Name, "hello")
		}
		if cfg.Tasks[0].Script != "tasks/hello.sh" {
			t.Errorf("Tasks[0].Script = %q, want %q", cfg.Tasks[0].Script, "tasks/hello.sh")
		}
	})

	t.Run("valid full", func(t *testing.T) {
		cfg, err := Load(filepath.Join("testdata", "valid_full.toml"))
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.DAG.Name != "full_example" {
			t.Errorf("DAG.Name = %q, want %q", cfg.DAG.Name, "full_example")
		}
		if cfg.DAG.Overlap != "skip" {
			t.Errorf("DAG.Overlap = %q, want %q", cfg.DAG.Overlap, "skip")
		}
		if cfg.DAG.Timeout.Duration != time.Hour {
			t.Errorf("DAG.Timeout = %v, want %v", cfg.DAG.Timeout.Duration, time.Hour)
		}
		if cfg.DAG.SQL.Connection != "my_database" {
			t.Errorf("DAG.SQL.Connection = %q, want %q", cfg.DAG.SQL.Connection, "my_database")
		}
		if len(cfg.Tasks) != 3 {
			t.Fatalf("len(Tasks) = %d, want 3", len(cfg.Tasks))
		}

		// Check task with retries
		extract := cfg.Tasks[0]
		if extract.Retries != 2 {
			t.Errorf("extract.Retries = %d, want 2", extract.Retries)
		}
		if extract.RetryDelay.Duration != 30*time.Second {
			t.Errorf("extract.RetryDelay = %v, want 30s", extract.RetryDelay.Duration)
		}

		// Check depends_on
		transform := cfg.Tasks[1]
		if len(transform.DependsOn) != 1 || transform.DependsOn[0] != "extract" {
			t.Errorf("transform.DependsOn = %v, want [extract]", transform.DependsOn)
		}

		// Check outputs
		if len(cfg.Outputs) != 1 {
			t.Fatalf("len(Outputs) = %d, want 1", len(cfg.Outputs))
		}
		if cfg.Outputs[0].Type != "table" {
			t.Errorf("Outputs[0].Type = %q, want %q", cfg.Outputs[0].Type, "table")
		}
	})

	t.Run("custom runner", func(t *testing.T) {
		cfg, err := Load(filepath.Join("testdata", "valid_custom_runner.toml"))
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.Tasks[0].Runner != "$ node" {
			t.Errorf("Tasks[0].Runner = %q, want %q", cfg.Tasks[0].Runner, "$ node")
		}
	})

	t.Run("invalid syntax", func(t *testing.T) {
		_, err := Load(filepath.Join("testdata", "invalid_syntax.toml"))
		if err == nil {
			t.Error("Load() expected error for invalid TOML, got nil")
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := Load(filepath.Join("testdata", "does_not_exist.toml"))
		if err == nil {
			t.Error("Load() expected error for missing file, got nil")
		}
	})
}

func TestLoad_PathAndDir(t *testing.T) {
	cfg, err := Load(filepath.Join("testdata", "valid_minimal.toml"))
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	path := cfg.Path()
	if !filepath.IsAbs(path) {
		t.Errorf("Path() = %q, want absolute path", path)
	}

	dir := cfg.Dir()
	if filepath.Base(dir) != "testdata" {
		t.Errorf("Dir() base = %q, want %q", filepath.Base(dir), "testdata")
	}
}

func TestDiscover(t *testing.T) {
	root := t.TempDir()

	mkTestProject(t, filepath.Join(root, "projects", "alpha"), `[dag]
name = "alpha"

[[tasks]]
name = "hello"
script = "tasks/hello.sh"
`)
	mkTestProject(t, filepath.Join(root, "projects", "beta"), `[dag]
name = "beta"

[[tasks]]
name = "greet"
script = "tasks/hello.sh"
`)

	configs, err := Discover(root)
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("len(configs) = %d, want 2", len(configs))
	}
	if _, ok := configs["alpha"]; !ok {
		t.Error("Discover() missing 'alpha' config")
	}
	if _, ok := configs["beta"]; !ok {
		t.Error("Discover() missing 'beta' config")
	}
}

func TestDiscover_DuplicateName(t *testing.T) {
	root := t.TempDir()

	mkTestProject(t, filepath.Join(root, "projects", "first"), `[dag]
name = "same"

[[tasks]]
name = "a"
script = "tasks/a.sh"
`)
	mkTestProject(t, filepath.Join(root, "projects", "second"), `[dag]
name = "same"

[[tasks]]
name = "b"
script = "tasks/b.sh"
`)

	_, err := Discover(root)
	if err == nil {
		t.Error("Discover() expected error for duplicate DAG name, got nil")
	}
}

func TestDiscover_NoProjects(t *testing.T) {
	root := t.TempDir()
	configs, err := Discover(root)
	if err != nil {
		t.Fatalf("Discover() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("len(configs) = %d, want 0", len(configs))
	}
}

// mkTestProject creates a minimal project directory with a pit.toml.
func mkTestProject(t *testing.T, dir, tomlContent string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating project dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pit.toml"), []byte(tomlContent), 0o644); err != nil {
		t.Fatalf("writing pit.toml: %v", err)
	}
}
