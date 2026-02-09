package scaffold

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidType(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"python", true},
		{"sql", true},
		{"shell", true},
		{"dbt", true},
		{"ruby", false},
		{"", false},
		{"Python", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ValidType(tt.input)
			if got != tt.want {
				t.Errorf("ValidType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestCreate_Shell(t *testing.T) {
	root := t.TempDir()

	if err := Create(root, "my_dag", TypeShell); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	// Check expected files exist
	wantFiles := []string{
		"projects/my_dag/pit.toml",
		"projects/my_dag/tasks/hello.sh",
	}
	for _, f := range wantFiles {
		path := filepath.Join(root, f)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("missing expected file: %s", f)
		}
	}
}

func TestCreate_Python(t *testing.T) {
	root := t.TempDir()

	if err := Create(root, "py_dag", TypePython); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	wantFiles := []string{
		"projects/py_dag/pit.toml",
		"projects/py_dag/pyproject.toml",
		"projects/py_dag/src/py_dag/__init__.py",
		"projects/py_dag/tasks/hello.py",
	}
	for _, f := range wantFiles {
		path := filepath.Join(root, f)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("missing expected file: %s", f)
		}
	}
}

func TestCreate_SQL(t *testing.T) {
	root := t.TempDir()

	if err := Create(root, "sql_dag", TypeSQL); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	wantFiles := []string{
		"projects/sql_dag/pit.toml",
		"projects/sql_dag/tasks/example.sql",
	}
	for _, f := range wantFiles {
		path := filepath.Join(root, f)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("missing expected file: %s", f)
		}
	}
}

func TestCreate_DBT(t *testing.T) {
	root := t.TempDir()

	if err := Create(root, "dbt_dag", TypeDBT); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	wantFiles := []string{
		"projects/dbt_dag/pit.toml",
		"projects/dbt_dag/dbt_repo/dbt_project.yml",
	}
	for _, f := range wantFiles {
		path := filepath.Join(root, f)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("missing expected file: %s", f)
		}
	}
}

func TestCreate_InvalidName(t *testing.T) {
	tests := []string{
		"",
		"1starts_with_number",
		"has-hyphens",
		"HasCaps",
		"has spaces",
		"has.dots",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			err := Create(root, name, TypeShell)
			if err == nil {
				t.Errorf("Create(%q) expected error for invalid name, got nil", name)
			}
		})
	}
}

func TestCreate_ValidNames(t *testing.T) {
	tests := []string{
		"a",
		"my_dag",
		"dag123",
		"a1_b2_c3",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			err := Create(root, name, TypeShell)
			if err != nil {
				t.Errorf("Create(%q) unexpected error: %v", name, err)
			}
		})
	}
}

func TestCreate_AlreadyExists(t *testing.T) {
	root := t.TempDir()

	// Create once
	if err := Create(root, "existing", TypeShell); err != nil {
		t.Fatalf("first Create() error: %v", err)
	}

	// Creating again should fail
	err := Create(root, "existing", TypeShell)
	if err == nil {
		t.Error("second Create() expected error for existing project, got nil")
	}
}
