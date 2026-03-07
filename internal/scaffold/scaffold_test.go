package scaffold

import (
	"os"
	"path/filepath"
	"strings"
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

func TestCreateWorkspace_Shell(t *testing.T) {
	parent := t.TempDir()
	name := "my_workspace"

	if err := CreateWorkspace(parent, name, TypeShell); err != nil {
		t.Fatalf("CreateWorkspace() error: %v", err)
	}

	wsDir := filepath.Join(parent, name)

	wantFiles := []string{
		".gitignore",
		"pit_config.toml",
		"README.md",
		"projects/sample_pipeline/pit.toml",
		"projects/sample_pipeline/tasks/hello.sh",
	}
	for _, f := range wantFiles {
		path := filepath.Join(wsDir, f)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("missing expected file: %s", f)
		}
	}

	gitDir := filepath.Join(wsDir, ".git")
	info, err := os.Stat(gitDir)
	if err != nil {
		t.Errorf("missing .git directory: %v", err)
	} else if !info.IsDir() {
		t.Errorf(".git is not a directory")
	}

	content, err := os.ReadFile(filepath.Join(wsDir, ".gitignore"))
	if err != nil {
		t.Fatalf("reading .gitignore: %v", err)
	}
	for _, entry := range []string{"runs/", ".venv/", "repo_cache/", "*.db", "secrets/"} {
		if !strings.Contains(string(content), entry) {
			t.Errorf(".gitignore missing %q", entry)
		}
	}
}

func TestCreateWorkspace_Python(t *testing.T) {
	parent := t.TempDir()

	if err := CreateWorkspace(parent, "ws_python", TypePython); err != nil {
		t.Fatalf("CreateWorkspace() error: %v", err)
	}

	wsDir := filepath.Join(parent, "ws_python")
	wantFiles := []string{
		"pit_config.toml",
		"README.md",
		".gitignore",
		"projects/sample_pipeline/pit.toml",
		"projects/sample_pipeline/pyproject.toml",
		"projects/sample_pipeline/tasks/hello.py",
	}
	for _, f := range wantFiles {
		if _, err := os.Stat(filepath.Join(wsDir, f)); err != nil {
			t.Errorf("missing expected file: %s", f)
		}
	}
}

func TestCreateWorkspace_AlreadyExists(t *testing.T) {
	parent := t.TempDir()
	name := "existing"

	os.MkdirAll(filepath.Join(parent, name), 0o755)

	err := CreateWorkspace(parent, name, TypeShell)
	if err == nil {
		t.Error("CreateWorkspace() expected error for existing directory, got nil")
	}
}

func TestCreateWorkspace_InvalidName(t *testing.T) {
	parent := t.TempDir()

	err := CreateWorkspace(parent, "Bad-Name", TypeShell)
	if err == nil {
		t.Error("CreateWorkspace() expected error for invalid name, got nil")
	}
}
