# Workspace Scaffold Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `pit new <name>` to create a ready-to-run workspace with config, gitignore, README, and a sample project.

**Architecture:** Extend `internal/scaffold` with `CreateWorkspace()` that writes workspace files and delegates to existing `Create()` for the sample project. New CLI command in `internal/cli/new.go`.

**Tech Stack:** Go stdlib, `os/exec` for `git init`

---

### Task 1: Add workspace file generators to scaffold

**Files:**
- Modify: `internal/scaffold/scaffold.go`

**Step 1: Write the failing test**

Add to `internal/scaffold/scaffold_test.go`:

```go
func TestCreateWorkspace_Shell(t *testing.T) {
	parent := t.TempDir()
	name := "my_workspace"

	if err := CreateWorkspace(parent, name, TypeShell); err != nil {
		t.Fatalf("CreateWorkspace() error: %v", err)
	}

	wsDir := filepath.Join(parent, name)

	// Check workspace files
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

	// Check .git/ directory exists
	gitDir := filepath.Join(wsDir, ".git")
	info, err := os.Stat(gitDir)
	if err != nil {
		t.Errorf("missing .git directory: %v", err)
	} else if !info.IsDir() {
		t.Errorf(".git is not a directory")
	}

	// Check .gitignore contains expected entries
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
```

Note: add `"strings"` to the test file imports.

**Step 2: Run test to verify it fails**

Run: `go test ./internal/scaffold/ -run TestCreateWorkspace -v`
Expected: FAIL — `CreateWorkspace` not defined

**Step 3: Implement CreateWorkspace**

Add to `internal/scaffold/scaffold.go`:

```go
// CreateWorkspace creates a new workspace directory with config, gitignore,
// README, and a sample project.
func CreateWorkspace(parentDir, name string, projectType ProjectType) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("invalid workspace name %q: must match [a-z][a-z0-9_]*", name)
	}

	wsDir := filepath.Join(parentDir, name)
	if _, err := os.Stat(wsDir); err == nil {
		return fmt.Errorf("directory already exists: %s", wsDir)
	}

	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		return fmt.Errorf("creating workspace directory: %w", err)
	}

	// Write workspace files
	files := map[string]string{
		filepath.Join(wsDir, ".gitignore"):     workspaceGitignore(),
		filepath.Join(wsDir, "pit_config.toml"): workspacePitConfig(),
		filepath.Join(wsDir, "README.md"):       workspaceReadme(name),
	}
	if err := writeFiles(files); err != nil {
		return err
	}

	// Create sample project
	if err := Create(wsDir, "sample_pipeline", projectType); err != nil {
		return fmt.Errorf("creating sample project: %w", err)
	}

	// Initialize git repo
	if err := gitInit(wsDir); err != nil {
		return fmt.Errorf("git init: %w", err)
	}

	return nil
}
```

Add the `os/exec` import and helper functions:

```go
func gitInit(dir string) error {
	cmd := exec.Command("git", "init")
	cmd.Dir = dir
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func workspaceGitignore() string {
	return `runs/
.venv/
repo_cache/
*.db
secrets/
`
}

func workspacePitConfig() string {
	return `# Pit workspace configuration
# See: https://github.com/druarnfield/pit

# runs_dir = "runs"
# repo_cache_dir = "repo_cache"
# metadata_db = "pit_metadata.db"
# secrets_dir = "secrets/secrets.toml"
# api_token = ""
# dbt_driver = "ODBC Driver 17 for SQL Server"
# keep_artifacts = ["logs", "project", "data"]
`
}

func workspaceReadme(name string) string {
	return fmt.Sprintf(`# %s

Pit workspace. See [Pit documentation](https://github.com/druarnfield/pit) for full details.

## Layout

`+"```"+`
%s/
├── pit_config.toml          # workspace configuration
└── projects/                # DAG projects
    └── sample_pipeline/     # sample project (pit run sample_pipeline)
        ├── pit.toml         # DAG definition
        └── tasks/           # task scripts
`+"```"+`

## Commands

`+"```"+`bash
pit validate                    # check all project configs
pit run sample_pipeline         # run the sample DAG
pit run sample_pipeline/hello   # run a single task
pit init my_new_project         # add another project
pit serve                       # start the scheduler
pit status                      # view latest run status
`+"```"+`
`, name, name)
}
```

Add `"io"` and `"os/exec"` to the imports in scaffold.go.

**Step 4: Run tests**

Run: `go test -race ./internal/scaffold/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/scaffold/scaffold.go internal/scaffold/scaffold_test.go
git commit -m "Add CreateWorkspace to scaffold package"
```

---

### Task 2: Add pit new CLI command

**Files:**
- Create: `internal/cli/new.go`
- Modify: `internal/cli/root.go`

**Step 1: Create new.go**

Create `internal/cli/new.go`:

```go
package cli

import (
	"fmt"

	"github.com/druarnfield/pit/internal/scaffold"
	"github.com/spf13/cobra"
)

func newNewCmd() *cobra.Command {
	var projectType string

	cmd := &cobra.Command{
		Use:   "new <name>",
		Short: "Create a new Pit workspace",
		Long:  "Create a new workspace directory with configuration, a sample project, and git repository.\nUse --type to choose the sample project type: python (default), sql, shell, or dbt.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			pt := scaffold.ProjectType(projectType)

			if !scaffold.ValidType(projectType) {
				return fmt.Errorf("unknown project type %q (must be python, sql, shell, or dbt)", projectType)
			}

			if err := scaffold.CreateWorkspace(".", name, pt); err != nil {
				return err
			}

			fmt.Printf("Created workspace %q with a %s sample project.\n\n", name, projectType)
			fmt.Println("Next steps:")
			fmt.Printf("  cd %s\n", name)
			fmt.Println("  pit validate")
			fmt.Println("  pit run sample_pipeline")
			return nil
		},
	}

	cmd.Flags().StringVar(&projectType, "type", "python", "sample project type: python, sql, shell, or dbt")

	return cmd
}
```

**Step 2: Register the command in root.go**

Add `newNewCmd(),` to the `root.AddCommand(...)` call in `internal/cli/root.go`.

**Step 3: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 4: Commit**

```bash
git add internal/cli/new.go internal/cli/root.go
git commit -m "Add pit new command for workspace scaffolding"
```

---

### Task 3: Add pit new to README

**Files:**
- Modify: `README.md`

**Step 1: Update README**

Add `pit new` to the Quick Start section (before `pit init`):

```bash
# Create a new workspace
pit new my_workspace                 # Python sample project (default)
pit new my_workspace --type shell    # Shell sample project
```

Add to the CLI commands table:

```
| `pit new <name>` | Create a new workspace with config, sample project, and git repo (`--type python\|sql\|shell\|dbt`) |
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "Add pit new to README"
```

---

### Task 4: Run full test suite

**Step 1: Run all tests**

Run: `go test -race ./...`
Expected: scaffold, api, config, cli, dag, meta packages all PASS

**Step 2: Run vet**

Run: `go vet ./...`
Expected: clean

**Step 3: Build and smoke test**

Run: `go build -o /tmp/pit-test ./cmd/pit && /tmp/pit-test new --help`
Expected: shows help with --type flag

**Step 4: Commit plan doc**

```bash
git add docs/plans/2026-03-07-workspace-scaffold-plan.md
git commit -m "Add workspace scaffold implementation plan"
```
