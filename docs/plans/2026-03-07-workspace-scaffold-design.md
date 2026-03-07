# Workspace Scaffold Design

**Date:** 2026-03-07
**Status:** Accepted

## Summary

Add `pit new <name>` to create a ready-to-run workspace directory with config, gitignore, README, and a sample project. Reduces onboarding friction — users get a working structure they can `pit validate` and `pit run` immediately.

## Command

```
pit new <name> [--type python|sql|shell|dbt]
```

- Creates `./<name>/` relative to the current directory
- Errors if the directory already exists
- Name must match `[a-z][a-z0-9_]*` (reuses existing validation)
- `--type` defaults to `python`, passed through to the sample project scaffold

## Generated Layout

```
<name>/
├── .git/                    # git init
├── .gitignore
├── pit_config.toml
├── README.md
└── projects/
    └── sample_pipeline/     # type-dependent (reuses scaffold.Create)
        ├── pit.toml
        ├── tasks/
        │   └── hello.py
        └── ...
```

### `.gitignore`

```
runs/
.venv/
repo_cache/
*.db
secrets/
```

### `pit_config.toml`

All fields commented out so users see what's available without being opinionated about defaults:

```toml
# Pit workspace configuration
# See: https://github.com/druarnfield/pit

# runs_dir = "runs"
# repo_cache_dir = "repo_cache"
# metadata_db = "pit_metadata.db"
# secrets_dir = "secrets/secrets.toml"
# api_token = ""
# dbt_driver = "ODBC Driver 17 for SQL Server"
# keep_artifacts = ["logs", "project", "data"]
```

### `README.md`

Quick reference: layout description, commands cheat sheet (`pit validate`, `pit run`, `pit serve`, `pit init`), link to main repo README.

### Sample Project

Always named `sample_pipeline`. Created by calling the existing `scaffold.Create()` with the user's chosen `--type`. Reuses all existing project templates.

## Implementation

### Design Principle

All generated content lives as simple string functions — same pattern as the existing scaffold templates. No template engine, no embedded files. When the project grows and templates need to change, you edit one function.

### Package Changes

Extend `internal/scaffold` with one new public function:

```go
func CreateWorkspace(dir, name string, projectType ProjectType) error
```

Steps:
1. Validate name
2. Check directory doesn't exist
3. Create workspace directory
4. Write `.gitignore`, `pit_config.toml`, `README.md`
5. Call `scaffold.Create(dir, "sample_pipeline", projectType)`
6. Run `git init` in the workspace directory

Each generated file gets its own string function: `workspaceGitignore()`, `workspacePitConfig()`, `workspaceReadme(name)`. Same convention as `pitTomlPython()`, `helloPy()`, etc.

### CLI

New command in `internal/cli/new.go`:

```go
func newNewCmd() *cobra.Command
```

Thin wrapper: parse `--type` flag, call `scaffold.CreateWorkspace`, print next steps:

```
Created workspace "my_workspace" with a python sample project.

Next steps:
  cd my_workspace
  pit validate
  pit run sample_pipeline
```

### Testing

- `t.TempDir()` based tests in `internal/scaffold/`
- Verify all expected files exist
- Verify file contents contain expected strings
- Verify `.git/` directory exists (git init ran)
- Verify `pit validate` passes against the generated workspace (integration test)

## File Changes

| Action | Path | Description |
|--------|------|-------------|
| Modify | `internal/scaffold/scaffold.go` | Add `CreateWorkspace()` and string functions for generated files |
| Create | `internal/cli/new.go` | `pit new` command |
| Modify | `internal/cli/root.go` | Register `newNewCmd()` |
| Modify | `internal/scaffold/scaffold_test.go` | Tests for `CreateWorkspace` |
| Modify | `README.md` | Add `pit new` to commands table |
