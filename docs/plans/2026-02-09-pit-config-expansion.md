# Expand pit_config.toml — Workspace Config

## Context

`pit_config.toml` exists with only `secrets_dir`. Expanding it to reduce CLI flag repetition and centralise environment-specific settings.

## New Fields

```toml
# pit_config.toml (workspace level)
secrets_dir = "secrets/secrets.toml"     # existing
runs_dir = "runs"                         # new: custom snapshot location
dbt_driver = "ODBC Driver 17 for SQL Server"  # new: default ODBC driver for dbt profiles
keep_artifacts = ["logs", "project", "data"]  # new: which run subdirs to preserve
```

All fields optional. Defaults: `runs_dir = "runs"`, `dbt_driver = "ODBC Driver 17 for SQL Server"`, `keep_artifacts = ["logs", "project", "data"]`.

### Per-project override

Only `keep_artifacts` has a per-project override in `pit.toml`:

```toml
[dag]
name = "my_pipeline"
keep_artifacts = ["logs"]
```

Resolution: per-project (if non-empty) > workspace (if non-empty) > default (all three).

## Implementation Steps

### Step 1: Config structs

- Expand `PitConfig` with `RunsDir`, `DBTDriver`, `KeepArtifacts`
- Add `KeepArtifacts` to `DAGConfig`
- `LoadPitConfig`: resolve `runs_dir` relative paths, validate `keep_artifacts` entries against `{"logs", "project", "data"}`
- Tests + testdata fixtures

### Step 2: dbt driver passthrough

- Add `Driver` field to `DBTProfilesInput`
- Template uses `{{ .Driver }}` instead of hardcoded string
- Executor populates from `ExecuteOpts.DBTDriver`, default if empty
- Update existing profiles tests

### Step 3: Wire into CLI

- `root.go`: apply `runs_dir` as fallback (same pattern as `secrets_dir`), stash workspace config
- `ExecuteOpts`: add `KeepArtifacts`, `DBTDriver` fields
- CLI commands resolve and pass through to executor

### Step 4: Artifact cleanup

- `cleanupArtifacts(runDir, keepArtifacts)` in engine package
- Called after `printSummary` in `Execute()`
- Removes subdirs not in keep list
- Tests: keep logs only, keep all, keep none

### Step 5: Validation

- `dag/validate.go`: validate `dag.keep_artifacts` entries if set
- Tests for valid/invalid values

### Step 6: Documentation

- Update README: workspace config section, `keep_artifacts` docs, `dbt_driver` docs
