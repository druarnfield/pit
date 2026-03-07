# Metadata Store Design

**Date:** 2026-03-07
**Status:** Accepted

## Summary

Add a SQLite-backed metadata store to Pit that indexes run history, task instances, environment snapshots, and declared outputs. This becomes the backbone for `pit status`, future REST API endpoints, and cross-DAG dependency tracking.

## Decision

SQLite via `modernc.org/sqlite` (pure Go, no CGO).

### Why SQLite over alternatives

- **Query power** — the upcoming REST API needs filtering, sorting, and pagination. SQL handles this natively; a key-value store (e.g. bbolt) would require hand-written query logic.
- **Debugging** — `sqlite3 pit_metadata.db "SELECT ..."` works directly from the terminal.
- **Familiar** — contributors know SQL. The SQL runner already uses `database/sql`.
- **Concurrent access** — WAL mode supports the scheduler writing while the API reads.
- **Single file** — same deployment model as the rest of Pit. No external server.

### Trade-offs accepted

- ~8MB binary size increase from the pure-Go SQLite driver.
- Schema migrations needed over time (mitigated by a simple version table + sequential migration functions).

## Design

### Database location

```
pit_metadata.db    # workspace root, alongside pit_config.toml
```

Configurable via `pit_config.toml`:

```toml
metadata_db = "path/to/pit_metadata.db"
```

Default: `pit_metadata.db` in the workspace root (same directory where `pit run` is invoked, or the directory containing `pit_config.toml`).

### Schema

```sql
-- Schema version tracking
CREATE TABLE schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL        -- RFC3339
);

-- Run history
CREATE TABLE runs (
    id          TEXT PRIMARY KEY,    -- "20060102_150405.000_dag_name"
    dag_name    TEXT NOT NULL,
    status      TEXT NOT NULL,       -- pending/running/success/failed
    started_at  TEXT NOT NULL,       -- RFC3339
    ended_at    TEXT,                -- RFC3339, NULL while running
    run_dir     TEXT NOT NULL,       -- relative path to runs/{id}/
    trigger     TEXT,                -- cron/ftp_watch/webhook/manual
    error       TEXT                 -- top-level error message if failed
);
CREATE INDEX idx_runs_dag_started ON runs(dag_name, started_at DESC);
CREATE INDEX idx_runs_status ON runs(status);

-- Task instances within runs
CREATE TABLE task_instances (
    run_id      TEXT NOT NULL REFERENCES runs(id),
    task_name   TEXT NOT NULL,
    status      TEXT NOT NULL,       -- pending/running/success/failed/skipped/upstream_failed
    started_at  TEXT,                -- RFC3339
    ended_at    TEXT,                -- RFC3339
    attempts    INTEGER DEFAULT 1,
    error       TEXT,                -- error message from final attempt
    log_path    TEXT,                -- relative path to log file
    PRIMARY KEY (run_id, task_name)
);
CREATE INDEX idx_ti_status ON task_instances(status);

-- Environment snapshots (track when project files change)
CREATE TABLE env_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_name    TEXT NOT NULL,
    hash_type   TEXT NOT NULL,       -- "uv_lock", "pit_toml", "pyproject"
    hash_value  TEXT NOT NULL,       -- SHA-256 hex
    first_seen  TEXT NOT NULL,       -- RFC3339
    run_id      TEXT REFERENCES runs(id)
);
CREATE INDEX idx_env_dag_type ON env_snapshots(dag_name, hash_type, first_seen DESC);

-- Output declarations per run
CREATE TABLE outputs (
    run_id      TEXT NOT NULL REFERENCES runs(id),
    dag_name    TEXT NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT,                -- file, table, etc.
    location    TEXT,
    PRIMARY KEY (run_id, name)
);
CREATE INDEX idx_outputs_dag ON outputs(dag_name);
```

### Package: `internal/meta`

New package providing a `Store` interface and SQLite implementation.

```go
package meta

// Store is the metadata storage interface.
type Store interface {
    // Lifecycle
    Close() error

    // Run tracking
    InsertRun(r RunRecord) error
    UpdateRun(id string, status string, endedAt time.Time, errMsg string) error

    // Task instance tracking
    InsertTaskInstance(ti TaskInstanceRecord) error
    UpdateTaskInstance(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error

    // Environment tracking
    RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error

    // Output tracking
    RecordOutputs(runID, dagName string, outputs []OutputRecord) error

    // Queries
    LatestRuns(dagName string, limit int) ([]RunRecord, error)
    RunsByStatus(status string, limit int) ([]RunRecord, error)
    RunDetail(runID string) (*RunRecord, []TaskInstanceRecord, error)
    EnvHistory(dagName, hashType string, limit int) ([]EnvSnapshotRecord, error)
    OutputsByRun(runID string) ([]OutputRecord, error)
    LatestRunPerDAG() ([]RunRecord, error)
}
```

#### Record types

```go
type RunRecord struct {
    ID        string
    DAGName   string
    Status    string
    StartedAt time.Time
    EndedAt   *time.Time   // nil while running
    RunDir    string
    Trigger   string       // "manual", "cron", "ftp_watch", "webhook"
    Error     string
}

type TaskInstanceRecord struct {
    RunID    string
    TaskName string
    Status   string
    StartedAt *time.Time
    EndedAt   *time.Time
    Attempts  int
    Error     string
    LogPath   string
}

type EnvSnapshotRecord struct {
    ID        int
    DAGName   string
    HashType  string
    HashValue string
    FirstSeen time.Time
    RunID     string
}

type OutputRecord struct {
    RunID    string
    DAGName  string
    Name     string
    Type     string
    Location string
}
```

### Schema migrations

Simple sequential approach:

```go
var migrations = []string{
    v1Schema, // initial schema above
    // future: v2, v3, etc.
}

func (s *SQLiteStore) migrate() error {
    currentVersion := s.currentVersion() // 0 if fresh
    for i := currentVersion; i < len(migrations); i++ {
        // execute migrations[i] in a transaction
        // insert into schema_version
    }
    return nil
}
```

Migrations run automatically on `Open()`. Each migration is a SQL string executed in a transaction.

### Integration with engine

The store is passed into the engine via `ExecuteOpts`:

```go
type ExecuteOpts struct {
    // ... existing fields ...
    MetaStore meta.Store  // nil = no metadata tracking (backwards compatible)
}
```

The engine records metadata at these points:

| Event | Store call |
|-------|-----------|
| Run starts | `InsertRun(...)` |
| Task starts | `InsertTaskInstance(...)` with status "running" |
| Task completes | `UpdateTaskInstance(...)` with final status |
| Run completes | `UpdateRun(...)` with final status |
| Run starts (after snapshot) | `RecordEnvSnapshot(...)` for changed hashes |
| Run completes (success) | `RecordOutputs(...)` from config |

### Environment hash tracking

At run start, after the snapshot is created, the engine computes SHA-256 hashes of key project files:

| File | hash_type |
|------|-----------|
| `uv.lock` | `uv_lock` |
| `pit.toml` | `pit_toml` |
| `pyproject.toml` | `pyproject` |

`RecordEnvSnapshot` only inserts a row if the hash differs from the most recent entry for that `(dag_name, hash_type)` pair. This gives a changelog of when project configuration changed.

### CLI integration

**`pit run`** — opens the store before execution, passes it via `ExecuteOpts`, closes after.

**`pit status`** (currently stubbed) — queries `LatestRunPerDAG()` to show a table:

```
DAG            Last Run             Status    Duration
─────────────  ───────────────────  ────────  ────────
etl_pipeline   2026-03-07 14:30:00  success   2m 15s
daily_report   2026-03-07 06:00:00  failed    0m 42s
```

**`pit serve`** — opens the store once at startup, shares across all triggered runs.

### Future REST API (out of scope, but designed for)

The `Store` interface methods map directly to API endpoints:

| Endpoint | Store method |
|----------|-------------|
| `GET /api/dags` | `LatestRunPerDAG()` |
| `GET /api/dags/{name}/runs` | `LatestRuns(name, limit)` |
| `GET /api/runs/{id}` | `RunDetail(id)` |
| `GET /api/dags/{name}/env` | `EnvHistory(name, type, limit)` |
| `GET /api/runs/{id}/outputs` | `OutputsByRun(id)` |

### Concurrency

- WAL mode enabled on open: `PRAGMA journal_mode=WAL`
- Single writer (engine or scheduler) with concurrent readers (API, status CLI)
- `sync.Mutex` wrapping writes is not needed — SQLite WAL handles this internally with busy timeouts
- `PRAGMA busy_timeout=5000` to handle brief write contention

### Testing approach

- Unit tests use `:memory:` SQLite databases (no temp files needed)
- Test the `Store` interface methods directly: insert, update, query
- Integration tests verify engine-to-store wiring (behind `//go:build integration` tag)

## File changes

| Action | Path | Description |
|--------|------|-------------|
| Create | `internal/meta/store.go` | `Store` interface and record types |
| Create | `internal/meta/sqlite.go` | SQLite implementation |
| Create | `internal/meta/migrations.go` | Schema definitions and migration runner |
| Create | `internal/meta/meta_test.go` | Unit tests |
| Modify | `internal/engine/executor.go` | Add `MetaStore` to `ExecuteOpts`, call store at run/task lifecycle points |
| Modify | `internal/cli/run.go` | Open store, pass to engine |
| Modify | `internal/cli/serve.go` | Open store at startup, share across runs |
| Modify | `internal/cli/status.go` | Implement using store queries |
| Modify | `internal/config/pit_config.go` | Add `MetadataDB` field |
| Modify | `go.mod` | Add `modernc.org/sqlite` dependency |
