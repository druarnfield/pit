# Metadata Store Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a SQLite-backed metadata store that indexes run history, task instances, environment snapshots, and declared outputs.

**Architecture:** New `internal/meta` package with a `Store` interface and `SQLiteStore` implementation. The engine writes metadata at run/task lifecycle points via an optional `MetaStore` field on `ExecuteOpts`. CLI commands open the store and pass it through. `pit status` is implemented using store queries.

**Tech Stack:** `modernc.org/sqlite` (pure-Go SQLite driver), `database/sql` stdlib interface

---

### Task 1: Add SQLite dependency

**Files:**
- Modify: `go.mod`

**Step 1: Add the dependency**

Run: `cd /Users/dru/Documents/Development/go/pit && go get modernc.org/sqlite`

**Step 2: Verify it compiles**

Run: `go build ./...`
Expected: clean build, no errors

**Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "Add modernc.org/sqlite dependency for metadata store"
```

---

### Task 2: Create Store interface and record types

**Files:**
- Create: `internal/meta/store.go`
- Test: `internal/meta/meta_test.go`

**Step 1: Write the test that imports the package and checks record types**

Create `internal/meta/meta_test.go`:

```go
package meta

import (
	"testing"
	"time"
)

func TestRunRecordFields(t *testing.T) {
	now := time.Now()
	r := RunRecord{
		ID:        "20260307_143000.000_test_dag",
		DAGName:   "test_dag",
		Status:    "running",
		StartedAt: now,
		RunDir:    "runs/20260307_143000.000_test_dag",
		Trigger:   "manual",
	}
	if r.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	if r.EndedAt != nil {
		t.Error("expected nil EndedAt for running run")
	}
}

func TestTaskInstanceRecordFields(t *testing.T) {
	ti := TaskInstanceRecord{
		RunID:    "20260307_143000.000_test_dag",
		TaskName: "extract",
		Status:   "pending",
		Attempts: 0,
		LogPath:  "runs/20260307_143000.000_test_dag/logs/extract.log",
	}
	if ti.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/meta/`
Expected: FAIL — package does not exist yet

**Step 3: Write the store.go file**

Create `internal/meta/store.go`:

```go
package meta

import "time"

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

// RunRecord represents a DAG execution in the metadata store.
type RunRecord struct {
	ID        string
	DAGName   string
	Status    string
	StartedAt time.Time
	EndedAt   *time.Time
	RunDir    string
	Trigger   string
	Error     string
}

// TaskInstanceRecord represents a task execution within a run.
type TaskInstanceRecord struct {
	RunID     string
	TaskName  string
	Status    string
	StartedAt *time.Time
	EndedAt   *time.Time
	Attempts  int
	Error     string
	LogPath   string
}

// EnvSnapshotRecord represents a point-in-time hash of a project file.
type EnvSnapshotRecord struct {
	ID        int
	DAGName   string
	HashType  string
	HashValue string
	FirstSeen time.Time
	RunID     string
}

// OutputRecord represents a declared output from a run.
type OutputRecord struct {
	RunID    string
	DAGName  string
	Name     string
	Type     string
	Location string
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./internal/meta/`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/meta/store.go internal/meta/meta_test.go
git commit -m "Add meta package with Store interface and record types"
```

---

### Task 3: Create schema migrations

**Files:**
- Create: `internal/meta/migrations.go`
- Modify: `internal/meta/meta_test.go`

**Step 1: Write test for migration**

Add to `internal/meta/meta_test.go`:

```go
func TestOpenMemory(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open(:memory:) unexpected error: %v", err)
	}
	defer store.Close()
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/meta/`
Expected: FAIL — `Open` not defined

**Step 3: Write migrations.go**

Create `internal/meta/migrations.go`:

```go
package meta

const v1Schema = `
CREATE TABLE IF NOT EXISTS schema_version (
	version    INTEGER PRIMARY KEY,
	applied_at TEXT NOT NULL
);

CREATE TABLE runs (
	id         TEXT PRIMARY KEY,
	dag_name   TEXT NOT NULL,
	status     TEXT NOT NULL,
	started_at TEXT NOT NULL,
	ended_at   TEXT,
	run_dir    TEXT NOT NULL,
	trigger_source TEXT,
	error      TEXT
);
CREATE INDEX idx_runs_dag_started ON runs(dag_name, started_at DESC);
CREATE INDEX idx_runs_status ON runs(status);

CREATE TABLE task_instances (
	run_id    TEXT NOT NULL REFERENCES runs(id),
	task_name TEXT NOT NULL,
	status    TEXT NOT NULL,
	started_at TEXT,
	ended_at   TEXT,
	attempts   INTEGER DEFAULT 1,
	error      TEXT,
	log_path   TEXT,
	PRIMARY KEY (run_id, task_name)
);
CREATE INDEX idx_ti_status ON task_instances(status);

CREATE TABLE env_snapshots (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	dag_name   TEXT NOT NULL,
	hash_type  TEXT NOT NULL,
	hash_value TEXT NOT NULL,
	first_seen TEXT NOT NULL,
	run_id     TEXT REFERENCES runs(id)
);
CREATE INDEX idx_env_dag_type ON env_snapshots(dag_name, hash_type, first_seen DESC);

CREATE TABLE outputs (
	run_id   TEXT NOT NULL REFERENCES runs(id),
	dag_name TEXT NOT NULL,
	name     TEXT NOT NULL,
	type     TEXT,
	location TEXT,
	PRIMARY KEY (run_id, name)
);
CREATE INDEX idx_outputs_dag ON outputs(dag_name);
`

var migrations = []string{
	v1Schema,
}
```

Note: the design doc used `trigger` as a column name but that's a SQL reserved word. Use `trigger_source` instead. Update the `RunRecord.Trigger` field mapping accordingly in the SQLite implementation.

**Step 4: Run test to verify it still fails**

Run: `go test ./internal/meta/`
Expected: FAIL — `Open` still not defined (that's in sqlite.go, next task)

---

### Task 4: Implement SQLiteStore — Open, Close, migrate

**Files:**
- Create: `internal/meta/sqlite.go`
- Modify: `internal/meta/meta_test.go`

**Step 1: The test from Task 3 Step 1 is already written** (`TestOpenMemory`)

**Step 2: Write sqlite.go with Open, Close, migrate**

Create `internal/meta/sqlite.go`:

```go
package meta

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore implements Store using a SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// Open creates or opens a SQLite database at the given path and runs migrations.
// Use ":memory:" for an in-memory database (useful for testing).
func Open(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Enable WAL mode and busy timeout for concurrent access
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("setting WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("setting busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enabling foreign keys: %w", err)
	}

	s := &SQLiteStore{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return s, nil
}

// Close closes the database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) currentVersion() int {
	var v int
	err := s.db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_version").Scan(&v)
	if err != nil {
		return 0 // table doesn't exist yet
	}
	return v
}

func (s *SQLiteStore) migrate() error {
	// Ensure the schema_version table exists for version checking
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (
		version    INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("creating schema_version table: %w", err)
	}

	current := s.currentVersion()
	for i := current; i < len(migrations); i++ {
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("beginning migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(migrations[i]); err != nil {
			tx.Rollback()
			return fmt.Errorf("executing migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(
			"INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
			i+1, time.Now().UTC().Format(time.RFC3339),
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("recording migration %d: %w", i+1, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing migration %d: %w", i+1, err)
		}
	}
	return nil
}
```

**Step 3: Run test to verify Open works**

Run: `go test ./internal/meta/ -run TestOpenMemory -v`
Expected: PASS

**Step 4: Add migration idempotency test**

Add to `internal/meta/meta_test.go`:

```go
func TestOpenTwice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	s2.Close()
}
```

Add `"path/filepath"` to the test imports.

**Step 5: Run tests**

Run: `go test ./internal/meta/ -v`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/meta/sqlite.go internal/meta/migrations.go internal/meta/meta_test.go
git commit -m "Add SQLiteStore with Open, Close, and schema migrations"
```

---

### Task 5: Implement run write methods (InsertRun, UpdateRun)

**Files:**
- Modify: `internal/meta/sqlite.go`
- Modify: `internal/meta/meta_test.go`

**Step 1: Write failing tests**

Add to `internal/meta/meta_test.go`:

```go
func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open(:memory:): %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestInsertRun(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()

	err := s.InsertRun(RunRecord{
		ID:        "20260307_143000.000_test_dag",
		DAGName:   "test_dag",
		Status:    "running",
		StartedAt: now,
		RunDir:    "runs/20260307_143000.000_test_dag",
		Trigger:   "manual",
	})
	if err != nil {
		t.Fatalf("InsertRun: %v", err)
	}

	// Verify it was inserted
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM runs").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 run, got %d", count)
	}
}

func TestUpdateRun(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()

	s.InsertRun(RunRecord{
		ID:        "20260307_143000.000_test_dag",
		DAGName:   "test_dag",
		Status:    "running",
		StartedAt: now,
		RunDir:    "runs/20260307_143000.000_test_dag",
		Trigger:   "manual",
	})

	ended := now.Add(2 * time.Minute)
	err := s.UpdateRun("20260307_143000.000_test_dag", "success", ended, "")
	if err != nil {
		t.Fatalf("UpdateRun: %v", err)
	}

	var status, endedAt string
	s.db.QueryRow("SELECT status, ended_at FROM runs WHERE id = ?", "20260307_143000.000_test_dag").Scan(&status, &endedAt)
	if status != "success" {
		t.Errorf("status = %q, want %q", status, "success")
	}
}

func TestInsertRunDuplicate(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	r := RunRecord{
		ID: "20260307_143000.000_test_dag", DAGName: "test_dag",
		Status: "running", StartedAt: now, RunDir: "runs/x", Trigger: "manual",
	}
	if err := s.InsertRun(r); err != nil {
		t.Fatalf("first InsertRun: %v", err)
	}
	if err := s.InsertRun(r); err == nil {
		t.Error("expected error on duplicate InsertRun, got nil")
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/meta/ -run "TestInsertRun|TestUpdateRun" -v`
Expected: FAIL — methods not implemented

**Step 3: Implement InsertRun and UpdateRun**

Add to `internal/meta/sqlite.go`:

```go
func (s *SQLiteStore) InsertRun(r RunRecord) error {
	var endedAt *string
	if r.EndedAt != nil {
		v := r.EndedAt.UTC().Format(time.RFC3339)
		endedAt = &v
	}
	_, err := s.db.Exec(
		`INSERT INTO runs (id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.DAGName, r.Status,
		r.StartedAt.UTC().Format(time.RFC3339),
		endedAt, r.RunDir, r.Trigger, nilIfEmpty(r.Error),
	)
	return err
}

func (s *SQLiteStore) UpdateRun(id string, status string, endedAt time.Time, errMsg string) error {
	_, err := s.db.Exec(
		`UPDATE runs SET status = ?, ended_at = ?, error = ? WHERE id = ?`,
		status, endedAt.UTC().Format(time.RFC3339), nilIfEmpty(errMsg), id,
	)
	return err
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
```

**Step 4: Run tests to verify they pass**

Run: `go test ./internal/meta/ -run "TestInsertRun|TestUpdateRun" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/meta/sqlite.go internal/meta/meta_test.go
git commit -m "Implement InsertRun and UpdateRun on SQLiteStore"
```

---

### Task 6: Implement task instance write methods

**Files:**
- Modify: `internal/meta/sqlite.go`
- Modify: `internal/meta/meta_test.go`

**Step 1: Write failing tests**

Add to `internal/meta/meta_test.go`:

```go
func insertTestRun(t *testing.T, s *SQLiteStore) string {
	t.Helper()
	id := "20260307_143000.000_test_dag"
	err := s.InsertRun(RunRecord{
		ID: id, DAGName: "test_dag", Status: "running",
		StartedAt: time.Now().UTC(), RunDir: "runs/" + id, Trigger: "manual",
	})
	if err != nil {
		t.Fatalf("insertTestRun: %v", err)
	}
	return id
}

func TestInsertTaskInstance(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)
	now := time.Now().UTC()

	err := s.InsertTaskInstance(TaskInstanceRecord{
		RunID: runID, TaskName: "extract", Status: "running",
		StartedAt: &now, Attempts: 1,
		LogPath: "runs/" + runID + "/logs/extract.log",
	})
	if err != nil {
		t.Fatalf("InsertTaskInstance: %v", err)
	}

	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM task_instances").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 task instance, got %d", count)
	}
}

func TestUpdateTaskInstance(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)
	now := time.Now().UTC()

	s.InsertTaskInstance(TaskInstanceRecord{
		RunID: runID, TaskName: "extract", Status: "running",
		StartedAt: &now, Attempts: 1,
	})

	ended := now.Add(30 * time.Second)
	err := s.UpdateTaskInstance(runID, "extract", "success", ended, 1, "")
	if err != nil {
		t.Fatalf("UpdateTaskInstance: %v", err)
	}

	var status string
	var attempts int
	s.db.QueryRow("SELECT status, attempts FROM task_instances WHERE run_id = ? AND task_name = ?",
		runID, "extract").Scan(&status, &attempts)
	if status != "success" {
		t.Errorf("status = %q, want %q", status, "success")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/meta/ -run "TestInsertTaskInstance|TestUpdateTaskInstance" -v`
Expected: FAIL

**Step 3: Implement methods**

Add to `internal/meta/sqlite.go`:

```go
func (s *SQLiteStore) InsertTaskInstance(ti TaskInstanceRecord) error {
	var startedAt *string
	if ti.StartedAt != nil {
		v := ti.StartedAt.UTC().Format(time.RFC3339)
		startedAt = &v
	}
	var endedAt *string
	if ti.EndedAt != nil {
		v := ti.EndedAt.UTC().Format(time.RFC3339)
		endedAt = &v
	}
	_, err := s.db.Exec(
		`INSERT INTO task_instances (run_id, task_name, status, started_at, ended_at, attempts, error, log_path)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ti.RunID, ti.TaskName, ti.Status, startedAt, endedAt,
		ti.Attempts, nilIfEmpty(ti.Error), nilIfEmpty(ti.LogPath),
	)
	return err
}

func (s *SQLiteStore) UpdateTaskInstance(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error {
	_, err := s.db.Exec(
		`UPDATE task_instances SET status = ?, ended_at = ?, attempts = ?, error = ?
		 WHERE run_id = ? AND task_name = ?`,
		status, endedAt.UTC().Format(time.RFC3339), attempts, nilIfEmpty(errMsg),
		runID, taskName,
	)
	return err
}
```

**Step 4: Run tests**

Run: `go test ./internal/meta/ -run "TestInsertTaskInstance|TestUpdateTaskInstance" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/meta/sqlite.go internal/meta/meta_test.go
git commit -m "Implement InsertTaskInstance and UpdateTaskInstance on SQLiteStore"
```

---

### Task 7: Implement env snapshot and output write methods

**Files:**
- Modify: `internal/meta/sqlite.go`
- Modify: `internal/meta/meta_test.go`

**Step 1: Write failing tests**

Add to `internal/meta/meta_test.go`:

```go
func TestRecordEnvSnapshot(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	// First insert — new hash
	err := s.RecordEnvSnapshot("test_dag", "uv_lock", "abc123", runID)
	if err != nil {
		t.Fatalf("RecordEnvSnapshot: %v", err)
	}

	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM env_snapshots").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 env snapshot, got %d", count)
	}

	// Same hash — should not insert
	err = s.RecordEnvSnapshot("test_dag", "uv_lock", "abc123", runID)
	if err != nil {
		t.Fatalf("RecordEnvSnapshot same hash: %v", err)
	}

	s.db.QueryRow("SELECT COUNT(*) FROM env_snapshots").Scan(&count)
	if count != 1 {
		t.Errorf("expected still 1 env snapshot after same hash, got %d", count)
	}

	// Different hash — should insert
	err = s.RecordEnvSnapshot("test_dag", "uv_lock", "def456", runID)
	if err != nil {
		t.Fatalf("RecordEnvSnapshot new hash: %v", err)
	}

	s.db.QueryRow("SELECT COUNT(*) FROM env_snapshots").Scan(&count)
	if count != 2 {
		t.Errorf("expected 2 env snapshots after new hash, got %d", count)
	}
}

func TestRecordOutputs(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	outputs := []OutputRecord{
		{Name: "report", Type: "file", Location: "/data/report.csv"},
		{Name: "summary", Type: "table", Location: "dbo.summary"},
	}

	err := s.RecordOutputs(runID, "test_dag", outputs)
	if err != nil {
		t.Fatalf("RecordOutputs: %v", err)
	}

	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM outputs").Scan(&count)
	if count != 2 {
		t.Errorf("expected 2 outputs, got %d", count)
	}
}

func TestRecordOutputsEmpty(t *testing.T) {
	s := newTestStore(t)
	_ = insertTestRun(t, s)

	// Empty outputs should not error
	err := s.RecordOutputs("20260307_143000.000_test_dag", "test_dag", nil)
	if err != nil {
		t.Fatalf("RecordOutputs(nil): %v", err)
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/meta/ -run "TestRecordEnv|TestRecordOutputs" -v`
Expected: FAIL

**Step 3: Implement methods**

Add to `internal/meta/sqlite.go`:

```go
func (s *SQLiteStore) RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error {
	// Check if the latest hash for this dag+type is the same
	var latest string
	err := s.db.QueryRow(
		`SELECT hash_value FROM env_snapshots
		 WHERE dag_name = ? AND hash_type = ?
		 ORDER BY first_seen DESC LIMIT 1`,
		dagName, hashType,
	).Scan(&latest)
	if err == nil && latest == hashValue {
		return nil // no change
	}

	_, err = s.db.Exec(
		`INSERT INTO env_snapshots (dag_name, hash_type, hash_value, first_seen, run_id)
		 VALUES (?, ?, ?, ?, ?)`,
		dagName, hashType, hashValue,
		time.Now().UTC().Format(time.RFC3339), runID,
	)
	return err
}

func (s *SQLiteStore) RecordOutputs(runID, dagName string, outputs []OutputRecord) error {
	if len(outputs) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		`INSERT INTO outputs (run_id, dag_name, name, type, location)
		 VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, o := range outputs {
		if _, err := stmt.Exec(runID, dagName, o.Name, nilIfEmpty(o.Type), nilIfEmpty(o.Location)); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
```

**Step 4: Run tests**

Run: `go test ./internal/meta/ -run "TestRecordEnv|TestRecordOutputs" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/meta/sqlite.go internal/meta/meta_test.go
git commit -m "Implement RecordEnvSnapshot and RecordOutputs on SQLiteStore"
```

---

### Task 8: Implement query methods

**Files:**
- Modify: `internal/meta/sqlite.go`
- Modify: `internal/meta/meta_test.go`

**Step 1: Write failing tests**

Add to `internal/meta/meta_test.go`:

```go
func seedRuns(t *testing.T, s *SQLiteStore) {
	t.Helper()
	for i, name := range []string{"dag_a", "dag_b"} {
		started := time.Date(2026, 3, 7, 10+i, 0, 0, 0, time.UTC)
		ended := started.Add(5 * time.Minute)
		id := fmt.Sprintf("20260307_%02d0000.000_%s", 10+i, name)
		s.InsertRun(RunRecord{
			ID: id, DAGName: name, Status: "success",
			StartedAt: started, EndedAt: &ended,
			RunDir: "runs/" + id, Trigger: "cron",
		})
		now := started
		s.InsertTaskInstance(TaskInstanceRecord{
			RunID: id, TaskName: "step1", Status: "success",
			StartedAt: &now, EndedAt: &ended, Attempts: 1,
			LogPath: "runs/" + id + "/logs/step1.log",
		})
	}
}

func TestLatestRuns(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.LatestRuns("dag_a", 10)
	if err != nil {
		t.Fatalf("LatestRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run for dag_a, got %d", len(runs))
	}
	if runs[0].DAGName != "dag_a" {
		t.Errorf("DAGName = %q, want %q", runs[0].DAGName, "dag_a")
	}
}

func TestLatestRunsAllDAGs(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.LatestRuns("", 10)
	if err != nil {
		t.Fatalf("LatestRuns all: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("expected 2 runs, got %d", len(runs))
	}
}

func TestRunsByStatus(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.RunsByStatus("success", 10)
	if err != nil {
		t.Fatalf("RunsByStatus: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("expected 2 success runs, got %d", len(runs))
	}

	runs, err = s.RunsByStatus("failed", 10)
	if err != nil {
		t.Fatalf("RunsByStatus failed: %v", err)
	}
	if len(runs) != 0 {
		t.Errorf("expected 0 failed runs, got %d", len(runs))
	}
}

func TestRunDetail(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	run, tasks, err := s.RunDetail("20260307_100000.000_dag_a")
	if err != nil {
		t.Fatalf("RunDetail: %v", err)
	}
	if run == nil {
		t.Fatal("expected non-nil run")
	}
	if run.DAGName != "dag_a" {
		t.Errorf("DAGName = %q, want %q", run.DAGName, "dag_a")
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}
}

func TestRunDetailNotFound(t *testing.T) {
	s := newTestStore(t)

	run, tasks, err := s.RunDetail("nonexistent")
	if err != nil {
		t.Fatalf("RunDetail: %v", err)
	}
	if run != nil {
		t.Error("expected nil run for nonexistent ID")
	}
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(tasks))
	}
}

func TestEnvHistory(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	s.RecordEnvSnapshot("test_dag", "uv_lock", "hash1", runID)
	s.RecordEnvSnapshot("test_dag", "uv_lock", "hash2", runID)

	records, err := s.EnvHistory("test_dag", "uv_lock", 10)
	if err != nil {
		t.Fatalf("EnvHistory: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 env records, got %d", len(records))
	}
	// Should be newest first
	if records[0].HashValue != "hash2" {
		t.Errorf("first record hash = %q, want %q", records[0].HashValue, "hash2")
	}
}

func TestOutputsByRun(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	s.RecordOutputs(runID, "test_dag", []OutputRecord{
		{Name: "report", Type: "file", Location: "/data/report.csv"},
	})

	outputs, err := s.OutputsByRun(runID)
	if err != nil {
		t.Fatalf("OutputsByRun: %v", err)
	}
	if len(outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outputs))
	}
	if outputs[0].Name != "report" {
		t.Errorf("output name = %q, want %q", outputs[0].Name, "report")
	}
}

func TestLatestRunPerDAG(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.LatestRunPerDAG()
	if err != nil {
		t.Fatalf("LatestRunPerDAG: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("expected 2 DAGs, got %d", len(runs))
	}
}
```

Add `"fmt"` to test imports.

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/meta/ -run "TestLatestRun|TestRunsByStatus|TestRunDetail|TestEnvHistory|TestOutputsByRun|TestLatestRunPerDAG" -v`
Expected: FAIL

**Step 3: Implement all query methods**

Add to `internal/meta/sqlite.go`:

```go
func (s *SQLiteStore) LatestRuns(dagName string, limit int) ([]RunRecord, error) {
	var query string
	var args []any
	if dagName != "" {
		query = `SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
				 FROM runs WHERE dag_name = ? ORDER BY started_at DESC LIMIT ?`
		args = []any{dagName, limit}
	} else {
		query = `SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
				 FROM runs ORDER BY started_at DESC LIMIT ?`
		args = []any{limit}
	}
	return s.scanRuns(query, args...)
}

func (s *SQLiteStore) RunsByStatus(status string, limit int) ([]RunRecord, error) {
	return s.scanRuns(
		`SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
		 FROM runs WHERE status = ? ORDER BY started_at DESC LIMIT ?`,
		status, limit,
	)
}

func (s *SQLiteStore) RunDetail(runID string) (*RunRecord, []TaskInstanceRecord, error) {
	runs, err := s.scanRuns(
		`SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
		 FROM runs WHERE id = ?`, runID,
	)
	if err != nil {
		return nil, nil, err
	}
	if len(runs) == 0 {
		return nil, nil, nil
	}

	rows, err := s.db.Query(
		`SELECT run_id, task_name, status, started_at, ended_at, attempts, error, log_path
		 FROM task_instances WHERE run_id = ? ORDER BY task_name`, runID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var tasks []TaskInstanceRecord
	for rows.Next() {
		var ti TaskInstanceRecord
		var startedAt, endedAt, errMsg, logPath sql.NullString
		if err := rows.Scan(&ti.RunID, &ti.TaskName, &ti.Status,
			&startedAt, &endedAt, &ti.Attempts, &errMsg, &logPath); err != nil {
			return nil, nil, err
		}
		if startedAt.Valid {
			t, _ := time.Parse(time.RFC3339, startedAt.String)
			ti.StartedAt = &t
		}
		if endedAt.Valid {
			t, _ := time.Parse(time.RFC3339, endedAt.String)
			ti.EndedAt = &t
		}
		ti.Error = errMsg.String
		ti.LogPath = logPath.String
		tasks = append(tasks, ti)
	}

	return &runs[0], tasks, rows.Err()
}

func (s *SQLiteStore) EnvHistory(dagName, hashType string, limit int) ([]EnvSnapshotRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, dag_name, hash_type, hash_value, first_seen, run_id
		 FROM env_snapshots WHERE dag_name = ? AND hash_type = ?
		 ORDER BY first_seen DESC LIMIT ?`,
		dagName, hashType, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []EnvSnapshotRecord
	for rows.Next() {
		var r EnvSnapshotRecord
		var firstSeen, runID string
		if err := rows.Scan(&r.ID, &r.DAGName, &r.HashType, &r.HashValue, &firstSeen, &runID); err != nil {
			return nil, err
		}
		r.FirstSeen, _ = time.Parse(time.RFC3339, firstSeen)
		r.RunID = runID
		records = append(records, r)
	}
	return records, rows.Err()
}

func (s *SQLiteStore) OutputsByRun(runID string) ([]OutputRecord, error) {
	rows, err := s.db.Query(
		`SELECT run_id, dag_name, name, type, location
		 FROM outputs WHERE run_id = ? ORDER BY name`, runID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []OutputRecord
	for rows.Next() {
		var o OutputRecord
		var typ, loc sql.NullString
		if err := rows.Scan(&o.RunID, &o.DAGName, &o.Name, &typ, &loc); err != nil {
			return nil, err
		}
		o.Type = typ.String
		o.Location = loc.String
		outputs = append(outputs, o)
	}
	return outputs, rows.Err()
}

func (s *SQLiteStore) LatestRunPerDAG() ([]RunRecord, error) {
	return s.scanRuns(
		`SELECT r.id, r.dag_name, r.status, r.started_at, r.ended_at, r.run_dir, r.trigger_source, r.error
		 FROM runs r
		 INNER JOIN (
			 SELECT dag_name, MAX(started_at) AS max_started
			 FROM runs GROUP BY dag_name
		 ) latest ON r.dag_name = latest.dag_name AND r.started_at = latest.max_started
		 ORDER BY r.dag_name`,
	)
}

// scanRuns is a helper that scans RunRecord rows from a query.
func (s *SQLiteStore) scanRuns(query string, args ...any) ([]RunRecord, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []RunRecord
	for rows.Next() {
		var r RunRecord
		var startedAt string
		var endedAt, trigger, errMsg sql.NullString
		if err := rows.Scan(&r.ID, &r.DAGName, &r.Status, &startedAt,
			&endedAt, &r.RunDir, &trigger, &errMsg); err != nil {
			return nil, err
		}
		r.StartedAt, _ = time.Parse(time.RFC3339, startedAt)
		if endedAt.Valid {
			t, _ := time.Parse(time.RFC3339, endedAt.String)
			r.EndedAt = &t
		}
		r.Trigger = trigger.String
		r.Error = errMsg.String
		runs = append(runs, r)
	}
	return runs, rows.Err()
}
```

Add `"database/sql"` to the imports in `sqlite.go`.

**Step 4: Run all tests**

Run: `go test ./internal/meta/ -v`
Expected: ALL PASS

**Step 5: Run with race detector**

Run: `go test -race ./internal/meta/`
Expected: PASS, no races

**Step 6: Commit**

```bash
git add internal/meta/sqlite.go internal/meta/meta_test.go
git commit -m "Implement query methods on SQLiteStore"
```

---

### Task 9: Verify Store interface satisfaction

**Files:**
- Modify: `internal/meta/sqlite.go`

**Step 1: Add compile-time interface check**

Add to `internal/meta/sqlite.go` after the struct definition:

```go
var _ Store = (*SQLiteStore)(nil)
```

**Step 2: Verify it compiles**

Run: `go build ./internal/meta/`
Expected: clean build. If it fails, a method signature doesn't match — fix it.

**Step 3: Run full test suite**

Run: `go test -race ./internal/meta/ -v`
Expected: ALL PASS

**Step 4: Commit**

```bash
git add internal/meta/sqlite.go
git commit -m "Add compile-time Store interface check for SQLiteStore"
```

---

### Task 10: Add MetadataDB to PitConfig

**Files:**
- Modify: `internal/config/pit_config.go`

**Step 1: Add MetadataDB field**

Add to the `PitConfig` struct in `internal/config/pit_config.go`:

```go
MetadataDB string `toml:"metadata_db"`
```

Add to the path resolution block in `LoadPitConfig` (after the `RepoCacheDir` block):

```go
if cfg.MetadataDB != "" && !filepath.IsAbs(cfg.MetadataDB) {
	cfg.MetadataDB = filepath.Join(rootDir, cfg.MetadataDB)
}
```

**Step 2: Verify it compiles**

Run: `go build ./internal/config/`
Expected: clean

**Step 3: Run existing config tests**

Run: `go test -race ./internal/config/`
Expected: PASS (no behaviour change for existing tests)

**Step 4: Commit**

```bash
git add internal/config/pit_config.go
git commit -m "Add metadata_db field to PitConfig"
```

---

### Task 11: Add resolveMetadataDB helper and integrate store into pit run

**Files:**
- Modify: `internal/cli/root.go`
- Modify: `internal/cli/run.go`

**Step 1: Add resolveMetadataDB to root.go**

Add to `internal/cli/root.go`:

```go
// resolveMetadataDB returns the metadata database path from workspace config or the default.
func resolveMetadataDB() string {
	if workspaceCfg != nil && workspaceCfg.MetadataDB != "" {
		return workspaceCfg.MetadataDB
	}
	return filepath.Join(projectDir, "pit_metadata.db")
}
```

**Step 2: Modify run.go to open the store**

Modify `internal/cli/run.go` — add the meta store import and open/close in the RunE function. The modified RunE body becomes:

```go
RunE: func(cmd *cobra.Command, args []string) error {
	dagName, taskName, err := parseRunArg(args[0])
	if err != nil {
		return err
	}

	configs, err := config.Discover(projectDir)
	if err != nil {
		return err
	}

	cfg, ok := configs[dagName]
	if !ok {
		return fmt.Errorf("DAG %q not found (available: %s)", dagName, availableDAGs(configs))
	}

	if errs := dag.Validate(cfg, cfg.Dir()); len(errs) > 0 {
		for _, e := range errs {
			cmd.PrintErrf("ERROR: %s\n", e)
		}
		return fmt.Errorf("validation failed with %d error(s)", len(errs))
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Open metadata store
	metaStore, err := meta.Open(resolveMetadataDB())
	if err != nil {
		return fmt.Errorf("opening metadata store: %w", err)
	}
	defer metaStore.Close()

	opts := engine.ExecuteOpts{
		RunsDir:       resolveRunsDir(),
		RepoCacheDir:  resolveRepoCacheDir(),
		TaskName:      taskName,
		Verbose:       verbose,
		SecretsPath:   secretsPath,
		DBTDriver:     resolveDBTDriver(),
		KeepArtifacts: resolveKeepArtifacts(cfg.DAG.KeepArtifacts),
		MetaStore:     metaStore,
	}

	run, err := engine.Execute(ctx, cfg, opts)
	if err != nil {
		return err
	}

	if run.Status == engine.StatusFailed {
		return errRunFailed
	}

	return nil
},
```

Add `"github.com/druarnfield/pit/internal/meta"` to the imports in `run.go`.

**Step 3: Add MetaStore field to ExecuteOpts**

This will fail to compile until the engine accepts it — that's Task 12. For now, add the field to `internal/engine/executor.go` `ExecuteOpts`:

```go
MetaStore interface {
	InsertRun(r interface{}) error
} // temporary — will be replaced with meta.Store in Task 12
```

Actually, to avoid a circular dependency (engine cannot import meta), the engine should accept meta.Store via an interface defined in the engine package. Let me reconsider.

**Better approach:** Define a minimal `MetadataRecorder` interface in the engine package that `meta.SQLiteStore` satisfies. This avoids circular imports:

Add to `internal/engine/run.go`:

```go
// MetadataRecorder is an optional interface for recording run metadata.
// Implemented by meta.SQLiteStore.
type MetadataRecorder interface {
	InsertRun(r interface{}) error
	UpdateRun(id string, status string, endedAt time.Time, errMsg string) error
	InsertTaskInstance(ti interface{}) error
	UpdateTaskInstance(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error
	RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error
	RecordOutputs(runID, dagName string, outputs interface{}) error
}
```

Wait — using `interface{}` parameters defeats type safety. Better approach: define the record types in the engine package or use a separate shared types package.

**Simplest approach:** The `meta` package doesn't import `engine`, and `engine` doesn't import `meta`. The CLI layer (`cli/run.go`) imports both and wires them together. The engine accepts a recorder interface using its own parameter types.

Let me restructure. The engine should define its own recorder interface with concrete parameter types:

Add to `internal/engine/run.go`:

```go
// MetadataRecorder records run and task metadata to a persistent store.
// Pass nil to skip metadata recording.
type MetadataRecorder interface {
	RecordRunStart(id, dagName, status, runDir, trigger string, startedAt time.Time) error
	RecordRunEnd(id, status string, endedAt time.Time, errMsg string) error
	RecordTaskStart(runID, taskName, status, logPath string, startedAt time.Time) error
	RecordTaskEnd(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error
	RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error
}
```

Then `meta.SQLiteStore` gets a thin adapter, or we make `SQLiteStore` implement this interface directly via wrapper methods.

Actually, the cleanest approach: **make the meta package not depend on engine, and have the CLI bridge them.** The engine takes a simple callback-style interface with primitive parameters. The `meta.SQLiteStore` implements it via an adapter in the CLI.

Let me simplify even further. Since `meta` doesn't import `engine` and `engine` doesn't import `meta`, we can define a recorder interface in `engine` and have `meta.SQLiteStore` satisfy it by adding matching methods.

I'll revise the plan:

**Step 3 (revised): Add MetadataRecorder interface to engine/run.go**

Add to `internal/engine/run.go`:

```go
// MetadataRecorder records run and task metadata to a persistent store.
type MetadataRecorder interface {
	RecordRunStart(id, dagName, status, runDir, trigger string, startedAt time.Time) error
	RecordRunEnd(id, status string, endedAt time.Time, errMsg string) error
	RecordTaskStart(runID, taskName, status, logPath string, startedAt time.Time) error
	RecordTaskEnd(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error
	RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error
}
```

Add to `ExecuteOpts` in `internal/engine/executor.go`:

```go
MetaStore MetadataRecorder // nil = no metadata tracking
```

**Step 4: Add MetadataRecorder methods to SQLiteStore**

Add to `internal/meta/sqlite.go`:

```go
func (s *SQLiteStore) RecordRunStart(id, dagName, status, runDir, trigger string, startedAt time.Time) error {
	return s.InsertRun(RunRecord{
		ID: id, DAGName: dagName, Status: status,
		StartedAt: startedAt, RunDir: runDir, Trigger: trigger,
	})
}

func (s *SQLiteStore) RecordRunEnd(id, status string, endedAt time.Time, errMsg string) error {
	return s.UpdateRun(id, status, endedAt, errMsg)
}

func (s *SQLiteStore) RecordTaskStart(runID, taskName, status, logPath string, startedAt time.Time) error {
	return s.InsertTaskInstance(TaskInstanceRecord{
		RunID: runID, TaskName: taskName, Status: status,
		StartedAt: &startedAt, Attempts: 1, LogPath: logPath,
	})
}

func (s *SQLiteStore) RecordTaskEnd(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error {
	return s.UpdateTaskInstance(runID, taskName, status, endedAt, attempts, errMsg)
}

// RecordEnvSnapshot is already implemented and matches the interface.
```

**Step 5: Update run.go to use MetaStore**

In `internal/cli/run.go`, the `MetaStore` field on `ExecuteOpts` accepts `*meta.SQLiteStore` because it satisfies `engine.MetadataRecorder`.

**Step 6: Verify it compiles**

Run: `go build ./...`
Expected: clean build

**Step 7: Run all tests**

Run: `go test -race ./...`
Expected: PASS

**Step 8: Commit**

```bash
git add internal/engine/run.go internal/engine/executor.go internal/meta/sqlite.go internal/cli/run.go internal/cli/root.go
git commit -m "Wire metadata store into pit run via MetadataRecorder interface"
```

---

### Task 12: Instrument engine with metadata recording

**Files:**
- Modify: `internal/engine/executor.go`

**Step 1: Add metadata recording calls to Execute()**

In the `Execute` function in `internal/engine/executor.go`, add recording calls at the lifecycle points. The key changes:

After building the `run` struct (around line 115), add:

```go
// Record run start in metadata store
if opts.MetaStore != nil {
	trigger := "manual"
	if err := opts.MetaStore.RecordRunStart(run.ID, run.DAGName, string(run.Status), filepath.Dir(snapshotDir), trigger, run.StartedAt); err != nil {
		fmt.Fprintf(os.Stderr, "warning: metadata recording failed: %v\n", err)
	}
}
```

After the run status determination (around line 192, after `run.Status = StatusFailed`/`StatusSuccess`), add:

```go
// Record run end in metadata store
if opts.MetaStore != nil {
	var errMsg string
	if run.Status == StatusFailed {
		for _, ti := range run.Tasks {
			if ti.Status == StatusFailed && ti.Error != nil {
				errMsg = ti.Error.Error()
				break
			}
		}
	}
	if err := opts.MetaStore.RecordRunEnd(run.ID, string(run.Status), run.EndedAt, errMsg); err != nil {
		fmt.Fprintf(os.Stderr, "warning: metadata recording failed: %v\n", err)
	}
}
```

**Step 2: Add metadata recording to executeTask()**

In `executeTask()`, add recording calls. After setting status to `StatusRunning` (line 329-331):

```go
// Record task start
if opts.MetaStore != nil {
	logPath := filepath.Join(run.LogDir, ti.Name+".log")
	opts.MetaStore.RecordTaskStart(run.ID, ti.Name, string(StatusRunning), logPath, ti.StartedAt)
}
```

At each exit point where task status is set to final (StatusSuccess, StatusFailed), add recording. The cleanest approach: add a deferred function after the status is set to running:

Actually, the simplest approach is to add a single deferred recording call at the end of executeTask. But since the function has multiple exit points with `return`, we need to catch them all.

Better: add metadata recording right before each `return` in executeTask where status is finalized. There are several exit points:

1. After dbt config check fails (line 344-348)
2. After runner resolve fails (line 380-385)
3. After log file create fails (line 396-401)
4. After script validation fails (line 447-452)
5. After context cancellation in retry loop (line 464-469)
6. After successful task (line 489-493)
7. After all retries exhausted (line 518-521)

To avoid modifying every exit point, use a **deferred function** that records the final state:

After the initial `ti.Status = StatusRunning` block, add:

```go
// Deferred metadata recording for task completion
if opts.MetaStore != nil {
	defer func() {
		run.mu.Lock()
		status := string(ti.Status)
		endedAt := ti.EndedAt
		attempts := ti.Attempt
		var errMsg string
		if ti.Error != nil {
			errMsg = ti.Error.Error()
		}
		logPath := filepath.Join(run.LogDir, ti.Name+".log")
		run.mu.Unlock()
		opts.MetaStore.RecordTaskEnd(run.ID, ti.Name, status, endedAt, attempts, errMsg)
		_ = opts.MetaStore.RecordTaskStart(run.ID, ti.Name, status, logPath, ti.StartedAt)
	}()
}
```

Wait, that's wrong — we want to record the start first, then update on end. Let me restructure:

Record task start immediately after setting running (not deferred). Record task end with a defer:

```go
run.mu.Lock()
ti.Status = StatusRunning
ti.StartedAt = time.Now()
run.mu.Unlock()

if opts.MetaStore != nil {
	logPath := filepath.Join(run.LogDir, ti.Name+".log")
	opts.MetaStore.RecordTaskStart(run.ID, ti.Name, string(StatusRunning), logPath, ti.StartedAt)
	defer func() {
		run.mu.Lock()
		status := string(ti.Status)
		endedAt := ti.EndedAt
		attempts := ti.Attempt
		var errMsg string
		if ti.Error != nil {
			errMsg = ti.Error.Error()
		}
		run.mu.Unlock()
		opts.MetaStore.RecordTaskEnd(run.ID, ti.Name, status, endedAt, attempts, errMsg)
	}()
}
```

But note: some exit points set the final status *before* `ti.Status = StatusRunning` (the dbt config check and runner resolve checks). For those early exits, the task was never truly started — we can skip recording them, or record them as immediate failures. The simplest approach: move the task start recording and defer *after* the runner resolution, so only tasks that actually attempt execution are recorded. Tasks that fail validation won't appear in the store (which is fine — they're configuration errors, not execution results).

Actually, let's record all tasks. The initial task instances should be inserted at run start for all tasks, then updated as they complete. This matches the design doc: "Task starts → InsertTaskInstance with status running".

Revised approach: Insert all task instances at run start (in Execute, after building the run), then update them as they complete.

After building the Tasks list in Execute() (after the `for _, tc := range cfg.Tasks` loop):

```go
// Record all task instances at run start
if opts.MetaStore != nil {
	for _, ti := range run.Tasks {
		logPath := filepath.Join(logDir, ti.Name+".log")
		opts.MetaStore.RecordTaskStart(run.ID, ti.Name, string(ti.Status), logPath, time.Time{})
	}
}
```

Then in executeTask, add a defer to record the final state:

```go
// After ti.Status = StatusRunning
if opts.MetaStore != nil {
	defer func() {
		run.mu.Lock()
		status := string(ti.Status)
		endedAt := ti.EndedAt
		attempts := ti.Attempt
		var errMsg string
		if ti.Error != nil {
			errMsg = ti.Error.Error()
		}
		run.mu.Unlock()
		opts.MetaStore.RecordTaskEnd(run.ID, ti.Name, status, endedAt, attempts, errMsg)
	}()
}
```

**Step 3: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 4: Run all tests**

Run: `go test -race ./...`
Expected: PASS (MetaStore is nil in all existing tests, so no behavior change)

**Step 5: Commit**

```bash
git add internal/engine/executor.go
git commit -m "Instrument engine with metadata recording at run/task lifecycle points"
```

---

### Task 13: Add environment hash recording

**Files:**
- Modify: `internal/engine/executor.go`

**Step 1: Add hashFile helper**

Add to `internal/engine/executor.go`:

```go
// hashFile computes the SHA-256 hex digest of a file.
// Returns an empty string if the file doesn't exist.
func hashFile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
```

Add `"crypto/sha256"` to the imports.

**Step 2: Record env snapshots after snapshot creation**

In Execute(), after the Snapshot call and before building the Run struct, add:

```go
// Record environment hashes
if opts.MetaStore != nil {
	envFiles := map[string]string{
		"pit_toml":   filepath.Join(projectDir, "pit.toml"),
		"uv_lock":    filepath.Join(projectDir, "uv.lock"),
		"pyproject":  filepath.Join(projectDir, "pyproject.toml"),
	}
	for hashType, path := range envFiles {
		hash := hashFile(path)
		if hash != "" {
			opts.MetaStore.RecordEnvSnapshot(cfg.DAG.Name, hashType, hash, runID)
		}
	}
}
```

**Step 3: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 4: Run tests**

Run: `go test -race ./...`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/engine/executor.go
git commit -m "Record environment file hashes at run start"
```

---

### Task 14: Add output recording

**Files:**
- Modify: `internal/engine/executor.go`
- Modify: `internal/engine/run.go`

**Step 1: Add Outputs field to ExecuteOpts or pass through config**

The outputs come from `cfg.Outputs` (a `[]config.Output`). The engine needs to convert these to a format the MetadataRecorder can accept. Since the recorder uses `RecordEnvSnapshot` with primitive params, we need to add a similar method for outputs.

Actually, looking at the MetadataRecorder interface, it doesn't have a RecordOutputs method. Let's add one:

Add to `MetadataRecorder` in `internal/engine/run.go`:

```go
RecordOutputs(runID, dagName string, names, types, locations []string) error
```

Add to `meta.SQLiteStore`:

```go
func (s *SQLiteStore) RecordOutputs2(runID, dagName string, names, types, locations []string) error {
	var outputs []OutputRecord
	for i := range names {
		outputs = append(outputs, OutputRecord{
			Name: names[i], Type: types[i], Location: locations[i],
		})
	}
	return s.RecordOutputs(runID, dagName, outputs)
}
```

Hmm, this is getting messy with multiple method names. Let me simplify.

**Better approach:** Use the same parallel-slices pattern consistently in the MetadataRecorder interface, or just pass output info as struct-like grouped params.

Actually, the simplest solution: add a single `RecordOutput` method (singular) to MetadataRecorder:

```go
RecordOutput(runID, dagName, name, outputType, location string) error
```

Then in `meta.SQLiteStore`:

```go
func (s *SQLiteStore) RecordOutput(runID, dagName, name, outputType, location string) error {
	_, err := s.db.Exec(
		`INSERT INTO outputs (run_id, dag_name, name, type, location) VALUES (?, ?, ?, ?, ?)`,
		runID, dagName, name, nilIfEmpty(outputType), nilIfEmpty(location),
	)
	return err
}
```

**Step 2: Add RecordOutput to MetadataRecorder interface**

In `internal/engine/run.go`, add to `MetadataRecorder`:

```go
RecordOutput(runID, dagName, name, outputType, location string) error
```

**Step 3: Implement RecordOutput on SQLiteStore**

Add to `internal/meta/sqlite.go`:

```go
func (s *SQLiteStore) RecordOutput(runID, dagName, name, outputType, location string) error {
	_, err := s.db.Exec(
		`INSERT INTO outputs (run_id, dag_name, name, type, location) VALUES (?, ?, ?, ?, ?)`,
		runID, dagName, name, nilIfEmpty(outputType), nilIfEmpty(location),
	)
	return err
}
```

**Step 4: Record outputs in Execute() after successful run**

In `internal/engine/executor.go`, after determining overall run status, add:

```go
// Record outputs on successful run
if opts.MetaStore != nil && run.Status == StatusSuccess {
	for _, o := range cfg.Outputs {
		opts.MetaStore.RecordOutput(run.ID, run.DAGName, o.Name, o.Type, o.Location)
	}
}
```

**Step 5: Verify it compiles and tests pass**

Run: `go build ./... && go test -race ./...`
Expected: clean build and PASS

**Step 6: Commit**

```bash
git add internal/engine/run.go internal/engine/executor.go internal/meta/sqlite.go
git commit -m "Record declared outputs on successful DAG completion"
```

---

### Task 15: Add trigger source to serve integration

**Files:**
- Modify: `internal/engine/executor.go`
- Modify: `internal/serve/server.go`
- Modify: `internal/cli/serve.go`

**Step 1: Add Trigger field to ExecuteOpts**

In `internal/engine/executor.go`, add to `ExecuteOpts`:

```go
Trigger string // trigger source: "manual", "cron", "ftp_watch", "webhook"
```

Update the RecordRunStart call in Execute() to use `opts.Trigger` instead of hardcoded `"manual"`:

```go
trigger := opts.Trigger
if trigger == "" {
	trigger = "manual"
}
```

**Step 2: Pass trigger source from serve**

In `internal/serve/server.go`, in `handleEvent()`, set the trigger on opts before calling Execute:

```go
opts := s.opts
opts.Trigger = ev.Source
```

**Step 3: Pass MetaStore to serve**

Add `MetaStore engine.MetadataRecorder` to `serve.Options`. In `NewServer`, pass it through to `s.opts.MetaStore`.

In `internal/cli/serve.go`, open the meta store and pass it:

```go
RunE: func(cmd *cobra.Command, args []string) error {
	metaStore, err := meta.Open(resolveMetadataDB())
	if err != nil {
		return fmt.Errorf("opening metadata store: %w", err)
	}
	defer metaStore.Close()

	var wsArtifacts []string
	if workspaceCfg != nil {
		wsArtifacts = workspaceCfg.KeepArtifacts
	}
	srv, err := serve.NewServer(projectDir, secretsPath, verbose, serve.Options{
		RunsDir:            resolveRunsDir(),
		RepoCacheDir:       resolveRepoCacheDir(),
		DBTDriver:          resolveDBTDriver(),
		WorkspaceArtifacts: wsArtifacts,
		WebhookPort:        port,
		MetaStore:          metaStore,
	})
	// ...
```

Add `MetaStore` to `serve.Options`:

```go
type Options struct {
	RunsDir            string
	RepoCacheDir       string
	DBTDriver          string
	WorkspaceArtifacts []string
	WebhookPort        int
	MetaStore          engine.MetadataRecorder
}
```

In `NewServer`, set `MetaStore` on opts:

```go
opts: engine.ExecuteOpts{
	RunsDir:      srvOpts.RunsDir,
	RepoCacheDir: srvOpts.RepoCacheDir,
	Verbose:      verbose,
	SecretsPath:  secretsPath,
	DBTDriver:    srvOpts.DBTDriver,
	MetaStore:    srvOpts.MetaStore,
},
```

Add `"github.com/druarnfield/pit/internal/engine"` to serve.go imports (already there) and `"github.com/druarnfield/pit/internal/meta"` to serve.go CLI imports.

**Step 4: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 5: Run all tests**

Run: `go test -race ./...`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/engine/executor.go internal/serve/server.go internal/cli/serve.go
git commit -m "Wire metadata store and trigger source into serve mode"
```

---

### Task 16: Implement pit status command

**Files:**
- Modify: `internal/cli/status.go`

**Step 1: Implement the status command**

Replace `internal/cli/status.go`:

```go
package cli

import (
	"fmt"
	"time"

	"github.com/druarnfield/pit/internal/meta"
	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show pipeline status",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := meta.Open(resolveMetadataDB())
			if err != nil {
				return fmt.Errorf("opening metadata store: %w", err)
			}
			defer store.Close()

			runs, err := store.LatestRunPerDAG()
			if err != nil {
				return fmt.Errorf("querying status: %w", err)
			}

			if len(runs) == 0 {
				fmt.Println("No runs recorded yet.")
				return nil
			}

			fmt.Printf("%-20s %-21s %-8s %s\n", "DAG", "Last Run", "Status", "Duration")
			fmt.Printf("%-20s %-21s %-8s %s\n", "───", "────────", "──────", "────────")

			for _, r := range runs {
				var duration string
				if r.EndedAt != nil {
					duration = r.EndedAt.Sub(r.StartedAt).Round(time.Second).String()
				} else {
					duration = "running"
				}
				fmt.Printf("%-20s %-21s %-8s %s\n",
					r.DAGName,
					r.StartedAt.Local().Format("2006-01-02 15:04:05"),
					r.Status,
					duration,
				)
			}
			return nil
		},
	}
}
```

**Step 2: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 3: Run all tests**

Run: `go test -race ./...`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/cli/status.go
git commit -m "Implement pit status using metadata store"
```

---

### Task 17: Add meta package test for MetadataRecorder adapter methods

**Files:**
- Modify: `internal/meta/meta_test.go`

**Step 1: Write tests for the adapter methods**

Add to `internal/meta/meta_test.go`:

```go
func TestRecordRunStartEnd(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()

	err := s.RecordRunStart("run1", "my_dag", "running", "runs/run1", "cron", now)
	if err != nil {
		t.Fatalf("RecordRunStart: %v", err)
	}

	ended := now.Add(time.Minute)
	err = s.RecordRunEnd("run1", "success", ended, "")
	if err != nil {
		t.Fatalf("RecordRunEnd: %v", err)
	}

	run, _, err := s.RunDetail("run1")
	if err != nil {
		t.Fatalf("RunDetail: %v", err)
	}
	if run.Status != "success" {
		t.Errorf("status = %q, want %q", run.Status, "success")
	}
	if run.Trigger != "cron" {
		t.Errorf("trigger = %q, want %q", run.Trigger, "cron")
	}
}

func TestRecordTaskStartEnd(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	s.RecordRunStart("run1", "my_dag", "running", "runs/run1", "manual", now)

	err := s.RecordTaskStart("run1", "extract", "running", "runs/run1/logs/extract.log", now)
	if err != nil {
		t.Fatalf("RecordTaskStart: %v", err)
	}

	ended := now.Add(30 * time.Second)
	err = s.RecordTaskEnd("run1", "extract", "success", ended, 1, "")
	if err != nil {
		t.Fatalf("RecordTaskEnd: %v", err)
	}

	_, tasks, err := s.RunDetail("run1")
	if err != nil {
		t.Fatalf("RunDetail: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Status != "success" {
		t.Errorf("task status = %q, want %q", tasks[0].Status, "success")
	}
}

func TestRecordOutput(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	s.RecordRunStart("run1", "my_dag", "running", "runs/run1", "manual", now)

	err := s.RecordOutput("run1", "my_dag", "report", "file", "/data/report.csv")
	if err != nil {
		t.Fatalf("RecordOutput: %v", err)
	}

	outputs, err := s.OutputsByRun("run1")
	if err != nil {
		t.Fatalf("OutputsByRun: %v", err)
	}
	if len(outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outputs))
	}
}
```

**Step 2: Run tests**

Run: `go test -race ./internal/meta/ -v`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/meta/meta_test.go
git commit -m "Add tests for MetadataRecorder adapter methods"
```

---

### Task 18: Update README

**Files:**
- Modify: `README.md`

**Step 1: Update the README**

- Add `pit status` to the CLI commands table (move from "planned" to "implemented")
- Add a "Metadata Store" section explaining `pit_metadata.db`, what it tracks, and the `metadata_db` config option
- Move "SQLite metadata store" from the roadmap to the "Implemented" section

**Step 2: Commit**

```bash
git add README.md
git commit -m "Update README with metadata store documentation"
```

---

### Task 19: Final verification

**Step 1: Run full test suite with race detector**

Run: `go test -race ./...`
Expected: ALL PASS

**Step 2: Run vet**

Run: `go vet ./...`
Expected: clean

**Step 3: Build the binary**

Run: `go build -o pit ./cmd/pit`
Expected: clean build

**Step 4: Smoke test**

Run: `./pit status`
Expected: "No runs recorded yet." (clean output, no errors — proves the store opens and queries correctly)
