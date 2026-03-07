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

// Close closes the underlying database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) currentVersion() int {
	var v int
	err := s.db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_version").Scan(&v)
	if err != nil {
		return 0
	}
	return v
}

// nilIfEmpty returns nil for empty strings (for nullable TEXT columns).
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// InsertRun inserts a new run record into the database.
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
		endedAt, r.RunDir, nilIfEmpty(r.Trigger), nilIfEmpty(r.Error),
	)
	return err
}

// UpdateRun updates the status, ended_at, and error for an existing run.
func (s *SQLiteStore) UpdateRun(id, status string, endedAt time.Time, errMsg string) error {
	_, err := s.db.Exec(
		`UPDATE runs SET status = ?, ended_at = ?, error = ? WHERE id = ?`,
		status, endedAt.UTC().Format(time.RFC3339), nilIfEmpty(errMsg), id,
	)
	return err
}

// InsertTaskInstance inserts a new task instance record.
func (s *SQLiteStore) InsertTaskInstance(ti TaskInstanceRecord) error {
	var startedAt, endedAt *string
	if ti.StartedAt != nil {
		v := ti.StartedAt.UTC().Format(time.RFC3339)
		startedAt = &v
	}
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

// UpdateTaskInstance updates a task instance's status, ended_at, attempts, and error.
func (s *SQLiteStore) UpdateTaskInstance(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error {
	_, err := s.db.Exec(
		`UPDATE task_instances SET status = ?, ended_at = ?, attempts = ?, error = ? WHERE run_id = ? AND task_name = ?`,
		status, endedAt.UTC().Format(time.RFC3339), attempts, nilIfEmpty(errMsg), runID, taskName,
	)
	return err
}

// RecordEnvSnapshot records an environment snapshot, skipping if the latest hash matches.
func (s *SQLiteStore) RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error {
	var latest sql.NullString
	err := s.db.QueryRow(
		`SELECT hash_value FROM env_snapshots
		 WHERE dag_name = ? AND hash_type = ?
		 ORDER BY first_seen DESC LIMIT 1`,
		dagName, hashType,
	).Scan(&latest)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if latest.Valid && latest.String == hashValue {
		return nil
	}
	_, err = s.db.Exec(
		`INSERT INTO env_snapshots (dag_name, hash_type, hash_value, first_seen, run_id)
		 VALUES (?, ?, ?, ?, ?)`,
		dagName, hashType, hashValue, time.Now().UTC().Format(time.RFC3339), runID,
	)
	return err
}

// RecordOutputs batch-inserts output records in a transaction.
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

// scanRuns is a helper to execute a query and scan the results into RunRecords.
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
		if err := rows.Scan(&r.ID, &r.DAGName, &r.Status, &startedAt, &endedAt, &r.RunDir, &trigger, &errMsg); err != nil {
			return nil, err
		}
		r.StartedAt, _ = time.Parse(time.RFC3339, startedAt)
		if endedAt.Valid {
			t, _ := time.Parse(time.RFC3339, endedAt.String)
			r.EndedAt = &t
		}
		if trigger.Valid {
			r.Trigger = trigger.String
		}
		if errMsg.Valid {
			r.Error = errMsg.String
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// LatestRuns returns the most recent runs, optionally filtered by DAG name.
func (s *SQLiteStore) LatestRuns(dagName string, limit int) ([]RunRecord, error) {
	if dagName == "" {
		return s.scanRuns(
			`SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
			 FROM runs ORDER BY started_at DESC LIMIT ?`, limit)
	}
	return s.scanRuns(
		`SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
		 FROM runs WHERE dag_name = ? ORDER BY started_at DESC LIMIT ?`, dagName, limit)
}

// RunsByStatus returns runs filtered by status.
func (s *SQLiteStore) RunsByStatus(status string, limit int) ([]RunRecord, error) {
	return s.scanRuns(
		`SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
		 FROM runs WHERE status = ? ORDER BY started_at DESC LIMIT ?`, status, limit)
}

// RunDetail returns a run and its task instances, or nil,nil,nil if not found.
func (s *SQLiteStore) RunDetail(runID string) (*RunRecord, []TaskInstanceRecord, error) {
	runs, err := s.scanRuns(
		`SELECT id, dag_name, status, started_at, ended_at, run_dir, trigger_source, error
		 FROM runs WHERE id = ?`, runID)
	if err != nil {
		return nil, nil, err
	}
	if len(runs) == 0 {
		return nil, nil, nil
	}
	run := runs[0]

	rows, err := s.db.Query(
		`SELECT run_id, task_name, status, started_at, ended_at, attempts, error, log_path
		 FROM task_instances WHERE run_id = ?`, runID)
	if err != nil {
		return &run, nil, err
	}
	defer rows.Close()

	var tasks []TaskInstanceRecord
	for rows.Next() {
		var ti TaskInstanceRecord
		var startedAt, endedAt, errMsg, logPath sql.NullString
		if err := rows.Scan(&ti.RunID, &ti.TaskName, &ti.Status, &startedAt, &endedAt, &ti.Attempts, &errMsg, &logPath); err != nil {
			return &run, nil, err
		}
		if startedAt.Valid {
			t, _ := time.Parse(time.RFC3339, startedAt.String)
			ti.StartedAt = &t
		}
		if endedAt.Valid {
			t, _ := time.Parse(time.RFC3339, endedAt.String)
			ti.EndedAt = &t
		}
		if errMsg.Valid {
			ti.Error = errMsg.String
		}
		if logPath.Valid {
			ti.LogPath = logPath.String
		}
		tasks = append(tasks, ti)
	}
	return &run, tasks, rows.Err()
}

// EnvHistory returns environment snapshot history for a DAG and hash type.
func (s *SQLiteStore) EnvHistory(dagName, hashType string, limit int) ([]EnvSnapshotRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, dag_name, hash_type, hash_value, first_seen, run_id
		 FROM env_snapshots WHERE dag_name = ? AND hash_type = ?
		 ORDER BY first_seen DESC LIMIT ?`, dagName, hashType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snaps []EnvSnapshotRecord
	for rows.Next() {
		var e EnvSnapshotRecord
		var firstSeen string
		var runID sql.NullString
		if err := rows.Scan(&e.ID, &e.DAGName, &e.HashType, &e.HashValue, &firstSeen, &runID); err != nil {
			return nil, err
		}
		e.FirstSeen, _ = time.Parse(time.RFC3339, firstSeen)
		if runID.Valid {
			e.RunID = runID.String
		}
		snaps = append(snaps, e)
	}
	return snaps, rows.Err()
}

// OutputsByRun returns outputs for a given run, ordered by name.
func (s *SQLiteStore) OutputsByRun(runID string) ([]OutputRecord, error) {
	rows, err := s.db.Query(
		`SELECT run_id, dag_name, name, type, location
		 FROM outputs WHERE run_id = ? ORDER BY name`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outs []OutputRecord
	for rows.Next() {
		var o OutputRecord
		var typ, loc sql.NullString
		if err := rows.Scan(&o.RunID, &o.DAGName, &o.Name, &typ, &loc); err != nil {
			return nil, err
		}
		if typ.Valid {
			o.Type = typ.String
		}
		if loc.Valid {
			o.Location = loc.String
		}
		outs = append(outs, o)
	}
	return outs, rows.Err()
}

// LatestRunPerDAG returns the most recent run for each DAG.
func (s *SQLiteStore) LatestRunPerDAG() ([]RunRecord, error) {
	return s.scanRuns(
		`SELECT r.id, r.dag_name, r.status, r.started_at, r.ended_at, r.run_dir, r.trigger_source, r.error
		 FROM runs r
		 INNER JOIN (SELECT dag_name, MAX(started_at) AS max_started FROM runs GROUP BY dag_name) sub
		 ON r.dag_name = sub.dag_name AND r.started_at = sub.max_started
		 ORDER BY r.dag_name`)
}

// RecordRunStart implements engine.MetadataRecorder.
func (s *SQLiteStore) RecordRunStart(id, dagName, status, runDir, trigger string, startedAt time.Time) error {
	return s.InsertRun(RunRecord{
		ID: id, DAGName: dagName, Status: status,
		StartedAt: startedAt, RunDir: runDir, Trigger: trigger,
	})
}

// RecordRunEnd implements engine.MetadataRecorder.
func (s *SQLiteStore) RecordRunEnd(id, status string, endedAt time.Time, errMsg string) error {
	return s.UpdateRun(id, status, endedAt, errMsg)
}

// RecordTaskStart implements engine.MetadataRecorder.
func (s *SQLiteStore) RecordTaskStart(runID, taskName, status, logPath string, startedAt time.Time) error {
	return s.InsertTaskInstance(TaskInstanceRecord{
		RunID: runID, TaskName: taskName, Status: status,
		StartedAt: &startedAt, Attempts: 1, LogPath: logPath,
	})
}

// RecordTaskEnd implements engine.MetadataRecorder.
func (s *SQLiteStore) RecordTaskEnd(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error {
	return s.UpdateTaskInstance(runID, taskName, status, endedAt, attempts, errMsg)
}

// RecordOutput implements engine.MetadataRecorder.
func (s *SQLiteStore) RecordOutput(runID, dagName, name, outputType, location string) error {
	_, err := s.db.Exec(
		`INSERT INTO outputs (run_id, dag_name, name, type, location) VALUES (?, ?, ?, ?, ?)`,
		runID, dagName, name, nilIfEmpty(outputType), nilIfEmpty(location),
	)
	return err
}

// UpdateRunDir updates the run_dir for a given run ID.
func (s *SQLiteStore) UpdateRunDir(runID, runDir string) error {
	_, err := s.db.Exec("UPDATE runs SET run_dir = ? WHERE id = ?", runDir, runID)
	return err
}

// RecordSecretEvent inserts a secret audit record.
func (s *SQLiteStore) RecordSecretEvent(event SecretAuditRecord) error {
	_, err := s.db.Exec(
		`INSERT INTO secret_audit (event_type, project, secret_key, dag_name, task_name, run_id, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.EventType, event.Project, event.SecretKey,
		nilIfEmpty(event.DAGName), nilIfEmpty(event.TaskName), nilIfEmpty(event.RunID),
		event.Timestamp.UTC().Format(time.RFC3339),
	)
	return err
}

// SecretAuditHistory returns audit records for a specific secret, most recent first.
func (s *SQLiteStore) SecretAuditHistory(project, secretKey string, limit int) ([]SecretAuditRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, event_type, project, secret_key, dag_name, task_name, run_id, timestamp
		 FROM secret_audit WHERE project = ? AND secret_key = ?
		 ORDER BY timestamp DESC LIMIT ?`,
		project, secretKey, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []SecretAuditRecord
	for rows.Next() {
		var r SecretAuditRecord
		var ts string
		var dagName, taskName, runID sql.NullString
		if err := rows.Scan(&r.ID, &r.EventType, &r.Project, &r.SecretKey, &dagName, &taskName, &runID, &ts); err != nil {
			return nil, err
		}
		r.Timestamp, _ = time.Parse(time.RFC3339, ts)
		if dagName.Valid {
			r.DAGName = dagName.String
		}
		if taskName.Valid {
			r.TaskName = taskName.String
		}
		if runID.Valid {
			r.RunID = runID.String
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// Compile-time interface satisfaction check.
var _ Store = (*SQLiteStore)(nil)

func (s *SQLiteStore) migrate() error {
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
