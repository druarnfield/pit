package meta

import (
	"fmt"
	"testing"
	"time"
)

// newTestStore opens an in-memory SQLiteStore and registers cleanup.
func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open(:memory:) unexpected error: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// insertTestRun inserts a test run and returns its ID.
func insertTestRun(t *testing.T, s *SQLiteStore) string {
	t.Helper()
	id := fmt.Sprintf("run_%d", time.Now().UnixNano())
	err := s.InsertRun(RunRecord{
		ID:        id,
		DAGName:   "test_dag",
		Status:    "running",
		StartedAt: time.Now().UTC(),
		RunDir:    "runs/" + id,
		Trigger:   "manual",
	})
	if err != nil {
		t.Fatalf("InsertRun() unexpected error: %v", err)
	}
	return id
}

// seedRuns inserts 2 runs (dag_a success, dag_b success) with task instances.
func seedRuns(t *testing.T, s *SQLiteStore) {
	t.Helper()
	now := time.Now().UTC()
	ended := now.Add(10 * time.Second)

	runA := RunRecord{
		ID: "run_dag_a_1", DAGName: "dag_a", Status: "success",
		StartedAt: now, EndedAt: &ended, RunDir: "runs/run_dag_a_1", Trigger: "manual",
	}
	runB := RunRecord{
		ID: "run_dag_b_1", DAGName: "dag_b", Status: "success",
		StartedAt: now.Add(-time.Minute), EndedAt: &ended, RunDir: "runs/run_dag_b_1", Trigger: "schedule",
	}
	if err := s.InsertRun(runA); err != nil {
		t.Fatalf("InsertRun(dag_a): %v", err)
	}
	if err := s.InsertRun(runB); err != nil {
		t.Fatalf("InsertRun(dag_b): %v", err)
	}

	startedAt := now
	tiA := TaskInstanceRecord{
		RunID: "run_dag_a_1", TaskName: "extract", Status: "success",
		StartedAt: &startedAt, EndedAt: &ended, Attempts: 1,
	}
	tiB := TaskInstanceRecord{
		RunID: "run_dag_b_1", TaskName: "load", Status: "success",
		StartedAt: &startedAt, EndedAt: &ended, Attempts: 1,
	}
	if err := s.InsertTaskInstance(tiA); err != nil {
		t.Fatalf("InsertTaskInstance(extract): %v", err)
	}
	if err := s.InsertTaskInstance(tiB); err != nil {
		t.Fatalf("InsertTaskInstance(load): %v", err)
	}

	// Record an env snapshot and an output for dag_a
	if err := s.RecordEnvSnapshot("dag_a", "requirements", "abc123", "run_dag_a_1"); err != nil {
		t.Fatalf("RecordEnvSnapshot: %v", err)
	}
	if err := s.RecordEnvSnapshot("dag_a", "requirements", "def456", "run_dag_a_1"); err != nil {
		t.Fatalf("RecordEnvSnapshot(2): %v", err)
	}
	if err := s.RecordOutputs("run_dag_a_1", "dag_a", []OutputRecord{
		{Name: "report.csv", Type: "file", Location: "/tmp/report.csv"},
	}); err != nil {
		t.Fatalf("RecordOutputs: %v", err)
	}
}

func TestRunRecordFields(t *testing.T) {
	now := time.Now()
	r := RunRecord{
		ID: "20260307_143000.000_test_dag", DAGName: "test_dag",
		Status: "running", StartedAt: now,
		RunDir: "runs/20260307_143000.000_test_dag", Trigger: "manual",
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
		RunID: "20260307_143000.000_test_dag", TaskName: "extract",
		Status: "pending", Attempts: 0,
		LogPath: "runs/20260307_143000.000_test_dag/logs/extract.log",
	}
	if ti.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}
}

func TestOpenMemory(t *testing.T) {
	s := newTestStore(t)
	_ = s
}

func TestOpenTwice(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.db"

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

// Task 5 tests

func TestInsertRun(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	err := s.InsertRun(RunRecord{
		ID: "run1", DAGName: "my_dag", Status: "running",
		StartedAt: now, RunDir: "runs/run1", Trigger: "manual",
	})
	if err != nil {
		t.Fatalf("InsertRun() unexpected error: %v", err)
	}

	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM runs").Scan(&count); err != nil {
		t.Fatalf("counting runs: %v", err)
	}
	if count != 1 {
		t.Errorf("runs count = %d, want 1", count)
	}
}

func TestUpdateRun(t *testing.T) {
	s := newTestStore(t)
	id := insertTestRun(t, s)

	ended := time.Now().UTC()
	if err := s.UpdateRun(id, "success", ended, ""); err != nil {
		t.Fatalf("UpdateRun() unexpected error: %v", err)
	}

	var status string
	if err := s.db.QueryRow("SELECT status FROM runs WHERE id = ?", id).Scan(&status); err != nil {
		t.Fatalf("querying status: %v", err)
	}
	if status != "success" {
		t.Errorf("status = %q, want %q", status, "success")
	}
}

func TestInsertRunDuplicate(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	r := RunRecord{
		ID: "dup_run", DAGName: "my_dag", Status: "running",
		StartedAt: now, RunDir: "runs/dup_run",
	}
	if err := s.InsertRun(r); err != nil {
		t.Fatalf("first InsertRun() unexpected error: %v", err)
	}
	err := s.InsertRun(r)
	if err == nil {
		t.Errorf("second InsertRun() expected error, got nil")
	}
}

// Task 6 tests

func TestInsertTaskInstance(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	now := time.Now().UTC()
	err := s.InsertTaskInstance(TaskInstanceRecord{
		RunID: runID, TaskName: "extract", Status: "running",
		StartedAt: &now, Attempts: 1,
	})
	if err != nil {
		t.Fatalf("InsertTaskInstance() unexpected error: %v", err)
	}

	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM task_instances").Scan(&count); err != nil {
		t.Fatalf("counting task_instances: %v", err)
	}
	if count != 1 {
		t.Errorf("task_instances count = %d, want 1", count)
	}
}

func TestUpdateTaskInstance(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	now := time.Now().UTC()
	err := s.InsertTaskInstance(TaskInstanceRecord{
		RunID: runID, TaskName: "extract", Status: "running",
		StartedAt: &now, Attempts: 1,
	})
	if err != nil {
		t.Fatalf("InsertTaskInstance() unexpected error: %v", err)
	}

	ended := time.Now().UTC()
	if err := s.UpdateTaskInstance(runID, "extract", "success", ended, 1, ""); err != nil {
		t.Fatalf("UpdateTaskInstance() unexpected error: %v", err)
	}

	var status string
	if err := s.db.QueryRow("SELECT status FROM task_instances WHERE run_id = ? AND task_name = ?", runID, "extract").Scan(&status); err != nil {
		t.Fatalf("querying status: %v", err)
	}
	if status != "success" {
		t.Errorf("status = %q, want %q", status, "success")
	}
}

// Task 7 tests

func TestRecordEnvSnapshot(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	// First insert
	if err := s.RecordEnvSnapshot("my_dag", "requirements", "hash1", runID); err != nil {
		t.Fatalf("RecordEnvSnapshot(1) unexpected error: %v", err)
	}
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM env_snapshots").Scan(&count)
	if count != 1 {
		t.Errorf("after first insert: count = %d, want 1", count)
	}

	// Same hash again — should skip
	if err := s.RecordEnvSnapshot("my_dag", "requirements", "hash1", runID); err != nil {
		t.Fatalf("RecordEnvSnapshot(2) unexpected error: %v", err)
	}
	s.db.QueryRow("SELECT COUNT(*) FROM env_snapshots").Scan(&count)
	if count != 1 {
		t.Errorf("after same hash: count = %d, want 1", count)
	}

	// Different hash — should insert
	if err := s.RecordEnvSnapshot("my_dag", "requirements", "hash2", runID); err != nil {
		t.Fatalf("RecordEnvSnapshot(3) unexpected error: %v", err)
	}
	s.db.QueryRow("SELECT COUNT(*) FROM env_snapshots").Scan(&count)
	if count != 2 {
		t.Errorf("after different hash: count = %d, want 2", count)
	}
}

func TestRecordOutputs(t *testing.T) {
	s := newTestStore(t)
	runID := insertTestRun(t, s)

	outputs := []OutputRecord{
		{Name: "file1.csv", Type: "file", Location: "/tmp/file1.csv"},
		{Name: "file2.csv", Type: "file", Location: "/tmp/file2.csv"},
	}
	if err := s.RecordOutputs(runID, "test_dag", outputs); err != nil {
		t.Fatalf("RecordOutputs() unexpected error: %v", err)
	}

	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM outputs").Scan(&count)
	if count != 2 {
		t.Errorf("outputs count = %d, want 2", count)
	}
}

func TestRecordOutputsEmpty(t *testing.T) {
	s := newTestStore(t)
	if err := s.RecordOutputs("any", "any", nil); err != nil {
		t.Fatalf("RecordOutputs(nil) unexpected error: %v", err)
	}
}

// Task 8 tests

func TestLatestRuns(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.LatestRuns("dag_a", 10)
	if err != nil {
		t.Fatalf("LatestRuns(dag_a) unexpected error: %v", err)
	}
	if len(runs) != 1 {
		t.Errorf("LatestRuns(dag_a) returned %d runs, want 1", len(runs))
	}
}

func TestLatestRunsAllDAGs(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.LatestRuns("", 10)
	if err != nil {
		t.Fatalf("LatestRuns('') unexpected error: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("LatestRuns('') returned %d runs, want 2", len(runs))
	}
}

func TestRunsByStatus(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.RunsByStatus("success", 10)
	if err != nil {
		t.Fatalf("RunsByStatus(success) unexpected error: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("RunsByStatus(success) returned %d runs, want 2", len(runs))
	}

	runs, err = s.RunsByStatus("failed", 10)
	if err != nil {
		t.Fatalf("RunsByStatus(failed) unexpected error: %v", err)
	}
	if len(runs) != 0 {
		t.Errorf("RunsByStatus(failed) returned %d runs, want 0", len(runs))
	}
}

func TestRunDetail(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	run, tasks, err := s.RunDetail("run_dag_a_1")
	if err != nil {
		t.Fatalf("RunDetail() unexpected error: %v", err)
	}
	if run == nil {
		t.Fatal("RunDetail() returned nil run")
	}
	if run.ID != "run_dag_a_1" {
		t.Errorf("run.ID = %q, want %q", run.ID, "run_dag_a_1")
	}
	if len(tasks) != 1 {
		t.Errorf("RunDetail() returned %d tasks, want 1", len(tasks))
	}
	if len(tasks) > 0 && tasks[0].TaskName != "extract" {
		t.Errorf("tasks[0].TaskName = %q, want %q", tasks[0].TaskName, "extract")
	}
}

func TestRunDetailNotFound(t *testing.T) {
	s := newTestStore(t)

	run, tasks, err := s.RunDetail("nonexistent")
	if err != nil {
		t.Fatalf("RunDetail(nonexistent) unexpected error: %v", err)
	}
	if run != nil {
		t.Errorf("expected nil run, got %+v", run)
	}
	if tasks != nil {
		t.Errorf("expected nil tasks, got %+v", tasks)
	}
}

func TestEnvHistory(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	snaps, err := s.EnvHistory("dag_a", "requirements", 10)
	if err != nil {
		t.Fatalf("EnvHistory() unexpected error: %v", err)
	}
	if len(snaps) != 2 {
		t.Errorf("EnvHistory() returned %d snapshots, want 2", len(snaps))
	}
	// Verify both hashes are present
	if len(snaps) >= 2 {
		hashes := map[string]bool{snaps[0].HashValue: true, snaps[1].HashValue: true}
		if !hashes["abc123"] || !hashes["def456"] {
			t.Errorf("expected hashes abc123 and def456, got %q and %q", snaps[0].HashValue, snaps[1].HashValue)
		}
	}
}

func TestOutputsByRun(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	outs, err := s.OutputsByRun("run_dag_a_1")
	if err != nil {
		t.Fatalf("OutputsByRun() unexpected error: %v", err)
	}
	if len(outs) != 1 {
		t.Errorf("OutputsByRun() returned %d outputs, want 1", len(outs))
	}
}

func TestLatestRunPerDAG(t *testing.T) {
	s := newTestStore(t)
	seedRuns(t, s)

	runs, err := s.LatestRunPerDAG()
	if err != nil {
		t.Fatalf("LatestRunPerDAG() unexpected error: %v", err)
	}
	if len(runs) != 2 {
		t.Errorf("LatestRunPerDAG() returned %d runs, want 2", len(runs))
	}
}
