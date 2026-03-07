package meta

import (
	"testing"
	"time"
)

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
