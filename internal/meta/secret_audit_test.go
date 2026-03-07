package meta

import (
	"testing"
	"time"
)

func TestRecordSecretEvent_Created(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer store.Close()

	err = store.RecordSecretEvent(SecretAuditRecord{
		EventType: "created",
		Project:   "global",
		SecretKey: "smtp_password",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("RecordSecretEvent() error: %v", err)
	}
}

func TestRecordSecretEvent_Accessed(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer store.Close()

	err = store.RecordSecretEvent(SecretAuditRecord{
		EventType: "accessed",
		Project:   "claims_pipeline",
		SecretKey: "claims_db",
		DAGName:   "claims_pipeline",
		TaskName:  "extract",
		RunID:     "20260308_120000.000_claims_pipeline",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("RecordSecretEvent() error: %v", err)
	}
}

func TestSecretAuditHistory(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer store.Close()

	now := time.Now()
	events := []SecretAuditRecord{
		{EventType: "created", Project: "global", SecretKey: "key_a", Timestamp: now.Add(-2 * time.Hour)},
		{EventType: "updated", Project: "global", SecretKey: "key_a", Timestamp: now.Add(-1 * time.Hour)},
		{EventType: "accessed", Project: "global", SecretKey: "key_a", DAGName: "dag1", TaskName: "task1", RunID: "run1", Timestamp: now},
	}
	for _, e := range events {
		if err := store.RecordSecretEvent(e); err != nil {
			t.Fatalf("RecordSecretEvent() error: %v", err)
		}
	}

	history, err := store.SecretAuditHistory("global", "key_a", 10)
	if err != nil {
		t.Fatalf("SecretAuditHistory() error: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	if history[0].EventType != "accessed" {
		t.Errorf("history[0].EventType = %q, want %q", history[0].EventType, "accessed")
	}
	if history[0].DAGName != "dag1" {
		t.Errorf("history[0].DAGName = %q, want %q", history[0].DAGName, "dag1")
	}
}

func TestSecretAuditHistory_Empty(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer store.Close()

	history, err := store.SecretAuditHistory("global", "nonexistent", 10)
	if err != nil {
		t.Fatalf("SecretAuditHistory() error: %v", err)
	}
	if len(history) != 0 {
		t.Errorf("len(history) = %d, want 0", len(history))
	}
}
