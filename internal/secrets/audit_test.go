package secrets

import (
	"sync"
	"testing"
)

func TestAuditFunc_CalledOnResolve(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	var mu sync.Mutex
	var events []AuditEvent

	store.OnAccess = func(e AuditEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}

	_, err = store.Resolve("claims_pipeline", "claims_db")
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Project != "claims_pipeline" {
		t.Errorf("events[0].Project = %q, want %q", events[0].Project, "claims_pipeline")
	}
	if events[0].Key != "claims_db" {
		t.Errorf("events[0].Key = %q, want %q", events[0].Key, "claims_db")
	}
}

func TestAuditFunc_CalledOnResolveField(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	var events []AuditEvent
	store.OnAccess = func(e AuditEvent) {
		events = append(events, e)
	}

	_, err = store.ResolveField("claims_pipeline", "ftp_creds", "host")
	if err != nil {
		t.Fatalf("ResolveField() error: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Key != "ftp_creds" {
		t.Errorf("events[0].Key = %q, want %q", events[0].Key, "ftp_creds")
	}
}

func TestAuditFunc_NotCalledOnError(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	var events []AuditEvent
	store.OnAccess = func(e AuditEvent) {
		events = append(events, e)
	}

	_, _ = store.Resolve("claims_pipeline", "nonexistent")

	if len(events) != 0 {
		t.Errorf("len(events) = %d, want 0 (should not fire on error)", len(events))
	}
}

func TestAuditFunc_NilIsOK(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	// OnAccess is nil — should not panic
	_, err = store.Resolve("claims_pipeline", "claims_db")
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
}
