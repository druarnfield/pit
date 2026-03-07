package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/meta"
)

func newTestStore(t *testing.T) *meta.SQLiteStore {
	t.Helper()
	s, err := meta.Open(":memory:")
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newTestConfigs() map[string]*config.ProjectConfig {
	return map[string]*config.ProjectConfig{
		"dag_a": {
			DAG: config.DAGConfig{
				Name:     "dag_a",
				Schedule: "0 6 * * *",
				Overlap:  "skip",
			},
			Tasks: []config.TaskConfig{
				{Name: "extract", Script: "tasks/extract.py"},
				{Name: "load", Script: "tasks/load.py", DependsOn: []string{"extract"}},
			},
		},
		"dag_b": {
			DAG: config.DAGConfig{
				Name: "dag_b",
			},
			Tasks: []config.TaskConfig{
				{Name: "step1", Script: "tasks/step1.sh"},
			},
		},
	}
}

func seedTestRuns(t *testing.T, store *meta.SQLiteStore) {
	t.Helper()
	now := time.Date(2026, 3, 7, 14, 30, 0, 0, time.UTC)
	ended := now.Add(2 * time.Minute)

	store.RecordRunStart("20260307_143000.000_dag_a", "dag_a", "success", "runs/20260307_143000.000_dag_a", "cron", now)
	store.RecordRunEnd("20260307_143000.000_dag_a", "success", ended, "")

	store.RecordTaskStart("20260307_143000.000_dag_a", "extract", "success", "runs/20260307_143000.000_dag_a/logs/extract.log", now)
	taskEnded := now.Add(45 * time.Second)
	store.RecordTaskEnd("20260307_143000.000_dag_a", "extract", "success", taskEnded, 1, "")

	store.RecordTaskStart("20260307_143000.000_dag_a", "load", "success", "runs/20260307_143000.000_dag_a/logs/load.log", taskEnded)
	store.RecordTaskEnd("20260307_143000.000_dag_a", "load", "success", ended, 1, "")

	store.RecordOutput("20260307_143000.000_dag_a", "dag_a", "claims_staging", "table", "warehouse.staging.claims")
}

func TestHealth(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "")
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["status"] != "ok" {
		t.Errorf("body = %v, want status=ok", body)
	}
}

func TestAuthRequired(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "secret-token")

	// No token — should get 401
	req := httptest.NewRequest(http.MethodGet, "/api/dags", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("no token: status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	// Wrong token — should get 401
	req = httptest.NewRequest(http.MethodGet, "/api/dags", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	// Correct token — should get 200 (not 401)
	req = httptest.NewRequest(http.MethodGet, "/api/dags", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code == http.StatusUnauthorized {
		t.Error("correct token: got 401, want non-401")
	}
}

func TestAuthBypassedForHealth(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "secret-token")

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("health with token set: status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestNoAuthWhenEmpty(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code == http.StatusUnauthorized {
		t.Error("empty token: got 401, want non-401")
	}
}
