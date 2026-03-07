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

	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	check(store.RecordRunStart("20260307_143000.000_dag_a", "dag_a", "success", "runs/20260307_143000.000_dag_a", "cron", now))
	check(store.RecordRunEnd("20260307_143000.000_dag_a", "success", ended, ""))

	check(store.RecordTaskStart("20260307_143000.000_dag_a", "extract", "success", "runs/20260307_143000.000_dag_a/logs/extract.log", now))
	taskEnded := now.Add(45 * time.Second)
	check(store.RecordTaskEnd("20260307_143000.000_dag_a", "extract", "success", taskEnded, 1, ""))

	check(store.RecordTaskStart("20260307_143000.000_dag_a", "load", "success", "runs/20260307_143000.000_dag_a/logs/load.log", taskEnded))
	check(store.RecordTaskEnd("20260307_143000.000_dag_a", "load", "success", ended, 1, ""))

	check(store.RecordOutput("20260307_143000.000_dag_a", "dag_a", "claims_staging", "table", "warehouse.staging.claims"))
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

func TestListDAGs(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)
	h := NewHandler(newTestConfigs(), store, "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body struct {
		DAGs []struct {
			Name      string `json:"name"`
			Schedule  string `json:"schedule"`
			TaskCount int    `json:"task_count"`
			LatestRun *struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"latest_run"`
		} `json:"dags"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(body.DAGs) != 2 {
		t.Fatalf("got %d dags, want 2", len(body.DAGs))
	}

	// Sorted alphabetically: dag_a, dag_b
	if body.DAGs[0].Name != "dag_a" {
		t.Errorf("dags[0].name = %q, want %q", body.DAGs[0].Name, "dag_a")
	}
	if body.DAGs[0].LatestRun == nil {
		t.Errorf("dag_a latest_run = nil, want non-nil")
	} else if body.DAGs[0].LatestRun.Status != "success" {
		t.Errorf("dag_a latest_run.status = %q, want %q", body.DAGs[0].LatestRun.Status, "success")
	}

	if body.DAGs[1].Name != "dag_b" {
		t.Errorf("dags[1].name = %q, want %q", body.DAGs[1].Name, "dag_b")
	}
	if body.DAGs[1].LatestRun != nil {
		t.Errorf("dag_b latest_run = %v, want nil", body.DAGs[1].LatestRun)
	}
}

func TestDAGDetail(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)
	h := NewHandler(newTestConfigs(), store, "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags/dag_a", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body struct {
		Name    string `json:"name"`
		Tasks   []struct {
			Name      string   `json:"name"`
			Script    string   `json:"script"`
			DependsOn []string `json:"depends_on"`
		} `json:"tasks"`
		RecentRuns []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"recent_runs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.Name != "dag_a" {
		t.Errorf("name = %q, want %q", body.Name, "dag_a")
	}
	if len(body.Tasks) != 2 {
		t.Errorf("got %d tasks, want 2", len(body.Tasks))
	}
	if len(body.RecentRuns) != 1 {
		t.Errorf("got %d recent_runs, want 1", len(body.RecentRuns))
	}
}

func TestDAGDetailNotFound(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags/nonexistent", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestListRuns(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)
	h := NewHandler(newTestConfigs(), store, "")

	// All runs
	req := httptest.NewRequest(http.MethodGet, "/api/runs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body struct {
		Runs []struct {
			ID      string `json:"id"`
			DAGName string `json:"dag_name"`
		} `json:"runs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Runs) != 1 {
		t.Errorf("all runs: got %d, want 1", len(body.Runs))
	}

	// Filter by dag_b (no runs)
	req = httptest.NewRequest(http.MethodGet, "/api/runs?dag=dag_b", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var body2 struct {
		Runs []struct{} `json:"runs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body2.Runs) != 0 {
		t.Errorf("dag_b runs: got %d, want 0", len(body2.Runs))
	}

	// Limit=1
	req = httptest.NewRequest(http.MethodGet, "/api/runs?limit=1", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var body3 struct {
		Runs []struct{} `json:"runs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body3); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body3.Runs) != 1 {
		t.Errorf("limit=1 runs: got %d, want 1", len(body3.Runs))
	}
}

func TestRunDetail(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)
	h := NewHandler(newTestConfigs(), store, "")

	req := httptest.NewRequest(http.MethodGet, "/api/runs/20260307_143000.000_dag_a", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body struct {
		DAGName string `json:"dag_name"`
		Status  string `json:"status"`
		Tasks   []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.DAGName != "dag_a" {
		t.Errorf("dag_name = %q, want %q", body.DAGName, "dag_a")
	}
	if len(body.Tasks) != 2 {
		t.Errorf("got %d tasks, want 2", len(body.Tasks))
	}
}

func TestRunDetailNotFound(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "")

	req := httptest.NewRequest(http.MethodGet, "/api/runs/nonexistent", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestListOutputs(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)
	h := NewHandler(newTestConfigs(), store, "")

	// All outputs
	req := httptest.NewRequest(http.MethodGet, "/api/outputs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	var body struct {
		Outputs []struct {
			DAGName  string `json:"dag_name"`
			Name     string `json:"name"`
			Type     string `json:"type"`
			Location string `json:"location"`
		} `json:"outputs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Outputs) != 1 {
		t.Errorf("got %d outputs, want 1", len(body.Outputs))
	}
	if len(body.Outputs) > 0 && body.Outputs[0].Name != "claims_staging" {
		t.Errorf("output name = %q, want %q", body.Outputs[0].Name, "claims_staging")
	}

	// Filter by dag_b (no outputs)
	req = httptest.NewRequest(http.MethodGet, "/api/outputs?dag=dag_b", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var body2 struct {
		Outputs []struct{} `json:"outputs"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body2.Outputs) != 0 {
		t.Errorf("dag_b outputs: got %d, want 0", len(body2.Outputs))
	}
}
