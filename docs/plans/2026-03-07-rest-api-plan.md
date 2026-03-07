# REST API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a read-only REST API to `pit serve` exposing DAG config, run history, task instances, and outputs over HTTP.

**Architecture:** New `internal/api` package with `NewHandler()` returning an `http.Handler`. Mounted on the existing serve HTTP mux alongside webhooks. Uses `meta.Store` for queries and `config.ProjectConfig` map for DAG config. Optional bearer token auth via middleware.

**Tech Stack:** `net/http` stdlib, `encoding/json`, `crypto/subtle` for auth, `net/http/httptest` for tests

---

### Task 1: Add APIToken to PitConfig

**Files:**
- Modify: `internal/config/pit_config.go`

**Step 1: Add the field**

Add `APIToken string \`toml:"api_token"\`` to the `PitConfig` struct (after `MetadataDB`).

No path resolution needed — it's a plain string, not a file path.

**Step 2: Verify it compiles**

Run: `go build ./internal/config/`
Expected: clean

**Step 3: Run existing tests**

Run: `go test -race ./internal/config/`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/config/pit_config.go
git commit -m "Add api_token field to PitConfig"
```

---

### Task 2: Add resolveAPIToken helper and pass through serve

**Files:**
- Modify: `internal/cli/root.go`
- Modify: `internal/cli/serve.go`
- Modify: `internal/serve/server.go`

**Step 1: Add resolveAPIToken to root.go**

Add after `resolveMetadataDB()`:

```go
// resolveAPIToken returns the API bearer token from workspace config (empty = no auth).
func resolveAPIToken() string {
	if workspaceCfg != nil {
		return workspaceCfg.APIToken
	}
	return ""
}
```

**Step 2: Add APIToken to serve.Options**

In `internal/serve/server.go`, add to the `Options` struct:

```go
APIToken string // optional bearer token for /api/ endpoints (empty = no auth)
```

Add `apiToken` field to the `Server` struct:

```go
apiToken string
```

In `NewServer`, save the token:

```go
s.apiToken = srvOpts.APIToken
```

(Add this line right after `s.workspaceArtifacts = srvOpts.WorkspaceArtifacts`.)

**Step 3: Pass APIToken from serve.go CLI**

In `internal/cli/serve.go`, add `APIToken: resolveAPIToken(),` to the `serve.Options` struct literal.

**Step 4: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 5: Commit**

```bash
git add internal/cli/root.go internal/cli/serve.go internal/serve/server.go
git commit -m "Add API token config and pass through serve options"
```

---

### Task 3: Create api package — NewHandler with health endpoint

**Files:**
- Create: `internal/api/server.go`
- Create: `internal/api/api_test.go`

**Step 1: Write the test**

Create `internal/api/api_test.go`:

```go
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
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/api/`
Expected: FAIL — package does not exist

**Step 3: Create server.go**

Create `internal/api/server.go`:

```go
package api

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/meta"
)

type handler struct {
	configs map[string]*config.ProjectConfig
	store   meta.Store
	token   string
}

// NewHandler returns an http.Handler for the /api/ routes.
func NewHandler(configs map[string]*config.ProjectConfig, store meta.Store, token string) http.Handler {
	h := &handler{configs: configs, store: store, token: token}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", h.handleHealth)

	// Wrap with auth middleware (skips /api/health)
	return h.authMiddleware(mux)
}

func (h *handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.token != "" && r.URL.Path != "/api/health" {
			authHeader := r.Header.Get("Authorization")
			var provided string
			if strings.HasPrefix(authHeader, "Bearer ") {
				provided = authHeader[len("Bearer "):]
			}
			if subtle.ConstantTimeCompare([]byte(provided), []byte(h.token)) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
```

**Step 4: Run test**

Run: `go test ./internal/api/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/api/server.go internal/api/api_test.go
git commit -m "Add api package with NewHandler, health endpoint, and auth middleware"
```

---

### Task 4: Add auth tests

**Files:**
- Modify: `internal/api/api_test.go`

**Step 1: Write auth tests**

Add to `api_test.go`:

```go
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

	// Correct token — should get 200 (or 404, not 401)
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
```

**Step 2: Run tests — some may fail until dags handler is added**

The `/api/dags` route doesn't exist yet so `TestAuthRequired` correct-token case might get 404 and `TestNoAuthWhenEmpty` might get 404. That's fine — the test just checks it's NOT 401. But we need the route registered. We'll add a stub in server.go.

Add to `NewHandler` in `server.go` before the return:

```go
mux.HandleFunc("GET /api/dags", h.handleListDAGs)
mux.HandleFunc("GET /api/dags/{name}", h.handleDAGDetail)
mux.HandleFunc("GET /api/runs", h.handleListRuns)
mux.HandleFunc("GET /api/runs/{id}", h.handleRunDetail)
mux.HandleFunc("GET /api/outputs", h.handleListOutputs)
```

Add stub handlers to server.go:

```go
func (h *handler) handleListDAGs(w http.ResponseWriter, r *http.Request)  { writeJSON(w, http.StatusOK, nil) }
func (h *handler) handleDAGDetail(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, nil) }
func (h *handler) handleListRuns(w http.ResponseWriter, r *http.Request)  { writeJSON(w, http.StatusOK, nil) }
func (h *handler) handleRunDetail(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, nil) }
func (h *handler) handleListOutputs(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, nil) }
```

**Step 3: Run tests**

Run: `go test ./internal/api/ -v`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/api/server.go internal/api/api_test.go
git commit -m "Add auth tests and stub route handlers"
```

---

### Task 5: Implement GET /api/dags

**Files:**
- Create: `internal/api/handlers.go`
- Modify: `internal/api/server.go` (remove stub)
- Modify: `internal/api/api_test.go`

**Step 1: Write test**

Add to `api_test.go`:

```go
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
		t.Fatalf("expected 2 DAGs, got %d", len(body.DAGs))
	}

	// Find dag_a — should have a latest_run
	var foundA, foundB bool
	for _, d := range body.DAGs {
		if d.Name == "dag_a" {
			foundA = true
			if d.TaskCount != 2 {
				t.Errorf("dag_a task_count = %d, want 2", d.TaskCount)
			}
			if d.Schedule != "0 6 * * *" {
				t.Errorf("dag_a schedule = %q, want %q", d.Schedule, "0 6 * * *")
			}
			if d.LatestRun == nil {
				t.Error("dag_a latest_run is nil, want non-nil")
			} else if d.LatestRun.Status != "success" {
				t.Errorf("dag_a latest_run status = %q, want %q", d.LatestRun.Status, "success")
			}
		}
		if d.Name == "dag_b" {
			foundB = true
			if d.LatestRun != nil {
				t.Error("dag_b latest_run should be nil (no runs)")
			}
		}
	}
	if !foundA {
		t.Error("dag_a not found in response")
	}
	if !foundB {
		t.Error("dag_b not found in response")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/api/ -run TestListDAGs -v`
Expected: FAIL — stub returns null

**Step 3: Create handlers.go and implement handleListDAGs**

Create `internal/api/handlers.go`:

```go
package api

import (
	"net/http"
	"sort"
	"strconv"
	"time"
)

type runJSON struct {
	ID        string  `json:"id"`
	DAGName   string  `json:"dag_name,omitempty"`
	Status    string  `json:"status"`
	StartedAt string  `json:"started_at"`
	EndedAt   *string `json:"ended_at"`
	Trigger   string  `json:"trigger"`
	Error     *string `json:"error"`
}

type taskJSON struct {
	Name      string  `json:"name"`
	Status    string  `json:"status"`
	StartedAt *string `json:"started_at"`
	EndedAt   *string `json:"ended_at"`
	Attempts  int     `json:"attempts"`
	Error     *string `json:"error"`
}

func timeStr(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func timePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

func nilStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func parseLimit(r *http.Request, defaultVal, maxVal int) int {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return defaultVal
	}
	if n > maxVal {
		return maxVal
	}
	return n
}

func (h *handler) handleListDAGs(w http.ResponseWriter, r *http.Request) {
	// Get latest run per DAG from the store
	latestRuns, err := h.store.LatestRunPerDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "querying runs: "+err.Error())
		return
	}

	runsByDAG := make(map[string]*runJSON, len(latestRuns))
	for _, r := range latestRuns {
		rj := &runJSON{
			ID:        r.ID,
			Status:    r.Status,
			StartedAt: timeStr(r.StartedAt),
			EndedAt:   timePtr(r.EndedAt),
			Trigger:   r.Trigger,
		}
		runsByDAG[r.DAGName] = rj
	}

	type dagItem struct {
		Name      string   `json:"name"`
		Schedule  string   `json:"schedule"`
		TaskCount int      `json:"task_count"`
		LatestRun *runJSON `json:"latest_run"`
	}

	// Build sorted list from configs
	names := make([]string, 0, len(h.configs))
	for name := range h.configs {
		names = append(names, name)
	}
	sort.Strings(names)

	dags := make([]dagItem, 0, len(names))
	for _, name := range names {
		cfg := h.configs[name]
		dags = append(dags, dagItem{
			Name:      name,
			Schedule:  cfg.DAG.Schedule,
			TaskCount: len(cfg.Tasks),
			LatestRun: runsByDAG[name],
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"dags": dags})
}
```

Remove the `handleListDAGs` stub from `server.go`.

**Step 4: Run test**

Run: `go test ./internal/api/ -run TestListDAGs -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/api/handlers.go internal/api/server.go internal/api/api_test.go
git commit -m "Implement GET /api/dags endpoint"
```

---

### Task 6: Implement GET /api/dags/{name}

**Files:**
- Modify: `internal/api/handlers.go`
- Modify: `internal/api/server.go` (remove stub)
- Modify: `internal/api/api_test.go`

**Step 1: Write tests**

Add to `api_test.go`:

```go
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
		Schedule string `json:"schedule"`
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
		t.Errorf("expected 2 tasks, got %d", len(body.Tasks))
	}
	if len(body.RecentRuns) != 1 {
		t.Errorf("expected 1 recent run, got %d", len(body.RecentRuns))
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
```

**Step 2: Implement handleDAGDetail**

Add to `handlers.go`:

```go
func (h *handler) handleDAGDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg, ok := h.configs[name]
	if !ok {
		writeError(w, http.StatusNotFound, "DAG \""+name+"\" not found")
		return
	}

	type taskItem struct {
		Name      string   `json:"name"`
		Script    string   `json:"script"`
		DependsOn []string `json:"depends_on"`
	}

	tasks := make([]taskItem, 0, len(cfg.Tasks))
	for _, tc := range cfg.Tasks {
		deps := tc.DependsOn
		if deps == nil {
			deps = []string{}
		}
		tasks = append(tasks, taskItem{
			Name:      tc.Name,
			Script:    tc.Script,
			DependsOn: deps,
		})
	}

	// Get recent runs
	runs, err := h.store.LatestRuns(name, 10)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "querying runs: "+err.Error())
		return
	}

	recentRuns := make([]runJSON, 0, len(runs))
	for _, r := range runs {
		recentRuns = append(recentRuns, runJSON{
			ID:        r.ID,
			Status:    r.Status,
			StartedAt: timeStr(r.StartedAt),
			EndedAt:   timePtr(r.EndedAt),
			Trigger:   r.Trigger,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name":        name,
		"schedule":    cfg.DAG.Schedule,
		"overlap":     cfg.DAG.Overlap,
		"timeout":     cfg.DAG.Timeout.Duration.String(),
		"tasks":       tasks,
		"recent_runs": recentRuns,
	})
}
```

Remove the `handleDAGDetail` stub from `server.go`.

**Step 3: Run tests**

Run: `go test ./internal/api/ -run "TestDAGDetail" -v`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/api/handlers.go internal/api/server.go internal/api/api_test.go
git commit -m "Implement GET /api/dags/{name} endpoint"
```

---

### Task 7: Implement GET /api/runs and GET /api/runs/{id}

**Files:**
- Modify: `internal/api/handlers.go`
- Modify: `internal/api/server.go` (remove stubs)
- Modify: `internal/api/api_test.go`

**Step 1: Write tests**

Add to `api_test.go`:

```go
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
		Runs []runJSON `json:"runs"`
	}
	json.NewDecoder(w.Body).Decode(&body)
	if len(body.Runs) != 1 {
		t.Errorf("expected 1 run, got %d", len(body.Runs))
	}

	// Filter by DAG
	req = httptest.NewRequest(http.MethodGet, "/api/runs?dag=dag_b", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	json.NewDecoder(w.Body).Decode(&body)
	if len(body.Runs) != 0 {
		t.Errorf("expected 0 runs for dag_b, got %d", len(body.Runs))
	}

	// Limit
	req = httptest.NewRequest(http.MethodGet, "/api/runs?limit=1", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	json.NewDecoder(w.Body).Decode(&body)
	if len(body.Runs) > 1 {
		t.Errorf("expected at most 1 run with limit=1, got %d", len(body.Runs))
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
		ID      string     `json:"id"`
		DAGName string     `json:"dag_name"`
		Tasks   []taskJSON `json:"tasks"`
	}
	json.NewDecoder(w.Body).Decode(&body)

	if body.DAGName != "dag_a" {
		t.Errorf("dag_name = %q, want %q", body.DAGName, "dag_a")
	}
	if len(body.Tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(body.Tasks))
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
```

**Step 2: Implement handlers**

Add to `handlers.go`:

```go
func (h *handler) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 20, 100)
	dagName := r.URL.Query().Get("dag")

	runs, err := h.store.LatestRuns(dagName, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "querying runs: "+err.Error())
		return
	}

	result := make([]runJSON, 0, len(runs))
	for _, r := range runs {
		result = append(result, runJSON{
			ID:        r.ID,
			DAGName:   r.DAGName,
			Status:    r.Status,
			StartedAt: timeStr(r.StartedAt),
			EndedAt:   timePtr(r.EndedAt),
			Trigger:   r.Trigger,
			Error:     nilStr(r.Error),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"runs": result})
}

func (h *handler) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	run, taskInstances, err := h.store.RunDetail(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "querying run: "+err.Error())
		return
	}
	if run == nil {
		writeError(w, http.StatusNotFound, "run \""+id+"\" not found")
		return
	}

	tasks := make([]taskJSON, 0, len(taskInstances))
	for _, ti := range taskInstances {
		tasks = append(tasks, taskJSON{
			Name:      ti.TaskName,
			Status:    ti.Status,
			StartedAt: timePtr(ti.StartedAt),
			EndedAt:   timePtr(ti.EndedAt),
			Attempts:  ti.Attempts,
			Error:     nilStr(ti.Error),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":         run.ID,
		"dag_name":   run.DAGName,
		"status":     run.Status,
		"started_at": timeStr(run.StartedAt),
		"ended_at":   timePtr(run.EndedAt),
		"trigger":    run.Trigger,
		"error":      nilStr(run.Error),
		"tasks":      tasks,
	})
}
```

Remove the `handleListRuns` and `handleRunDetail` stubs from `server.go`.

**Step 3: Run tests**

Run: `go test ./internal/api/ -run "TestListRuns|TestRunDetail" -v`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/api/handlers.go internal/api/server.go internal/api/api_test.go
git commit -m "Implement GET /api/runs and GET /api/runs/{id} endpoints"
```

---

### Task 8: Implement GET /api/outputs

**Files:**
- Modify: `internal/api/handlers.go`
- Modify: `internal/api/server.go` (remove stub)
- Modify: `internal/api/api_test.go`

**Step 1: Write test**

Add to `api_test.go`:

```go
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
	json.NewDecoder(w.Body).Decode(&body)
	if len(body.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(body.Outputs))
	}
	if body.Outputs[0].Name != "claims_staging" {
		t.Errorf("output name = %q, want %q", body.Outputs[0].Name, "claims_staging")
	}

	// Filter by DAG
	req = httptest.NewRequest(http.MethodGet, "/api/outputs?dag=dag_b", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	json.NewDecoder(w.Body).Decode(&body)
	if len(body.Outputs) != 0 {
		t.Errorf("expected 0 outputs for dag_b, got %d", len(body.Outputs))
	}
}
```

**Step 2: Implement handleListOutputs**

The store has `OutputsByRun(runID)` but not "outputs by DAG from latest successful run." We need to compose: get latest successful run per DAG, then get outputs for those runs.

Add to `handlers.go`:

```go
func (h *handler) handleListOutputs(w http.ResponseWriter, r *http.Request) {
	dagFilter := r.URL.Query().Get("dag")

	// Get latest successful runs to find outputs
	latestRuns, err := h.store.LatestRunPerDAG()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "querying runs: "+err.Error())
		return
	}

	type outputJSON struct {
		DAGName  string `json:"dag_name"`
		Name     string `json:"name"`
		Type     string `json:"type"`
		Location string `json:"location"`
	}

	var allOutputs []outputJSON
	for _, run := range latestRuns {
		if run.Status != "success" {
			continue
		}
		if dagFilter != "" && run.DAGName != dagFilter {
			continue
		}
		outputs, err := h.store.OutputsByRun(run.ID)
		if err != nil {
			continue
		}
		for _, o := range outputs {
			allOutputs = append(allOutputs, outputJSON{
				DAGName:  o.DAGName,
				Name:     o.Name,
				Type:     o.Type,
				Location: o.Location,
			})
		}
	}

	if allOutputs == nil {
		allOutputs = []outputJSON{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"outputs": allOutputs})
}
```

Remove the `handleListOutputs` stub from `server.go`.

**Step 3: Run test**

Run: `go test ./internal/api/ -run TestListOutputs -v`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/api/handlers.go internal/api/server.go internal/api/api_test.go
git commit -m "Implement GET /api/outputs endpoint"
```

---

### Task 9: Mount API handler in serve and make HTTP server unconditional

**Files:**
- Modify: `internal/serve/server.go`

**Step 1: Add apiHandler field to Server and create it in NewServer**

Add to the `Server` struct:

```go
apiHandler http.Handler
```

In `NewServer`, after saving `s.apiToken`, add:

```go
if srvOpts.MetaStore != nil {
	s.apiHandler = api.NewHandler(configs, srvOpts.MetaStore, srvOpts.APIToken)
}
```

Note: `MetaStore` on `serve.Options` is `engine.MetadataRecorder` but `api.NewHandler` needs `meta.Store`. We need to pass the `meta.Store` directly. Add a new field to `serve.Options`:

```go
MetaQueryStore meta.Store // for API query endpoints (can be same instance as MetaStore)
```

And in `NewServer`:

```go
if srvOpts.MetaQueryStore != nil {
	s.apiHandler = api.NewHandler(configs, srvOpts.MetaQueryStore, srvOpts.APIToken)
}
```

Add import `"github.com/druarnfield/pit/internal/api"` and `"github.com/druarnfield/pit/internal/meta"` to `server.go`.

**Step 2: Refactor Start() to always start HTTP server**

In `Start()`, replace the conditional webhook server block (lines 164-186) with an unconditional HTTP server that includes both webhook and API routes:

```go
// Start HTTP server (API + webhooks)
mux := http.NewServeMux()
if s.apiHandler != nil {
	mux.Handle("/api/", s.apiHandler)
}
if len(s.webhookTokens) > 0 {
	mux.HandleFunc("/webhook/", s.webhookHandler)
}

httpSrv := &http.Server{
	Addr:    fmt.Sprintf(":%d", s.webhookPort),
	Handler: mux,
}
triggerWg.Add(1)
go func() {
	defer triggerWg.Done()
	log.Printf("pit serve: HTTP server on :%d", s.webhookPort)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("HTTP server error: %v", err)
	}
}()
go func() {
	<-triggerCtx.Done()
	if err := httpSrv.Shutdown(context.Background()); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
}()
```

**Step 3: Remove the trigger-only check**

The existing check `if len(s.triggers) == 0 && len(s.webhookTokens) == 0` returns an error. With the API, `pit serve` should work even with no triggers (just serving the API). Change this to a warning:

```go
if len(s.triggers) == 0 && len(s.webhookTokens) == 0 {
	log.Println("warning: no triggers registered (API-only mode)")
}
```

**Step 4: Update serve.go CLI to pass MetaQueryStore**

In `internal/cli/serve.go`, pass the meta store as both `MetaStore` and `MetaQueryStore`:

```go
srv, err := serve.NewServer(projectDir, secretsPath, verbose, serve.Options{
	RunsDir:            resolveRunsDir(),
	RepoCacheDir:       resolveRepoCacheDir(),
	DBTDriver:          resolveDBTDriver(),
	WorkspaceArtifacts: wsArtifacts,
	WebhookPort:        port,
	MetaStore:          metaStore,
	MetaQueryStore:     metaStore,
	APIToken:           resolveAPIToken(),
})
```

**Step 5: Verify it compiles**

Run: `go build ./...`
Expected: clean

**Step 6: Run all tests**

Run: `go test -race ./internal/api/ ./internal/serve/ ./internal/cli/`
Expected: PASS

**Step 7: Commit**

```bash
git add internal/serve/server.go internal/cli/serve.go
git commit -m "Mount API handler in serve, make HTTP server unconditional"
```

---

### Task 10: Update README

**Files:**
- Modify: `README.md`

**Step 1: Add REST API section**

Add a "REST API" section to `README.md` (after the "Metadata Store" section) documenting:
- The 6 endpoints with example curl commands
- Query parameters (`?limit`, `?dag`)
- Authentication via `api_token` in `pit_config.toml`
- That the API shares the `--port` with webhooks
- Example response for `GET /api/dags`

Add `api_token` to the workspace config table.

Move "REST API" from the long-term roadmap to implemented. Update the `pit serve` description in the commands table to mention the API.

**Step 2: Commit**

```bash
git add README.md
git commit -m "Update README with REST API documentation"
```

---

### Task 11: Final verification

**Step 1: Run full test suite**

Run: `go test -race ./...`
Expected: meta, api, config, cli, dag, scaffold packages all PASS

**Step 2: Run vet**

Run: `go vet ./...`
Expected: clean

**Step 3: Build and smoke test**

Run: `go build -o /tmp/pit-test ./cmd/pit`

Run: `/tmp/pit-test serve --help`
Expected: shows help with --port flag

**Step 4: Commit design doc and plan**

```bash
git add docs/plans/2026-03-07-rest-api-design.md docs/plans/2026-03-07-rest-api-plan.md
git commit -m "Add REST API design and implementation plan docs"
```
