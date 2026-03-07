package api

import (
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// JSON response types

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

// Helper functions

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

// handleListDAGs returns all DAGs with their latest run status.
func (h *handler) handleListDAGs(w http.ResponseWriter, r *http.Request) {
	runs, err := h.store.LatestRunPerDAG()
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	runMap := make(map[string]runJSON)
	for _, r := range runs {
		runMap[r.DAGName] = runJSON{
			ID:        r.ID,
			DAGName:   r.DAGName,
			Status:    r.Status,
			StartedAt: timeStr(r.StartedAt),
			EndedAt:   timePtr(r.EndedAt),
			Trigger:   r.Trigger,
			Error:     nilStr(r.Error),
		}
	}

	names := make([]string, 0, len(h.configs))
	for name := range h.configs {
		names = append(names, name)
	}
	sort.Strings(names)

	type dagItem struct {
		Name      string   `json:"name"`
		Schedule  string   `json:"schedule"`
		TaskCount int      `json:"task_count"`
		LatestRun *runJSON `json:"latest_run"`
	}

	dags := make([]dagItem, 0, len(names))
	for _, name := range names {
		cfg := h.configs[name]
		item := dagItem{
			Name:      name,
			Schedule:  cfg.DAG.Schedule,
			TaskCount: len(cfg.Tasks),
		}
		if rj, ok := runMap[name]; ok {
			rj.DAGName = "" // omit dag_name inside list context
			item.LatestRun = &rj
		}
		dags = append(dags, item)
	}

	writeJSON(w, http.StatusOK, map[string]any{"dags": dags})
}

// handleDAGDetail returns details for a single DAG.
func (h *handler) handleDAGDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg, ok := h.configs[name]
	if !ok {
		writeError(w, http.StatusNotFound, "dag not found")
		return
	}

	runs, err := h.store.LatestRuns(name, 10)
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
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

	recentRuns := make([]runJSON, 0, len(runs))
	for _, rr := range runs {
		recentRuns = append(recentRuns, runJSON{
			ID:        rr.ID,
			Status:    rr.Status,
			StartedAt: timeStr(rr.StartedAt),
			EndedAt:   timePtr(rr.EndedAt),
			Trigger:   rr.Trigger,
			Error:     nilStr(rr.Error),
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

// handleListRuns returns runs with optional dag and limit filters.
func (h *handler) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 20, 100)
	dagName := r.URL.Query().Get("dag")

	runs, err := h.store.LatestRuns(dagName, limit)
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	result := make([]runJSON, 0, len(runs))
	for _, rr := range runs {
		result = append(result, runJSON{
			ID:        rr.ID,
			DAGName:   rr.DAGName,
			Status:    rr.Status,
			StartedAt: timeStr(rr.StartedAt),
			EndedAt:   timePtr(rr.EndedAt),
			Trigger:   rr.Trigger,
			Error:     nilStr(rr.Error),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"runs": result})
}

// handleRunDetail returns a single run with its task instances.
func (h *handler) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	run, tasks, err := h.store.RunDetail(id)
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	if run == nil {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}

	taskItems := make([]taskJSON, 0, len(tasks))
	for _, ti := range tasks {
		taskItems = append(taskItems, taskJSON{
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
		"tasks":      taskItems,
	})
}

// handleListOutputs returns outputs from successful runs.
func (h *handler) handleListOutputs(w http.ResponseWriter, r *http.Request) {
	dagFilter := r.URL.Query().Get("dag")

	runs, err := h.store.LatestRunPerDAG()
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	type outputItem struct {
		DAGName  string `json:"dag_name"`
		Name     string `json:"name"`
		Type     string `json:"type"`
		Location string `json:"location"`
	}

	outputs := make([]outputItem, 0)

	for _, rr := range runs {
		if rr.Status != "success" {
			continue
		}
		if dagFilter != "" && rr.DAGName != dagFilter {
			continue
		}

		outs, err := h.store.OutputsByRun(rr.ID)
		if err != nil {
			log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		for _, o := range outs {
			outputs = append(outputs, outputItem{
				DAGName:  o.DAGName,
				Name:     o.Name,
				Type:     o.Type,
				Location: o.Location,
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"outputs": outputs})
}
