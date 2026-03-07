package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/druarnfield/pit/internal/loghub"
)

// handleRunLogs streams logs for a specific run via SSE.
func (h *handler) handleRunLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	lines := parseLines(r)

	run, _, err := h.store.RunDetail(runID)
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	if run == nil {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}

	h.streamRunLogs(w, r, runID, run.DAGName, run.RunDir, run.Status, lines)
}

// handleDAGLogs streams logs for the latest run of a DAG via SSE.
func (h *handler) handleDAGLogs(w http.ResponseWriter, r *http.Request) {
	dagName := r.PathValue("name")
	lines := parseLines(r)

	if _, ok := h.configs[dagName]; !ok {
		writeError(w, http.StatusNotFound, "dag not found")
		return
	}

	// Check hub for active run first
	if h.hub != nil {
		runID := h.hub.ActiveRunForDAG(dagName)
		if runID != "" {
			run, _, err := h.store.RunDetail(runID)
			if err == nil && run != nil {
				h.streamRunLogs(w, r, runID, dagName, run.RunDir, run.Status, lines)
				return
			}
		}
	}

	// Fall back to latest run from metadata store
	runs, err := h.store.LatestRuns(dagName, 1)
	if err != nil {
		log.Printf("api: %v", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	if len(runs) == 0 {
		writeError(w, http.StatusNotFound, "no runs found for dag")
		return
	}

	run := runs[0]
	h.streamRunLogs(w, r, run.ID, dagName, run.RunDir, run.Status, lines)
}

// streamRunLogs handles the SSE streaming for a run.
func (h *handler) streamRunLogs(w http.ResponseWriter, r *http.Request, runID, dagName, runDir, status string, lines int) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	isActive := h.hub != nil && h.hub.IsActive(runID)

	if isActive {
		ch := h.hub.Subscribe(runID)
		defer h.hub.Unsubscribe(runID, ch)

		// Send existing log lines from disk first
		if runDir != "" {
			h.sendLogsFromDisk(w, flusher, runID, dagName, runDir, lines)
		}

		for {
			select {
			case entry, ok := <-ch:
				if !ok {
					finalStatus, _ := h.hub.RunStatus(runID)
					if finalStatus == "" {
						finalStatus = "unknown"
					}
					writeSSEEvent(w, "complete", map[string]string{"status": finalStatus})
					flusher.Flush()
					return
				}
				writeSSEEvent(w, "log", entry)
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	} else {
		// Finished run — send from disk then complete
		if runDir != "" {
			h.sendLogsFromDisk(w, flusher, runID, dagName, runDir, lines)
		}
		writeSSEEvent(w, "complete", map[string]string{"status": status})
		flusher.Flush()
	}
}

// sendLogsFromDisk reads log files and sends them as SSE events.
func (h *handler) sendLogsFromDisk(w http.ResponseWriter, flusher http.Flusher, runID, dagName, runDir string, maxLines int) {
	logDir := filepath.Join(runDir, "logs")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return
	}

	var logFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
			logFiles = append(logFiles, e.Name())
		}
	}
	sort.Strings(logFiles)

	type logLine struct {
		taskName string
		message  string
	}
	var allLines []logLine

	for _, name := range logFiles {
		taskName := strings.TrimSuffix(name, ".log")
		f, err := os.Open(filepath.Join(logDir, name))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			allLines = append(allLines, logLine{taskName: taskName, message: scanner.Text()})
		}
		f.Close()
	}

	// Apply lines limit (tail behavior)
	if maxLines > 0 && len(allLines) > maxLines {
		allLines = allLines[len(allLines)-maxLines:]
	}

	for _, ll := range allLines {
		entry := loghub.Entry{
			Timestamp: time.Time{},
			RunID:     runID,
			DAGName:   dagName,
			TaskName:  ll.taskName,
			Level:     loghub.DetectLevel(ll.message),
			Message:   ll.message,
		}
		writeSSEEvent(w, "log", entry)
	}
	flusher.Flush()
}

func writeSSEEvent(w http.ResponseWriter, event string, data any) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("api: sse json encode error: %v", err)
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, jsonData)
}

func parseLines(r *http.Request) int {
	s := r.URL.Query().Get("lines")
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0
	}
	return n
}
