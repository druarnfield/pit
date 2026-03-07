# Log Streaming Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add SSE-based log streaming to the REST API with an in-process log hub for live tailing, plus structured metadata on log entries.

**Architecture:** New `internal/loghub` package provides a pub/sub hub with `Entry` types and a hub-aware `io.Writer`. The executor publishes log lines to the hub during execution. SSE endpoints in the API package subscribe to the hub for live runs or read from disk for finished runs. Webhook handler gains `?stream=true` opt-in to stream triggered run logs.

**Tech Stack:** Go stdlib (`log/slog`, `net/http`, SSE via `text/event-stream`), existing `meta.Store` for run lookups

---

### Task 1: Log hub core — Entry type and Hub pub/sub

**Files:**
- Create: `internal/loghub/hub.go`
- Create: `internal/loghub/hub_test.go`

**Step 1: Write the failing tests**

Create `internal/loghub/hub_test.go`:

```go
package loghub

import (
	"testing"
	"time"
)

func TestHub_PublishAndSubscribe(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	entry := Entry{
		Timestamp: time.Now(),
		RunID:     "run-1",
		DAGName:   "my_dag",
		TaskName:  "extract",
		Level:     "info",
		Message:   "hello world",
		Attempt:   1,
		Duration:  "1.2s",
	}

	h.Publish("run-1", entry)

	select {
	case got := <-ch:
		if got.Message != "hello world" {
			t.Errorf("message = %q, want %q", got.Message, "hello world")
		}
		if got.TaskName != "extract" {
			t.Errorf("task = %q, want %q", got.TaskName, "extract")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry")
	}
}

func TestHub_MultipleSubscribers(t *testing.T) {
	h := New()
	defer h.Close()

	ch1 := h.Subscribe("run-1")
	ch2 := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch1)
	defer h.Unsubscribe("run-1", ch2)

	entry := Entry{RunID: "run-1", Message: "broadcast"}
	h.Publish("run-1", entry)

	for i, ch := range []<-chan Entry{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Message != "broadcast" {
				t.Errorf("subscriber %d: message = %q, want %q", i, got.Message, "broadcast")
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out", i)
		}
	}
}

func TestHub_Complete(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")

	h.Complete("run-1", "success")

	// Channel should be closed after draining
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	// Status should be recorded
	status, done := h.RunStatus("run-1")
	if !done {
		t.Error("expected run to be done")
	}
	if status != "success" {
		t.Errorf("status = %q, want %q", status, "success")
	}
}

func TestHub_IsActive(t *testing.T) {
	h := New()
	defer h.Close()

	if h.IsActive("run-1") {
		t.Error("run-1 should not be active before Activate")
	}

	h.Activate("run-1")

	if !h.IsActive("run-1") {
		t.Error("run-1 should be active after Activate")
	}

	h.Complete("run-1", "success")

	if h.IsActive("run-1") {
		t.Error("run-1 should not be active after Complete")
	}
}

func TestHub_UnsubscribeCleanup(t *testing.T) {
	h := New()
	defer h.Close()

	ch1 := h.Subscribe("run-1")
	ch2 := h.Subscribe("run-1")

	h.Unsubscribe("run-1", ch1)

	// ch2 should still receive
	entry := Entry{RunID: "run-1", Message: "after unsub"}
	h.Publish("run-1", entry)

	select {
	case got := <-ch2:
		if got.Message != "after unsub" {
			t.Errorf("message = %q, want %q", got.Message, "after unsub")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for entry on ch2")
	}

	h.Unsubscribe("run-1", ch2)
}

func TestHub_SubscribeAfterComplete(t *testing.T) {
	h := New()
	defer h.Close()

	h.Activate("run-1")
	h.Complete("run-1", "failed")

	ch := h.Subscribe("run-1")

	// Channel should be closed immediately since run is done
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed for completed run")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out — channel should close immediately for completed run")
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/loghub/ -v`
Expected: FAIL — package doesn't exist

**Step 3: Implement the hub**

Create `internal/loghub/hub.go`:

```go
package loghub

import (
	"sync"
	"time"
)

// Entry is a structured log line with metadata.
type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	RunID     string    `json:"run_id"`
	DAGName   string    `json:"dag_name"`
	TaskName  string    `json:"task_name"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Attempt   int       `json:"attempt"`
	Duration  string    `json:"duration"`
}

// completedRun records the final status of a finished run.
type completedRun struct {
	Status string
}

// Hub is an in-process pub/sub for log entries, keyed by run ID.
type Hub struct {
	mu          sync.Mutex
	subscribers map[string][]chan Entry // runID → subscriber channels
	active      map[string]bool        // runID → true if run is in progress
	completed   map[string]completedRun // runID → final status
}

// New creates a new Hub.
func New() *Hub {
	return &Hub{
		subscribers: make(map[string][]chan Entry),
		active:      make(map[string]bool),
		completed:   make(map[string]completedRun),
	}
}

// Close is a no-op that satisfies cleanup patterns.
func (h *Hub) Close() {}

// Activate marks a run as active in the hub. Call this when a run starts
// so that subscribers know the run is live.
func (h *Hub) Activate(runID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active[runID] = true
}

// IsActive returns true if the run is currently active in the hub.
func (h *Hub) IsActive(runID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active[runID]
}

// RunStatus returns the final status and whether the run has completed.
// Returns ("", false) if the run is not tracked or still active.
func (h *Hub) RunStatus(runID string) (status string, done bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cr, ok := h.completed[runID]
	if !ok {
		return "", false
	}
	return cr.Status, true
}

// Subscribe returns a channel that receives Entry values for the given run.
// If the run is already completed, the returned channel is closed immediately.
func (h *Hub) Subscribe(runID string) <-chan Entry {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan Entry, 256)

	// If run already completed, close immediately
	if _, done := h.completed[runID]; done {
		close(ch)
		return ch
	}

	h.subscribers[runID] = append(h.subscribers[runID], ch)
	return ch
}

// Unsubscribe removes a subscriber channel for a run.
func (h *Hub) Unsubscribe(runID string, ch <-chan Entry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs := h.subscribers[runID]
	for i, s := range subs {
		if s == ch {
			h.subscribers[runID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(h.subscribers[runID]) == 0 {
		delete(h.subscribers, runID)
	}
}

// Publish sends an entry to all subscribers of the given run.
// Non-blocking: if a subscriber's buffer is full, the entry is dropped for that subscriber.
func (h *Hub) Publish(runID string, entry Entry) {
	h.mu.Lock()
	subs := make([]chan Entry, len(h.subscribers[runID]))
	copy(subs, h.subscribers[runID])
	h.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- entry:
		default:
			// subscriber too slow, drop
		}
	}
}

// Complete marks a run as finished, closes all subscriber channels, and
// records the final status. Subsequent Subscribe calls for this run will
// return an already-closed channel.
func (h *Hub) Complete(runID string, status string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.active, runID)
	h.completed[runID] = completedRun{Status: status}

	for _, ch := range h.subscribers[runID] {
		close(ch)
	}
	delete(h.subscribers, runID)
}
```

**Step 4: Run tests**

Run: `go test -race ./internal/loghub/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/loghub/hub.go internal/loghub/hub_test.go
git commit -m "Add loghub package with pub/sub hub for log streaming"
```

---

### Task 2: Hub-aware io.Writer

**Files:**
- Create: `internal/loghub/writer.go`
- Create: `internal/loghub/writer_test.go`

**Step 1: Write the failing tests**

Create `internal/loghub/writer_test.go`:

```go
package loghub

import (
	"strings"
	"testing"
	"time"
)

func TestWriter_SingleLine(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "my_dag", "extract", 1)

	n, err := w.Write([]byte("hello world\n"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != 12 {
		t.Errorf("n = %d, want 12", n)
	}

	select {
	case got := <-ch:
		if got.Message != "hello world" {
			t.Errorf("message = %q, want %q", got.Message, "hello world")
		}
		if got.RunID != "run-1" {
			t.Errorf("run_id = %q, want %q", got.RunID, "run-1")
		}
		if got.DAGName != "my_dag" {
			t.Errorf("dag_name = %q, want %q", got.DAGName, "my_dag")
		}
		if got.TaskName != "extract" {
			t.Errorf("task_name = %q, want %q", got.TaskName, "extract")
		}
		if got.Attempt != 1 {
			t.Errorf("attempt = %d, want 1", got.Attempt)
		}
		if got.Level != "info" {
			t.Errorf("level = %q, want %q", got.Level, "info")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestWriter_MultipleLines(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "dag", "task", 1)

	w.Write([]byte("line one\nline two\n"))

	var messages []string
	for i := 0; i < 2; i++ {
		select {
		case got := <-ch:
			messages = append(messages, got.Message)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", i+1)
		}
	}

	if messages[0] != "line one" {
		t.Errorf("messages[0] = %q, want %q", messages[0], "line one")
	}
	if messages[1] != "line two" {
		t.Errorf("messages[1] = %q, want %q", messages[1], "line two")
	}
}

func TestWriter_PartialLine(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "dag", "task", 1)

	// Write partial, then complete
	w.Write([]byte("hel"))
	w.Write([]byte("lo\n"))

	select {
	case got := <-ch:
		if got.Message != "hello" {
			t.Errorf("message = %q, want %q", got.Message, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestWriter_Duration(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "dag", "task", 1)

	// Sleep briefly so duration is non-zero
	time.Sleep(10 * time.Millisecond)
	w.Write([]byte("msg\n"))

	select {
	case got := <-ch:
		if got.Duration == "" || got.Duration == "0s" {
			t.Errorf("expected non-zero duration, got %q", got.Duration)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestWriter_ErrorLevel(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "dag", "task", 1)

	// Lines containing common error patterns should be level "error"
	w.Write([]byte("ERROR: something broke\n"))

	select {
	case got := <-ch:
		if got.Level != "error" {
			t.Errorf("level = %q, want %q", got.Level, "error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestWriter_SetAttempt(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "dag", "task", 1)
	w.SetAttempt(3)

	w.Write([]byte("retry msg\n"))

	select {
	case got := <-ch:
		if got.Attempt != 3 {
			t.Errorf("attempt = %d, want 3", got.Attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestWriter_Flush(t *testing.T) {
	h := New()
	defer h.Close()

	ch := h.Subscribe("run-1")
	defer h.Unsubscribe("run-1", ch)

	w := NewWriter(h, "run-1", "dag", "task", 1)
	w.Write([]byte("no newline"))
	w.Flush()

	select {
	case got := <-ch:
		if got.Message != "no newline" {
			t.Errorf("message = %q, want %q", got.Message, "no newline")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestDetectLevel(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"normal message", "info"},
		{"ERROR: something broke", "error"},
		{"error: something broke", "error"},
		{"Traceback (most recent call last):", "error"},
		{"WARNING: check this", "warn"},
		{"warning: check this", "warn"},
	}

	for _, tt := range tests {
		got := detectLevel(tt.msg)
		if got != tt.want {
			t.Errorf("detectLevel(%q) = %q, want %q", tt.msg, got, tt.want)
		}
	}
}
```

Note: add `"strings"` to imports if the compiler asks (it's already in the template but unused in tests — remove if not needed).

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/loghub/ -run TestWriter -v`
Expected: FAIL — `NewWriter` not defined

**Step 3: Implement the writer**

Create `internal/loghub/writer.go`:

```go
package loghub

import (
	"strings"
	"sync"
	"time"
)

// Writer is an io.Writer that splits output into lines and publishes
// each line as an Entry to the hub. It buffers partial lines until
// a newline is received.
type Writer struct {
	hub     *Hub
	runID   string
	dagName string
	task    string
	attempt int
	started time.Time

	mu  sync.Mutex
	buf []byte
}

// NewWriter creates a Writer that publishes lines to the hub.
func NewWriter(hub *Hub, runID, dagName, taskName string, attempt int) *Writer {
	return &Writer{
		hub:     hub,
		runID:   runID,
		dagName: dagName,
		task:    taskName,
		attempt: attempt,
		started: time.Now(),
	}
}

// SetAttempt updates the attempt number (for retries).
func (w *Writer) SetAttempt(attempt int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempt = attempt
}

// Write splits p into lines and publishes each as an Entry.
// Partial lines are buffered until a newline arrives.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(p)
	w.buf = append(w.buf, p...)

	for {
		idx := -1
		for i, b := range w.buf {
			if b == '\n' {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}

		line := string(w.buf[:idx])
		w.buf = w.buf[idx+1:]
		w.publishLine(line)
	}

	return n, nil
}

// Flush publishes any buffered partial line.
func (w *Writer) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buf) > 0 {
		line := string(w.buf)
		w.buf = w.buf[:0]
		w.publishLine(line)
	}
}

func (w *Writer) publishLine(line string) {
	entry := Entry{
		Timestamp: time.Now(),
		RunID:     w.runID,
		DAGName:   w.dagName,
		TaskName:  w.task,
		Level:     detectLevel(line),
		Message:   line,
		Attempt:   w.attempt,
		Duration:  time.Since(w.started).Round(time.Millisecond).String(),
	}
	w.hub.Publish(w.runID, entry)
}

// detectLevel infers a log level from the message content.
func detectLevel(msg string) string {
	upper := strings.ToUpper(msg)
	if strings.HasPrefix(upper, "ERROR") || strings.Contains(upper, "TRACEBACK") {
		return "error"
	}
	if strings.HasPrefix(upper, "WARNING") || strings.HasPrefix(upper, "WARN:") {
		return "warn"
	}
	return "info"
}
```

**Step 4: Run tests**

Run: `go test -race ./internal/loghub/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/loghub/writer.go internal/loghub/writer_test.go
git commit -m "Add hub-aware io.Writer for structured log entry publishing"
```

---

### Task 3: Wire hub writer into executor

**Files:**
- Modify: `internal/engine/executor.go`

**Step 1: Add LogHub to ExecuteOpts and wire into executeTask**

In `internal/engine/executor.go`, add to imports:

```go
"github.com/druarnfield/pit/internal/loghub"
```

Add field to `ExecuteOpts` struct (after `Trigger`):

```go
LogHub *loghub.Hub // nil = no live log streaming
```

In `executeTask`, after the log writer setup (after `var logWriter io.Writer = logFile`, around line 479), add the hub writer to the chain:

Replace this block (lines 478-490):

```go
	// Set up log writer — optionally tee to stdout
	var logWriter io.Writer = logFile
	if opts.Verbose {
		isConcurrent := len(concurrent) > 0 && concurrent[0]
		if isConcurrent {
			logWriter = io.MultiWriter(logFile, &prefixWriter{
				prefix: []byte("[" + ti.Name + "] "),
				dest:   os.Stdout,
			})
		} else {
			logWriter = io.MultiWriter(logFile, os.Stdout)
		}
	}
```

With:

```go
	// Set up log writer — optionally tee to stdout and/or hub
	var logWriter io.Writer = logFile
	var hubWriter *loghub.Writer
	writers := []io.Writer{logFile}
	if opts.Verbose {
		isConcurrent := len(concurrent) > 0 && concurrent[0]
		if isConcurrent {
			writers = append(writers, &prefixWriter{
				prefix: []byte("[" + ti.Name + "] "),
				dest:   os.Stdout,
			})
		} else {
			writers = append(writers, os.Stdout)
		}
	}
	if opts.LogHub != nil {
		hubWriter = loghub.NewWriter(opts.LogHub, run.ID, run.DAGName, ti.Name, 1)
		writers = append(writers, hubWriter)
	}
	if len(writers) > 1 {
		logWriter = io.MultiWriter(writers...)
	}
```

In the retry loop (around line 532 where `ti.Attempt` is set), add after `run.mu.Unlock()`:

```go
		if hubWriter != nil {
			hubWriter.SetAttempt(attempt)
		}
```

At the end of `Execute()` (after `printSummary`, before artifact cleanup, around line 250), add:

```go
	// Signal hub that run is complete
	if opts.LogHub != nil {
		opts.LogHub.Complete(run.ID, string(run.Status))
	}
```

Also in `Execute()`, after the run is created and before recording run start (around line 157), add:

```go
	// Activate run in log hub so SSE clients can discover it
	if opts.LogHub != nil {
		opts.LogHub.Activate(runID)
	}
```

**Step 2: Run tests**

Run: `go test -race ./internal/engine/ -v`
Expected: PASS (existing tests still pass — LogHub is nil by default)

**Step 3: Run full test suite**

Run: `go test -race ./...`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/engine/executor.go
git commit -m "Wire log hub into executor for live log streaming"
```

---

### Task 4: SSE streaming handler for finished runs

**Files:**
- Create: `internal/api/sse.go`
- Modify: `internal/api/server.go`

**Step 1: Update server.go to accept hub and runsDir**

In `internal/api/server.go`, add to the `handler` struct:

```go
	hub     *loghub.Hub
	runsDir string
```

Add import:

```go
	"github.com/druarnfield/pit/internal/loghub"
```

Update `NewHandler` signature and body:

```go
func NewHandler(configs map[string]*config.ProjectConfig, store meta.Store, token string, hub *loghub.Hub, runsDir string) http.Handler {
	h := &handler{configs: configs, store: store, token: token, hub: hub, runsDir: runsDir}
```

Add two new routes in `NewHandler` (after the existing routes, before `return`):

```go
	mux.HandleFunc("GET /api/runs/{id}/logs", h.handleRunLogs)
	mux.HandleFunc("GET /api/dags/{name}/logs", h.handleDAGLogs)
```

**Step 2: Create sse.go with handlers**

Create `internal/api/sse.go`:

```go
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

	// Look up run in metadata store
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

// streamRunLogs handles the actual SSE streaming for a run.
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

	// Check if run is active in the hub
	isActive := h.hub != nil && h.hub.IsActive(runID)

	if isActive {
		// Live run: subscribe and stream
		ch := h.hub.Subscribe(runID)
		defer h.hub.Unsubscribe(runID, ch)

		// Also send existing log lines from disk first
		if runDir != "" {
			h.sendLogsFromDisk(w, flusher, runID, dagName, runDir, lines)
		}

		// Stream live entries
		for {
			select {
			case entry, ok := <-ch:
				if !ok {
					// Channel closed — run completed
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
		// Finished run: send from disk, then complete
		if runDir != "" {
			h.sendLogsFromDisk(w, flusher, runID, dagName, runDir, lines)
		}
		writeSSEEvent(w, "complete", map[string]string{"status": status})
		flusher.Flush()
	}
}

// StreamRunLogs is an exported wrapper for streamRunLogs, used by the webhook handler.
func (h *handler) StreamRunLogs(w http.ResponseWriter, r *http.Request, runID, dagName string, lines int) {
	// For webhook streaming, the run is always live (just started).
	// We don't have a runDir yet (it's being created), so skip disk read.
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if h.hub == nil {
		writeSSEEvent(w, "complete", map[string]string{"status": "unknown"})
		flusher.Flush()
		return
	}

	ch := h.hub.Subscribe(runID)
	defer h.hub.Unsubscribe(runID, ch)

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
}

// sendLogsFromDisk reads log files from a run directory and sends them as SSE events.
func (h *handler) sendLogsFromDisk(w http.ResponseWriter, flusher http.Flusher, runID, dagName, runDir string, maxLines int) {
	logDir := filepath.Join(runDir, "logs")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return // no logs on disk yet
	}

	var logFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
			logFiles = append(logFiles, e.Name())
		}
	}
	sort.Strings(logFiles)

	// Collect all lines across all log files
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
			Timestamp: time.Time{}, // unknown for historical logs
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
		return 0 // 0 = all
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0
	}
	return n
}
```

Also export `detectLevel` in `internal/loghub/writer.go` — rename the function from `detectLevel` to `DetectLevel` and update all references in the same file.

**Step 3: Add ActiveRunForDAG to hub**

In `internal/loghub/hub.go`, add this method:

```go
// ActiveRunForDAG returns the run ID of an active run for the given DAG name,
// or empty string if no active run exists. If multiple runs are active for the
// same DAG, returns the first one found (non-deterministic).
func (h *Hub) ActiveRunForDAG(dagName string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	for runID := range h.active {
		// Run IDs have format: 20060102_150405.000_dag_name
		// The DAG name starts at position 20
		if len(runID) > 20 && runID[20:] == dagName {
			return runID
		}
	}
	return ""
}
```

**Step 4: Update all existing NewHandler call sites**

In `internal/api/api_test.go`, update every `NewHandler` call to pass `nil, ""` for the new hub and runsDir parameters. There are 11 calls — each becomes e.g.:

```go
NewHandler(newTestConfigs(), newTestStore(t), "", nil, "")
```

In `internal/serve/server.go`, update the `NewHandler` call (around line 102):

```go
s.apiHandler = api.NewHandler(configs, srvOpts.MetaQueryStore, srvOpts.APIToken, nil, srvOpts.RunsDir)
```

**Step 5: Run tests**

Run: `go test -race ./...`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/api/sse.go internal/api/server.go internal/api/api_test.go internal/loghub/hub.go internal/loghub/writer.go internal/serve/server.go
git commit -m "Add SSE log streaming endpoints and wire into API"
```

---

### Task 5: SSE endpoint tests

**Files:**
- Modify: `internal/api/api_test.go`

**Step 1: Write the tests**

Add to `internal/api/api_test.go`:

```go
func TestRunLogsFinished(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)

	// Create log files on disk
	runDir := t.TempDir()
	logDir := filepath.Join(runDir, "logs")
	os.MkdirAll(logDir, 0o755)
	os.WriteFile(filepath.Join(logDir, "extract.log"), []byte("fetching data\ndone\n"), 0o644)
	os.WriteFile(filepath.Join(logDir, "load.log"), []byte("loading records\n"), 0o644)

	// Update the run's RunDir in the store
	store.UpdateRunDir("20260307_143000.000_dag_a", runDir)

	h := NewHandler(newTestConfigs(), store, "", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/runs/20260307_143000.000_dag_a/logs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Errorf("content-type = %q, want %q", ct, "text/event-stream")
	}

	body := w.Body.String()
	if !strings.Contains(body, "event: log") {
		t.Error("response missing 'event: log'")
	}
	if !strings.Contains(body, "fetching data") {
		t.Error("response missing log line 'fetching data'")
	}
	if !strings.Contains(body, "event: complete") {
		t.Error("response missing 'event: complete'")
	}
	if !strings.Contains(body, `"status":"success"`) {
		t.Error("response missing status in complete event")
	}
}

func TestRunLogsWithLinesParam(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)

	runDir := t.TempDir()
	logDir := filepath.Join(runDir, "logs")
	os.MkdirAll(logDir, 0o755)
	os.WriteFile(filepath.Join(logDir, "extract.log"), []byte("line1\nline2\nline3\n"), 0o644)

	store.UpdateRunDir("20260307_143000.000_dag_a", runDir)

	h := NewHandler(newTestConfigs(), store, "", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/runs/20260307_143000.000_dag_a/logs?lines=2", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	// Should have last 2 lines (line2, line3), not line1
	if strings.Contains(body, "line1") {
		t.Error("response should not contain 'line1' with lines=2")
	}
	if !strings.Contains(body, "line2") {
		t.Error("response missing 'line2'")
	}
	if !strings.Contains(body, "line3") {
		t.Error("response missing 'line3'")
	}
}

func TestRunLogsNotFound(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/runs/nonexistent/logs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestDAGLogsNotFound(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags/nonexistent/logs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestDAGLogsResolvesLatest(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)

	runDir := t.TempDir()
	logDir := filepath.Join(runDir, "logs")
	os.MkdirAll(logDir, 0o755)
	os.WriteFile(filepath.Join(logDir, "extract.log"), []byte("dag_a log\n"), 0o644)

	store.UpdateRunDir("20260307_143000.000_dag_a", runDir)

	h := NewHandler(newTestConfigs(), store, "", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags/dag_a/logs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !strings.Contains(body, "dag_a log") {
		t.Error("response missing 'dag_a log'")
	}
	if !strings.Contains(body, "event: complete") {
		t.Error("response missing complete event")
	}
}

func TestSSEAuthRequired(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "secret-token", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/runs/any/logs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestDAGLogsNoRuns(t *testing.T) {
	h := NewHandler(newTestConfigs(), newTestStore(t), "", nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/dags/dag_a/logs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}
```

Add these imports to the test file if not already present: `"os"`, `"path/filepath"`, `"strings"`.

**Step 2: Add UpdateRunDir to SQLiteStore**

The tests need to set the `run_dir` for a seeded run so the SSE handler can find log files. Add this method to `internal/meta/sqlite.go`:

```go
// UpdateRunDir updates the run_dir for a run record (used in tests).
func (s *SQLiteStore) UpdateRunDir(runID, runDir string) error {
	_, err := s.db.Exec("UPDATE runs SET run_dir = ? WHERE id = ?", runDir, runID)
	return err
}
```

**Step 3: Run tests**

Run: `go test -race ./internal/api/ -v`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/api/api_test.go internal/meta/sqlite.go
git commit -m "Add SSE endpoint tests for finished run and DAG log streaming"
```

---

### Task 6: Live streaming test with hub

**Files:**
- Modify: `internal/api/api_test.go`

**Step 1: Write the live streaming test**

Add to `internal/api/api_test.go`:

```go
func TestRunLogsLive(t *testing.T) {
	store := newTestStore(t)
	seedTestRuns(t, store)

	hub := loghub.New()
	defer hub.Close()

	runID := "20260307_143000.000_dag_a"

	// Mark run as active in hub
	hub.Activate(runID)

	// Update run status to "running" in store
	store.UpdateRunDir(runID, t.TempDir())

	h := NewHandler(newTestConfigs(), store, "", hub, "")

	// Create a pipe to read the SSE response as it streams
	req := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/logs", nil)
	w := httptest.NewRecorder()

	// Run handler in goroutine (it blocks on live stream)
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(w, req)
	}()

	// Publish some entries
	time.Sleep(50 * time.Millisecond) // let handler subscribe
	hub.Publish(runID, loghub.Entry{
		RunID:    runID,
		DAGName:  "dag_a",
		TaskName: "extract",
		Level:    "info",
		Message:  "live log line",
		Attempt:  1,
		Duration: "0s",
	})

	// Complete the run
	time.Sleep(50 * time.Millisecond)
	hub.Complete(runID, "success")

	// Wait for handler to finish
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish")
	}

	body := w.Body.String()
	if !strings.Contains(body, "live log line") {
		t.Error("response missing live log line")
	}
	if !strings.Contains(body, "event: complete") {
		t.Error("response missing complete event")
	}
}
```

Add import for `"github.com/druarnfield/pit/internal/loghub"` to `api_test.go`.

**Step 2: Run tests**

Run: `go test -race ./internal/api/ -v`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/api/api_test.go
git commit -m "Add live SSE streaming test with hub"
```

---

### Task 7: Wire hub into serve package

**Files:**
- Modify: `internal/serve/server.go`

**Step 1: Add hub to serve.Server**

In `internal/serve/server.go`, add import:

```go
"github.com/druarnfield/pit/internal/loghub"
```

Add field to `Server` struct:

```go
	logHub *loghub.Hub
```

In `NewServer`, create the hub (before the trigger loop, around line 79):

```go
	logHub := loghub.New()
```

Add to the `s := &Server{` initialization:

```go
		logHub: logHub,
```

Set `LogHub` on the engine opts:

```go
		opts: engine.ExecuteOpts{
			RunsDir:      srvOpts.RunsDir,
			RepoCacheDir: srvOpts.RepoCacheDir,
			Verbose:      verbose,
			SecretsPath:  secretsPath,
			DBTDriver:    srvOpts.DBTDriver,
			MetaStore:    srvOpts.MetaStore,
			LogHub:       logHub,
		},
```

Update the `api.NewHandler` call to pass the hub and runsDir:

```go
s.apiHandler = api.NewHandler(configs, srvOpts.MetaQueryStore, srvOpts.APIToken, logHub, srvOpts.RunsDir)
```

**Step 2: Run tests**

Run: `go test -race ./internal/serve/ -v`
Expected: PASS

**Step 3: Run full suite**

Run: `go test -race ./...`
Expected: PASS

**Step 4: Commit**

```bash
git add internal/serve/server.go
git commit -m "Wire log hub into serve package for live log streaming"
```

---

### Task 8: Webhook streaming opt-in

**Files:**
- Modify: `internal/serve/server.go`

**Step 1: Update webhookHandler for streaming**

The webhook handler needs to be refactored so that when `?stream=true` is present, it starts the run synchronously and streams logs via SSE instead of firing-and-forgetting.

Replace the `webhookHandler` method in `internal/serve/server.go`:

```go
// webhookHandler handles inbound POST /webhook/{dag-name} requests.
func (s *Server) webhookHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dagName := strings.TrimPrefix(r.URL.Path, "/webhook/")
	if dagName == "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	expected, ok := s.webhookTokens[dagName]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	authHeader := r.Header.Get("Authorization")
	var provided string
	if strings.HasPrefix(authHeader, "Bearer ") {
		provided = authHeader[len("Bearer "):]
	}
	if subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	stream := r.URL.Query().Get("stream") == "true"

	if stream {
		s.webhookStreamRun(w, r, dagName)
	} else {
		select {
		case s.eventCh <- trigger.Event{DAGName: dagName, Source: "webhook"}:
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "server busy", http.StatusServiceUnavailable)
		}
	}
}

// webhookStreamRun triggers a run and streams its logs via SSE.
func (s *Server) webhookStreamRun(w http.ResponseWriter, r *http.Request, dagName string) {
	cfg, ok := s.configs[dagName]
	if !ok {
		http.Error(w, "unknown DAG", http.StatusNotFound)
		return
	}

	// Check overlap
	overlap := cfg.DAG.Overlap
	if overlap == "" {
		overlap = "allow"
	}
	s.mu.Lock()
	isActive := s.activeRuns[dagName]
	if isActive && overlap == "skip" {
		s.mu.Unlock()
		http.Error(w, "DAG already running (overlap=skip)", http.StatusConflict)
		return
	}
	s.activeRuns[dagName] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.activeRuns[dagName] = false
		s.mu.Unlock()
	}()

	// Build execution opts
	opts := s.opts
	opts.Trigger = "webhook"
	opts.KeepArtifacts = resolveArtifacts(cfg.DAG.KeepArtifacts, s.workspaceArtifacts)

	// Generate run ID so we can subscribe before execution starts
	runID := engine.GenerateRunID(dagName)

	// Subscribe to hub before starting execution
	if s.logHub != nil {
		s.logHub.Activate(runID)
	}

	// Start execution in background
	go func() {
		log.Printf("[%s] triggered by webhook (streaming)", dagName)
		run, err := engine.Execute(r.Context(), cfg, opts)
		if err != nil {
			log.Printf("[%s] execution error: %v", dagName, err)
			return
		}
		log.Printf("[%s] completed: %s", dagName, run.Status)
	}()

	// Stream logs — this blocks until run completes or client disconnects
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if s.logHub == nil {
		fmt.Fprintf(w, "event: complete\ndata: {\"status\":\"unknown\"}\n\n")
		flusher.Flush()
		return
	}

	ch := s.logHub.Subscribe(runID)
	defer s.logHub.Unsubscribe(runID, ch)

	for {
		select {
		case entry, open := <-ch:
			if !open {
				finalStatus, _ := s.logHub.RunStatus(runID)
				if finalStatus == "" {
					finalStatus = "unknown"
				}
				data, _ := json.Marshal(map[string]string{"status": finalStatus})
				fmt.Fprintf(w, "event: complete\ndata: %s\n\n", data)
				flusher.Flush()
				return
			}
			data, _ := json.Marshal(entry)
			fmt.Fprintf(w, "event: log\ndata: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
```

Add `"encoding/json"` and `"fmt"` to `server.go` imports if not already present. Also add `"github.com/druarnfield/pit/internal/engine"` import (for `engine.GenerateRunID`).

**Note:** The `webhookStreamRun` method has a design issue — the executor currently generates its own run ID internally in `Execute()`, but we need the run ID before execution starts so we can subscribe to the hub. This requires one of two approaches:

**Option A:** Add a `RunID` field to `ExecuteOpts` — if set, use it instead of generating one. This is the cleanest approach.

**Option B:** Subscribe to the hub by DAG name instead of run ID.

Go with **Option A**. In `internal/engine/executor.go`, add to `ExecuteOpts`:

```go
RunID string // if set, use this instead of generating (for webhook streaming)
```

At the start of `Execute()`, change:

```go
runID := GenerateRunID(cfg.DAG.Name)
```

To:

```go
runID := opts.RunID
if runID == "" {
	runID = GenerateRunID(cfg.DAG.Name)
}
```

Then in `webhookStreamRun`, set `opts.RunID = runID` before calling `engine.Execute`.

**Step 2: Run tests**

Run: `go test -race ./...`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/serve/server.go internal/engine/executor.go
git commit -m "Add webhook streaming opt-in with ?stream=true"
```

---

### Task 9: Webhook streaming test

**Files:**
- Modify: `internal/serve/server_test.go`

**Step 1: Write the test**

Add to `internal/serve/server_test.go`:

```go
func TestWebhookStreamDefault(t *testing.T) {
	// Verify default webhook behavior (no ?stream) still works
	dir := t.TempDir()
	mkProject(t, dir, "hook_dag", `[dag]
name = "hook_dag"

[dag.webhook]
token_secret = "hook_token"

[[tasks]]
name = "hello"
script = "tasks/hello.sh"
`)

	secretsFile := filepath.Join(dir, "secrets.toml")
	os.WriteFile(secretsFile, []byte(`[global]
hook_token = "my-secret"
`), 0o644)

	s, err := NewServer(dir, secretsFile, false, Options{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook/hook_dag", nil)
	req.Header.Set("Authorization", "Bearer my-secret")
	w := httptest.NewRecorder()
	s.webhookHandler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", w.Code, http.StatusAccepted)
	}
}
```

Add imports: `"net/http/httptest"`, `"net/http"`.

**Step 2: Run tests**

Run: `go test -race ./internal/serve/ -v -run TestWebhook`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/serve/server_test.go
git commit -m "Add webhook streaming default behavior test"
```

---

### Task 10: Update README

**Files:**
- Modify: `README.md`

**Step 1: Read the README**

Read `README.md` to find the REST API section and CLI commands table.

**Step 2: Update README**

Add the SSE log streaming endpoints to the REST API section:

```markdown
| `GET /api/runs/{id}/logs` | Stream run logs via SSE (`?lines=N` for last N lines) |
| `GET /api/dags/{name}/logs` | Stream latest run logs for a DAG via SSE |
```

Add webhook streaming to the webhook section:

```markdown
Add `?stream=true` to receive an SSE stream of the run's logs instead of a fire-and-forget response.
```

**Step 3: Commit**

```bash
git add README.md
git commit -m "Add log streaming endpoints and webhook streaming to README"
```

---

### Task 11: Full test suite and vet

**Step 1: Run all tests**

Run: `go test -race ./...`
Expected: All packages PASS

**Step 2: Run vet**

Run: `go vet ./...`
Expected: Clean

**Step 3: Build and smoke test**

Run: `go build -o /tmp/pit-test ./cmd/pit && /tmp/pit-test serve --help`
Expected: Shows help

**Step 4: Commit plan doc**

```bash
git add docs/plans/2026-03-07-log-streaming-plan.md
git commit -m "Add log streaming implementation plan"
```

---

Plan complete and saved to `docs/plans/2026-03-07-log-streaming-plan.md`. Two execution options:

**1. Subagent-Driven (this session)** — I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** — Open new session with executing-plans, batch execution with checkpoints

Which approach?