package loghub

import (
	"sync"
	"time"
)

// Entry is a structured log line emitted by a task execution.
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

// Hub is an in-process pub/sub dispatcher keyed by run ID.
type Hub struct {
	mu          sync.Mutex
	subscribers map[string][]chan Entry
	active      map[string]bool
	completed   map[string]completedRun
}

// New creates a new Hub.
func New() *Hub {
	return &Hub{
		subscribers: make(map[string][]chan Entry),
		active:      make(map[string]bool),
		completed:   make(map[string]completedRun),
	}
}

// Close is a no-op cleanup method for future use.
func (h *Hub) Close() {}

// Activate marks a run as active.
func (h *Hub) Activate(runID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active[runID] = true
}

// IsActive returns true if the run is currently active.
func (h *Hub) IsActive(runID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active[runID]
}

// RunStatus returns the final status and whether the run has completed.
func (h *Hub) RunStatus(runID string) (status string, done bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cr, ok := h.completed[runID]
	if !ok {
		return "", false
	}
	return cr.Status, true
}

// Subscribe returns a read-only channel that receives log entries for the
// given run ID. The channel is buffered with a capacity of 256. If the run
// has already completed, a closed channel is returned immediately.
func (h *Hub) Subscribe(runID string) <-chan Entry {
	h.mu.Lock()
	defer h.mu.Unlock()

	// If the run already completed, return a closed channel.
	if _, done := h.completed[runID]; done {
		ch := make(chan Entry)
		close(ch)
		return ch
	}

	ch := make(chan Entry, 256)
	h.subscribers[runID] = append(h.subscribers[runID], ch)
	return ch
}

// Unsubscribe removes a subscriber channel for the given run ID.
func (h *Hub) Unsubscribe(runID string, ch <-chan Entry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	subs := h.subscribers[runID]
	for i, s := range subs {
		if s == ch {
			h.subscribers[runID] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Publish fans out an entry to all subscribers for the given run ID.
// It is non-blocking: if a subscriber's buffer is full, the entry is dropped.
func (h *Hub) Publish(runID string, entry Entry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, ch := range h.subscribers[runID] {
		select {
		case ch <- entry:
		default:
		}
	}
}

// Complete marks a run as done, records its final status, closes all
// subscriber channels, and removes the run from the active set.
func (h *Hub) Complete(runID string, status string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.completed[runID] = completedRun{Status: status}
	delete(h.active, runID)

	for _, ch := range h.subscribers[runID] {
		close(ch)
	}
	delete(h.subscribers, runID)
}
