package loghub

import (
	"bytes"
	"strings"
	"sync"
	"time"
)

// Writer implements io.Writer. It buffers input, splits on newlines, and
// publishes each complete line as an Entry to the hub.
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

// NewWriter creates a Writer that publishes lines to the given hub.
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

// SetAttempt updates the attempt number (mutex-protected).
func (w *Writer) SetAttempt(attempt int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempt = attempt
}

// Write buffers input, splits on newlines, and publishes each complete line
// as an Entry. It always returns len(p), nil.
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)

	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		line := string(w.buf[:idx])
		w.buf = w.buf[idx+1:]
		w.publish(line)
	}

	return len(p), nil
}

// Flush publishes any remaining buffered content as a line.
func (w *Writer) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buf) > 0 {
		line := string(w.buf)
		w.buf = w.buf[:0]
		w.publish(line)
	}
}

// publish sends a single line as an Entry to the hub. Must be called with mu held.
func (w *Writer) publish(line string) {
	w.hub.Publish(w.runID, Entry{
		Timestamp: time.Now(),
		RunID:     w.runID,
		DAGName:   w.dagName,
		TaskName:  w.task,
		Level:     DetectLevel(line),
		Message:   line,
		Attempt:   w.attempt,
		Duration:  time.Since(w.started).Round(time.Millisecond).String(),
	})
}

// DetectLevel infers a log level from the message content.
//   - Returns "error" if the message starts with "ERROR" (case-insensitive)
//     or contains "TRACEBACK" (case-insensitive).
//   - Returns "warn" if the message starts with "WARNING" or "WARN:"
//     (case-insensitive).
//   - Returns "info" otherwise.
func DetectLevel(msg string) string {
	upper := strings.ToUpper(msg)

	if strings.HasPrefix(upper, "ERROR") {
		return "error"
	}
	if strings.Contains(upper, "TRACEBACK") {
		return "error"
	}
	if strings.HasPrefix(upper, "WARNING") || strings.HasPrefix(upper, "WARN:") {
		return "warn"
	}
	return "info"
}
