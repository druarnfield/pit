package runner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// dbtLogParser is an io.Writer that transforms dbt JSON log lines into
// clean, progress-aware output. It tracks in-flight models so you always
// know what's still running.
//
// Supports both log formats:
//   - log_version 2 (dbt 1.3–1.4): flat top-level code/msg/level/data
//   - log_version 3 (dbt 1.5+):    nested info{name,code,msg,level} + data{}
type dbtLogParser struct {
	dest io.Writer
	pr   *io.PipeReader
	pw   *io.PipeWriter
	done chan struct{}

	mu       sync.Mutex
	running  []runningNode // nodes started but not yet finished, in start order
	total    int           // total node count from the first Q033 event
	finished int           // how many have completed so far
}

type runningNode struct {
	name      string
	uniqueID  string
	startedAt time.Time
}

// ── Unified event types ──────────────────────────────────────────

type dbtEvent struct {
	Code  string
	Name  string // empty in log_version 2
	Msg   string
	Level string
	Ts    time.Time
	Data  dbtEventData
}

type dbtEventData struct {
	Description   string  `json:"description"`
	ExecutionTime float64 `json:"execution_time"`
	Status        string  `json:"status"`
	StatLine      string  `json:"stat_line"`
	Index         int     `json:"index"`
	Total         int     `json:"total"`

	RowsAffected    int64 `json:"rows_affected"`
	NumRowsAffected int64 `json:"num_rows_affected"`

	NodeInfo dbtNodeInfo `json:"node_info"`

	Source struct {
		Name       string `json:"name"`
		SourceName string `json:"source_name"`
	} `json:"source"`

	Elapsed           float64        `json:"elapsed"`
	Execution         string         `json:"execution"`
	Stats             map[string]int `json:"stats"`
	KeyboardInterrupt bool           `json:"keyboard_interrupt"`
	NumErrors         int            `json:"num_errors"`
	NumWarnings       int            `json:"num_warnings"`
	NumThreads        int            `json:"num_threads"`
	TargetName        string         `json:"target_name"`
	V                 string         `json:"v"`

	Msg string `json:"msg"`
}

type dbtNodeInfo struct {
	NodeName      string `json:"node_name"`
	NodePath      string `json:"node_path"`
	Name          string `json:"name"`
	Path          string `json:"path"`
	Materialized  string `json:"materialized"`
	UniqueID      string `json:"unique_id"`
	NodeStatus    string `json:"node_status"`
	ResourceType  string `json:"resource_type"`
	NodeStartedAt string `json:"node_started_at"`
}

func (n dbtNodeInfo) resolvedName() string {
	if n.NodeName != "" {
		return n.NodeName
	}
	if n.Name != "" {
		return n.Name
	}
	if n.NodePath != "" {
		return n.NodePath
	}
	return n.Path
}

func (d dbtEventData) resolvedRows() int64 {
	if d.RowsAffected != 0 {
		return d.RowsAffected
	}
	return d.NumRowsAffected
}

// ── Parser lifecycle ─────────────────────────────────────────────

func newDBTLogParser(dest io.Writer) *dbtLogParser {
	pr, pw := io.Pipe()
	p := &dbtLogParser{
		dest: dest,
		pr:   pr,
		pw:   pw,
		done: make(chan struct{}),
	}
	go p.processLines()
	return p
}

func (p *dbtLogParser) Write(data []byte) (int, error) {
	return p.pw.Write(data)
}

func (p *dbtLogParser) Close() error {
	p.pw.Close()
	<-p.done
	return nil
}

func (p *dbtLogParser) processLines() {
	defer close(p.done)
	scanner := bufio.NewScanner(p.pr)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	for scanner.Scan() {
		p.handleLine(scanner.Bytes())
	}
}

func (p *dbtLogParser) emit(msg string) {
	fmt.Fprintln(p.dest, msg)
}

// ── Line handling ────────────────────────────────────────────────

func (p *dbtLogParser) handleLine(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}

	// Non-JSON passthrough
	if line[0] != '{' {
		p.emit(string(line))
		return
	}

	event, err := parseDBTLine(line)
	if err != nil {
		p.emit(string(line))
		return
	}

	if event.Level == "debug" {
		return
	}

	p.handleEvent(event)
}

func (p *dbtLogParser) handleEvent(event dbtEvent) {
	switch event.Code {

	// ── Header info ───────────────────────────────────────────
	case "A001": // MainReportVersion
		ver := event.Data.V
		if ver == "" {
			// Extract from msg as fallback
			ver = event.Msg
		}
		// Don't emit yet — wait for Q026 to build a combined header
		// Store version for later (we just emit it directly for simplicity)
		p.emit(event.Msg)

	case "W006": // FoundStats
		stat := event.Data.StatLine
		if stat == "" {
			stat = event.Msg
		}
		p.emit(stat)

	case "Q026": // ConcurrencyLine
		p.emit(event.Msg)
		p.emit("") // blank line before model output

	// ── Node started ──────────────────────────────────────────
	case "Q033": // LogStartLine
		p.mu.Lock()
		if event.Data.Total > 0 {
			p.total = event.Data.Total
		}
		name := event.Data.NodeInfo.resolvedName()
		uid := event.Data.NodeInfo.UniqueID

		// Parse node_started_at; fall back to event timestamp or now
		startedAt := event.Ts
		if ts := event.Data.NodeInfo.NodeStartedAt; ts != "" {
			startedAt = parseTimestamp(ts)
		}

		p.running = append(p.running, runningNode{name: name, uniqueID: uid, startedAt: startedAt})
		p.mu.Unlock()
		// Don't emit anything — we'll show it when something finishes

	// ── Model completed ───────────────────────────────────────
	case "Q012": // LogModelResult
		p.mu.Lock()
		p.finished++

		name := event.Data.NodeInfo.resolvedName()
		uid := event.Data.NodeInfo.UniqueID
		p.removeRunning(uid, name)

		progress := fmt.Sprintf("[%d/%d]", p.finished, p.total)
		still := p.runningStatus(event.Ts)
		p.mu.Unlock()

		mat := event.Data.NodeInfo.Materialized
		if mat != "" {
			mat = " " + mat
		}

		icon := "✓"
		status := event.Data.Status
		if status == "error" {
			icon = "✗"
		}

		line := fmt.Sprintf("%s %s %s%s (%s in %.1fs)",
			progress, icon, name, mat, status, event.Data.ExecutionTime)

		if len(still) > 0 {
			line += "  |  Running: " + strings.Join(still, ", ")
		}
		p.emit(line)

	// ── Test completed ────────────────────────────────────────
	case "Q035": // LogTestResult
		p.mu.Lock()
		p.finished++

		name := event.Data.NodeInfo.resolvedName()
		uid := event.Data.NodeInfo.UniqueID
		p.removeRunning(uid, name)

		progress := fmt.Sprintf("[%d/%d]", p.finished, p.total)
		still := p.runningStatus(event.Ts)
		p.mu.Unlock()

		icon := "✓"
		if event.Data.Status == "fail" || event.Data.Status == "error" {
			icon = "✗"
		} else if event.Data.Status == "warn" {
			icon = "⚠"
		}

		line := fmt.Sprintf("%s %s %s (%s, %.1fs)",
			progress, icon, name, event.Data.Status, event.Data.ExecutionTime)

		if len(still) > 0 {
			line += "  |  Running: " + strings.Join(still, ", ")
		}
		p.emit(line)

	// ── Source freshness ──────────────────────────────────────
	case "Q037": // LogFreshnessResult
		p.mu.Lock()
		p.finished++

		name := event.Data.Source.Name
		if name == "" {
			name = event.Data.Source.SourceName
		}
		p.removeRunning("", name)

		progress := fmt.Sprintf("[%d/%d]", p.finished, p.total)
		still := p.runningStatus(event.Ts)
		p.mu.Unlock()

		icon := "✓"
		if event.Data.Status == "stale" || event.Data.Status == "error" {
			icon = "✗"
		}

		line := fmt.Sprintf("%s %s %s [%s]", progress, icon, name, event.Data.Status)
		if len(still) > 0 {
			line += "  |  Running: " + strings.Join(still, ", ")
		}
		p.emit(line)

	// ── Run summary ───────────────────────────────────────────
	case "E040": // EndOfRunSummary
		p.emit("")
		p.emit(event.Msg)

	case "Z030": // CommandCompleted
		p.emit(event.Msg)

	case "Z023": // RunResultTotal
		p.emit(event.Msg)

	// ── Errors ────────────────────────────────────────────────
	case "E001", "E002", "E003", "E004", "E005":
		msg := event.Data.Msg
		if msg == "" {
			msg = event.Msg
		}
		p.emit(fmt.Sprintf("  ERROR: %s", msg))

	// ── Skip uninteresting events ─────────────────────────────
	case "I030": // PartialParseNotFound
		// skip

	default:
		// For any unrecognized event, fall back to msg if it's info/warn/error
		if event.Level != "debug" && event.Msg != "" {
			p.emit(event.Msg)
		}
	}
}

// removeRunning removes a node from the running list by unique_id or name.
// Must be called with p.mu held.
func (p *dbtLogParser) removeRunning(uid, name string) {
	for i, r := range p.running {
		if (uid != "" && r.uniqueID == uid) || (name != "" && r.name == name) {
			p.running = append(p.running[:i], p.running[i+1:]...)
			return
		}
	}
}

// runningStatus returns a formatted list of in-flight nodes with elapsed times.
// Must be called with p.mu held.
func (p *dbtLogParser) runningStatus(now time.Time) []string {
	entries := make([]string, 0, len(p.running))
	for _, r := range p.running {
		if r.name != "" {
			elapsed := now.Sub(r.startedAt).Seconds()
			entries = append(entries, fmt.Sprintf("%s (%.0fs)", r.name, elapsed))
		}
	}
	return entries
}

// ── JSON parsing (handles both log_version 2 and 3) ──────────────

// parseTimestamp tries common dbt timestamp formats.
func parseTimestamp(s string) time.Time {
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999Z",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		time.RFC3339,
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Now()
}

func parseDBTLine(line []byte) (dbtEvent, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return dbtEvent{}, err
	}

	var event dbtEvent

	if infoRaw, ok := raw["info"]; ok && len(infoRaw) > 0 && infoRaw[0] == '{' {
		// log_version 3 (dbt 1.5+): nested format
		var info struct {
			Name  string `json:"name"`
			Code  string `json:"code"`
			Msg   string `json:"msg"`
			Level string `json:"level"`
			Ts    string `json:"ts"`
		}
		if err := json.Unmarshal(infoRaw, &info); err != nil {
			return dbtEvent{}, err
		}
		event.Code = info.Code
		event.Name = info.Name
		event.Msg = info.Msg
		event.Level = info.Level
		event.Ts = parseTimestamp(info.Ts)
	} else {
		// log_version 2 (dbt 1.3–1.4): flat format
		var flat struct {
			Code  string `json:"code"`
			Msg   string `json:"msg"`
			Level string `json:"level"`
			Ts    string `json:"ts"`
		}
		if err := json.Unmarshal(line, &flat); err != nil {
			return dbtEvent{}, err
		}
		event.Code = flat.Code
		event.Msg = flat.Msg
		event.Level = flat.Level
		event.Ts = parseTimestamp(flat.Ts)
	}

	if dataRaw, ok := raw["data"]; ok {
		_ = json.Unmarshal(dataRaw, &event.Data)
	}

	return event, nil
}
