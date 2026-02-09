package runner

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// dbtLogParser is an io.Writer that transforms dbt JSON log lines into
// human-readable output. Non-JSON lines are passed through verbatim.
type dbtLogParser struct {
	dest    io.Writer
	scanner *bufio.Scanner
	pr      *io.PipeReader
	pw      *io.PipeWriter
	done    chan struct{}
}

// dbtLogLine represents the structure of a dbt JSON log line.
type dbtLogLine struct {
	Info struct {
		Name  string `json:"name"`
		Msg   string `json:"msg"`
		Level string `json:"level"`
	} `json:"info"`
	Data dbtLogData `json:"data"`
}

type dbtLogData struct {
	Description   string  `json:"description"`
	ExecutionTime float64 `json:"execution_time"`
	RowsAffected  int64   `json:"rows_affected"`
	Status        string  `json:"status"`
	// Node fields for model/test results
	Node struct {
		Name string `json:"name"`
		Path string `json:"path"`
	} `json:"node"`
	// Source freshness fields
	Source struct {
		Name string `json:"name"`
	} `json:"source"`
	Elapsed float64 `json:"elapsed"`
}

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
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024) // allow large log lines
	for scanner.Scan() {
		line := scanner.Bytes()
		formatted := formatDBTLine(line)
		if formatted != "" {
			fmt.Fprintln(p.dest, formatted)
		}
	}
}

// formatDBTLine parses a single dbt JSON log line and returns a formatted string.
// Returns empty string for events that should be suppressed.
// Non-JSON lines are returned verbatim.
func formatDBTLine(line []byte) string {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return ""
	}

	// Not JSON? Pass through verbatim.
	if line[0] != '{' {
		return string(line)
	}

	var entry dbtLogLine
	if err := json.Unmarshal(line, &entry); err != nil {
		// Malformed JSON — pass through
		return string(line)
	}

	switch entry.Info.Name {
	case "LogStartLine":
		desc := entry.Data.Description
		if desc == "" {
			desc = entry.Info.Msg
		}
		return fmt.Sprintf("Running: %s", desc)

	case "LogModelResult":
		name := entry.Data.Node.Name
		if name == "" {
			name = entry.Data.Node.Path
		}
		return fmt.Sprintf("OK %s (%.1fs, %d rows)",
			name, entry.Data.ExecutionTime, entry.Data.RowsAffected)

	case "LogTestResult":
		name := entry.Data.Node.Name
		if name == "" {
			name = entry.Data.Node.Path
		}
		status := "PASS"
		if entry.Data.Status == "fail" || entry.Data.Status == "error" {
			status = "FAIL"
		}
		return fmt.Sprintf("%s %s (%.1fs)", status, name, entry.Data.ExecutionTime)

	case "LogFreshnessResult":
		name := entry.Data.Source.Name
		status := "FRESH"
		if entry.Data.Status == "stale" || entry.Data.Status == "error" {
			status = "STALE"
		}
		return fmt.Sprintf("%s %s", status, name)

	case "CommandCompleted":
		return fmt.Sprintf("Completed in %.1fs", entry.Data.Elapsed)

	default:
		// Skip other event types
		return ""
	}
}
