package engine

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// RunInfo holds metadata about a discovered run on disk.
type RunInfo struct {
	ID        string
	DAGName   string
	Timestamp time.Time
	Dir       string // full path to the run directory (e.g. runs/<runID>)
	LogDir    string // full path to the logs directory (e.g. runs/<runID>/logs)
}

// runIDTimestampLen is the length of the timestamp portion of a run ID
// (format: 20060102_150405.000 = 19 chars) plus the trailing underscore separator.
const runIDTimestampLen = 20

// DAGNameFromRunID extracts the DAG name from a run ID.
// Run IDs have the format: 20060102_150405.000_dag_name
// The timestamp portion is always 19 chars, followed by an underscore.
func DAGNameFromRunID(runID string) (string, error) {
	if len(runID) <= runIDTimestampLen {
		return "", fmt.Errorf("run ID %q is too short to contain a DAG name", runID)
	}
	return runID[runIDTimestampLen:], nil
}

// TimestampFromRunID parses the timestamp portion of a run ID.
func TimestampFromRunID(runID string) (time.Time, error) {
	if len(runID) < runIDTimestampLen {
		return time.Time{}, fmt.Errorf("run ID %q is too short to contain a timestamp", runID)
	}
	ts := runID[:runIDTimestampLen-1] // exclude trailing underscore
	return time.ParseInLocation("20060102_150405.000", ts, time.Local)
}

// DiscoverRuns scans the runsDir for run directories belonging to the given DAG.
// If dagName is empty, all runs are returned.
// Returns runs sorted newest-first. Returns an empty slice (not error) if the
// runs directory doesn't exist.
func DiscoverRuns(runsDir, dagName string) ([]RunInfo, error) {
	entries, err := os.ReadDir(runsDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading runs directory: %w", err)
	}

	var runs []RunInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()

		// Parse the run ID to extract DAG name and timestamp
		dag, err := DAGNameFromRunID(name)
		if err != nil {
			continue // skip non-run directories
		}

		if dagName != "" && dag != dagName {
			continue
		}

		ts, err := TimestampFromRunID(name)
		if err != nil {
			continue
		}

		runDir := filepath.Join(runsDir, name)
		runs = append(runs, RunInfo{
			ID:        name,
			DAGName:   dag,
			Timestamp: ts,
			Dir:       runDir,
			LogDir:    filepath.Join(runDir, "logs"),
		})
	}

	// Sort newest first
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].Timestamp.After(runs[j].Timestamp)
	})

	return runs, nil
}

// ReadTaskLog reads a single task's log file from the given log directory.
func ReadTaskLog(logDir, taskName string) ([]byte, error) {
	path := filepath.Join(logDir, taskName+".log")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("no log file for task %q", taskName)
	}
	return data, nil
}

// ReadAllTaskLogs reads all .log files in the log directory in sorted order,
// writing each with a header to the given writer.
func ReadAllTaskLogs(logDir string, w io.Writer) error {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return fmt.Errorf("reading log directory: %w", err)
	}

	var logFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".log") {
			logFiles = append(logFiles, e.Name())
		}
	}
	sort.Strings(logFiles)

	for _, name := range logFiles {
		taskName := strings.TrimSuffix(name, ".log")
		data, err := os.ReadFile(filepath.Join(logDir, name))
		if err != nil {
			return fmt.Errorf("reading log %s: %w", name, err)
		}

		fmt.Fprintf(w, "── %s ──\n", taskName)
		w.Write(data)
		if len(data) > 0 && data[len(data)-1] != '\n' {
			fmt.Fprintln(w)
		}
	}

	return nil
}
