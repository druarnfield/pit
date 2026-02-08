package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDAGNameFromRunID(t *testing.T) {
	tests := []struct {
		name    string
		runID   string
		want    string
		wantErr bool
	}{
		{name: "simple name", runID: "20240115_143022.123_my_dag", want: "my_dag"},
		{name: "name with underscores", runID: "20240115_143022.123_my_cool_dag", want: "my_cool_dag"},
		{name: "too short", runID: "20240115_143022.123", wantErr: true},
		{name: "empty string", runID: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DAGNameFromRunID(tt.runID)
			if tt.wantErr {
				if err == nil {
					t.Errorf("DAGNameFromRunID(%q) expected error, got nil", tt.runID)
				}
				return
			}
			if err != nil {
				t.Fatalf("DAGNameFromRunID(%q) unexpected error: %v", tt.runID, err)
			}
			if got != tt.want {
				t.Errorf("DAGNameFromRunID(%q) = %q, want %q", tt.runID, got, tt.want)
			}
		})
	}
}

func TestTimestampFromRunID(t *testing.T) {
	runID := "20240115_143022.456_my_dag"
	got, err := TimestampFromRunID(runID)
	if err != nil {
		t.Fatalf("TimestampFromRunID(%q) error: %v", runID, err)
	}

	if got.Year() != 2024 || got.Month() != time.January || got.Day() != 15 {
		t.Errorf("date = %v, want 2024-01-15", got)
	}
	if got.Hour() != 14 || got.Minute() != 30 || got.Second() != 22 {
		t.Errorf("time = %v, want 14:30:22", got)
	}

	// Test too-short ID
	_, err = TimestampFromRunID("short")
	if err == nil {
		t.Error("TimestampFromRunID('short') expected error, got nil")
	}
}

func TestDiscoverRuns(t *testing.T) {
	runsDir := t.TempDir()
	mkRunDir(t, runsDir, "20240115_143022.123_my_dag")
	mkRunDir(t, runsDir, "20240116_100000.000_my_dag")
	mkRunDir(t, runsDir, "20240115_120000.000_other_dag")

	t.Run("filter by DAG", func(t *testing.T) {
		runs, err := DiscoverRuns(runsDir, "my_dag")
		if err != nil {
			t.Fatalf("DiscoverRuns() error: %v", err)
		}
		if len(runs) != 2 {
			t.Fatalf("len(runs) = %d, want 2", len(runs))
		}
		// Should be newest first
		if runs[0].ID != "20240116_100000.000_my_dag" {
			t.Errorf("runs[0].ID = %q, want newest first", runs[0].ID)
		}
		if runs[1].ID != "20240115_143022.123_my_dag" {
			t.Errorf("runs[1].ID = %q, want second newest", runs[1].ID)
		}
	})

	t.Run("all runs", func(t *testing.T) {
		runs, err := DiscoverRuns(runsDir, "")
		if err != nil {
			t.Fatalf("DiscoverRuns() error: %v", err)
		}
		if len(runs) != 3 {
			t.Fatalf("len(runs) = %d, want 3", len(runs))
		}
	})

	t.Run("no matches", func(t *testing.T) {
		runs, err := DiscoverRuns(runsDir, "nonexistent")
		if err != nil {
			t.Fatalf("DiscoverRuns() error: %v", err)
		}
		if len(runs) != 0 {
			t.Errorf("len(runs) = %d, want 0", len(runs))
		}
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		runs, err := DiscoverRuns(filepath.Join(runsDir, "nope"), "my_dag")
		if err != nil {
			t.Fatalf("DiscoverRuns() unexpected error: %v", err)
		}
		if runs != nil {
			t.Errorf("runs = %v, want nil", runs)
		}
	})

	t.Run("skips non-run entries", func(t *testing.T) {
		// Create a file (not dir) and a dir that doesn't parse as a run ID
		os.WriteFile(filepath.Join(runsDir, ".DS_Store"), []byte{}, 0o644)
		os.MkdirAll(filepath.Join(runsDir, "not_a_run"), 0o755)

		runs, err := DiscoverRuns(runsDir, "")
		if err != nil {
			t.Fatalf("DiscoverRuns() error: %v", err)
		}
		// Should still only find the 3 valid runs
		if len(runs) != 3 {
			t.Errorf("len(runs) = %d, want 3", len(runs))
		}
	})
}

func TestReadTaskLog(t *testing.T) {
	logDir := t.TempDir()
	os.WriteFile(filepath.Join(logDir, "extract.log"), []byte("extracted 100 rows\n"), 0o644)

	t.Run("existing task", func(t *testing.T) {
		data, err := ReadTaskLog(logDir, "extract")
		if err != nil {
			t.Fatalf("ReadTaskLog() error: %v", err)
		}
		if string(data) != "extracted 100 rows\n" {
			t.Errorf("ReadTaskLog() = %q, want %q", data, "extracted 100 rows\n")
		}
	})

	t.Run("missing task", func(t *testing.T) {
		_, err := ReadTaskLog(logDir, "nonexistent")
		if err == nil {
			t.Error("ReadTaskLog() expected error for missing task, got nil")
		}
		if !strings.Contains(err.Error(), "nonexistent") {
			t.Errorf("error = %q, want it to contain task name", err)
		}
	})
}

func TestReadAllTaskLogs(t *testing.T) {
	t.Run("multiple logs with headers", func(t *testing.T) {
		logDir := t.TempDir()
		os.WriteFile(filepath.Join(logDir, "alpha.log"), []byte("alpha output\n"), 0o644)
		os.WriteFile(filepath.Join(logDir, "bravo.log"), []byte("bravo output\n"), 0o644)
		os.WriteFile(filepath.Join(logDir, "charlie.log"), []byte("charlie output"), 0o644) // no trailing newline

		var buf bytes.Buffer
		if err := ReadAllTaskLogs(logDir, &buf); err != nil {
			t.Fatalf("ReadAllTaskLogs() error: %v", err)
		}

		got := buf.String()
		// Should contain headers in sorted order
		if !strings.Contains(got, "── alpha ──") {
			t.Error("missing alpha header")
		}
		if !strings.Contains(got, "── bravo ──") {
			t.Error("missing bravo header")
		}
		if !strings.Contains(got, "── charlie ──") {
			t.Error("missing charlie header")
		}

		// Verify sorted order: alpha should come before bravo
		alphaIdx := strings.Index(got, "── alpha ──")
		bravoIdx := strings.Index(got, "── bravo ──")
		if alphaIdx > bravoIdx {
			t.Error("logs not in sorted order: alpha should come before bravo")
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		logDir := t.TempDir()
		var buf bytes.Buffer
		if err := ReadAllTaskLogs(logDir, &buf); err != nil {
			t.Fatalf("ReadAllTaskLogs() error: %v", err)
		}
		if buf.Len() != 0 {
			t.Errorf("expected empty output, got %q", buf.String())
		}
	})

	t.Run("skips non-log files", func(t *testing.T) {
		logDir := t.TempDir()
		os.WriteFile(filepath.Join(logDir, "task.log"), []byte("output\n"), 0o644)
		os.WriteFile(filepath.Join(logDir, "metadata.json"), []byte("{}"), 0o644)

		var buf bytes.Buffer
		if err := ReadAllTaskLogs(logDir, &buf); err != nil {
			t.Fatalf("ReadAllTaskLogs() error: %v", err)
		}

		got := buf.String()
		if strings.Contains(got, "metadata") {
			t.Error("should not include non-.log files")
		}
		if !strings.Contains(got, "── task ──") {
			t.Error("missing task header")
		}
	})
}

// mkRunDir creates a run directory with a logs subdirectory.
func mkRunDir(t *testing.T, runsDir, runID string) {
	t.Helper()
	logDir := filepath.Join(runsDir, runID, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkRunDir(%q): %v", runID, err)
	}
}
