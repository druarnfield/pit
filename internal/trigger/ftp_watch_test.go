package trigger

import (
	"sort"
	"testing"
	"time"

	"github.com/druarnfield/pit/internal/config"
)

func TestFindStableFiles_Empty(t *testing.T) {
	tracking := map[string]fileState{}
	got := FindStableFiles(tracking, 30*time.Second, time.Now())
	if len(got) != 0 {
		t.Errorf("FindStableFiles() = %v, want empty", got)
	}
}

func TestFindStableFiles_AllStable(t *testing.T) {
	now := time.Now()
	tracking := map[string]fileState{
		"file_a.csv": {Size: 100, FirstSeen: now.Add(-60 * time.Second)},
		"file_b.csv": {Size: 200, FirstSeen: now.Add(-45 * time.Second)},
	}

	got := FindStableFiles(tracking, 30*time.Second, now)
	sort.Strings(got)
	if len(got) != 2 {
		t.Fatalf("FindStableFiles() returned %d files, want 2", len(got))
	}
	if got[0] != "file_a.csv" || got[1] != "file_b.csv" {
		t.Errorf("FindStableFiles() = %v, want [file_a.csv, file_b.csv]", got)
	}
}

func TestFindStableFiles_NoneStable(t *testing.T) {
	now := time.Now()
	tracking := map[string]fileState{
		"file_a.csv": {Size: 100, FirstSeen: now.Add(-10 * time.Second)},
		"file_b.csv": {Size: 200, FirstSeen: now.Add(-5 * time.Second)},
	}

	got := FindStableFiles(tracking, 30*time.Second, now)
	if len(got) != 0 {
		t.Errorf("FindStableFiles() = %v, want empty", got)
	}
}

func TestFindStableFiles_Mixed(t *testing.T) {
	now := time.Now()
	tracking := map[string]fileState{
		"old_file.csv": {Size: 100, FirstSeen: now.Add(-60 * time.Second)},
		"new_file.csv": {Size: 200, FirstSeen: now.Add(-5 * time.Second)},
	}

	got := FindStableFiles(tracking, 30*time.Second, now)
	if len(got) != 1 {
		t.Fatalf("FindStableFiles() returned %d files, want 1", len(got))
	}
	if got[0] != "old_file.csv" {
		t.Errorf("FindStableFiles() = %v, want [old_file.csv]", got)
	}
}

func TestFindStableFiles_ExactThreshold(t *testing.T) {
	now := time.Now()
	tracking := map[string]fileState{
		"exact.csv": {Size: 100, FirstSeen: now.Add(-30 * time.Second)},
	}

	got := FindStableFiles(tracking, 30*time.Second, now)
	if len(got) != 1 {
		t.Fatalf("FindStableFiles() returned %d files, want 1 (exact threshold)", len(got))
	}
}

func TestFindStableFiles_JustUnderThreshold(t *testing.T) {
	now := time.Now()
	tracking := map[string]fileState{
		"almost.csv": {Size: 100, FirstSeen: now.Add(-29 * time.Second)},
	}

	got := FindStableFiles(tracking, 30*time.Second, now)
	if len(got) != 0 {
		t.Errorf("FindStableFiles() = %v, want empty (just under threshold)", got)
	}
}

func TestNewFTPWatchTrigger_NilSecrets(t *testing.T) {
	_, err := NewFTPWatchTrigger("test", &config.FTPWatchConfig{
		PasswordSecret: "pass",
	}, nil)
	if err == nil {
		t.Error("NewFTPWatchTrigger() expected error for nil secrets, got nil")
	}
}
