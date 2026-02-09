package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGenerateRunID(t *testing.T) {
	id := GenerateRunID("my_dag")

	// Should end with _my_dag
	if !strings.HasSuffix(id, "_my_dag") {
		t.Errorf("GenerateRunID('my_dag') = %q, want suffix '_my_dag'", id)
	}

	// Should contain a dot for milliseconds
	if !strings.Contains(id, ".") {
		t.Errorf("GenerateRunID() = %q, want millisecond precision (contains '.')", id)
	}

	// Two IDs generated in sequence should differ (millisecond precision)
	id2 := GenerateRunID("my_dag")
	if id == id2 {
		// They could collide in theory, but very unlikely
		t.Logf("warning: two sequential RunIDs are identical: %s", id)
	}
}

func TestTopoSort_Linear(t *testing.T) {
	tasks := []*TaskInstance{
		{Name: "a", DependsOn: nil},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}

	levels, err := topoSort(tasks)
	if err != nil {
		t.Fatalf("topoSort() error: %v", err)
	}
	if len(levels) != 3 {
		t.Fatalf("len(levels) = %d, want 3", len(levels))
	}
	if levels[0][0].Name != "a" {
		t.Errorf("level 0 = %q, want 'a'", levels[0][0].Name)
	}
	if levels[1][0].Name != "b" {
		t.Errorf("level 1 = %q, want 'b'", levels[1][0].Name)
	}
	if levels[2][0].Name != "c" {
		t.Errorf("level 2 = %q, want 'c'", levels[2][0].Name)
	}
}

func TestTopoSort_Parallel(t *testing.T) {
	tasks := []*TaskInstance{
		{Name: "a", DependsOn: nil},
		{Name: "b", DependsOn: nil},
		{Name: "c", DependsOn: []string{"a", "b"}},
	}

	levels, err := topoSort(tasks)
	if err != nil {
		t.Fatalf("topoSort() error: %v", err)
	}
	if len(levels) != 2 {
		t.Fatalf("len(levels) = %d, want 2", len(levels))
	}
	if len(levels[0]) != 2 {
		t.Errorf("level 0 has %d tasks, want 2", len(levels[0]))
	}
	if len(levels[1]) != 1 || levels[1][0].Name != "c" {
		t.Errorf("level 1 = %v, want [c]", taskNames(levels[1]))
	}
}

func TestTopoSort_Cycle(t *testing.T) {
	tasks := []*TaskInstance{
		{Name: "a", DependsOn: []string{"c"}},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}

	_, err := topoSort(tasks)
	if err == nil {
		t.Fatal("topoSort() expected cycle error, got nil")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("topoSort() error = %q, want it to contain 'cycle'", err)
	}
}

func TestTopoSort_SingleTask(t *testing.T) {
	tasks := []*TaskInstance{
		{Name: "only"},
	}

	levels, err := topoSort(tasks)
	if err != nil {
		t.Fatalf("topoSort() error: %v", err)
	}
	if len(levels) != 1 {
		t.Fatalf("len(levels) = %d, want 1", len(levels))
	}
	if levels[0][0].Name != "only" {
		t.Errorf("level 0 = %q, want 'only'", levels[0][0].Name)
	}
}

func TestHasUpstreamFailure(t *testing.T) {
	statusMap := map[string]TaskStatus{
		"a": StatusSuccess,
		"b": StatusFailed,
		"c": StatusUpstreamFailed,
		"d": StatusPending,
	}

	tests := []struct {
		name      string
		dependsOn []string
		want      bool
	}{
		{name: "no deps", dependsOn: nil, want: false},
		{name: "all success", dependsOn: []string{"a"}, want: false},
		{name: "failed dep", dependsOn: []string{"a", "b"}, want: true},
		{name: "upstream_failed dep", dependsOn: []string{"c"}, want: true},
		{name: "pending dep", dependsOn: []string{"d"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ti := &TaskInstance{Name: "target", DependsOn: tt.dependsOn}
			got := hasUpstreamFailure(ti, statusMap)
			if got != tt.want {
				t.Errorf("hasUpstreamFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrintSummary(t *testing.T) {
	now := time.Now()
	run := &Run{
		ID:        "20240115_143022.123_test",
		DAGName:   "test",
		Status:    StatusSuccess,
		StartedAt: now,
		EndedAt:   now.Add(5 * time.Second),
		Tasks: []*TaskInstance{
			{
				Name:      "a",
				Status:    StatusSuccess,
				StartedAt: now,
				EndedAt:   now.Add(2 * time.Second),
			},
			{
				Name:    "b",
				Status:  StatusFailed,
				Error:   os.ErrNotExist,
				Attempt: 2,
				MaxRetries: 1,
				StartedAt: now.Add(2 * time.Second),
				EndedAt:   now.Add(4 * time.Second),
			},
		},
	}

	var buf bytes.Buffer
	printSummary(&buf, run)
	output := buf.String()

	// Should contain run ID
	if !strings.Contains(output, "20240115_143022.123_test") {
		t.Errorf("printSummary() missing run ID in output")
	}
	// Should contain DAG name
	if !strings.Contains(output, "test") {
		t.Errorf("printSummary() missing DAG name in output")
	}
	// Should show task statuses
	if !strings.Contains(output, "success") {
		t.Errorf("printSummary() missing 'success' status")
	}
	if !strings.Contains(output, "failed") {
		t.Errorf("printSummary() missing 'failed' status")
	}
	// Should show retry info
	if !strings.Contains(output, "attempt 2/2") {
		t.Errorf("printSummary() missing retry info, got: %s", output)
	}
}

func TestPrefixWriter(t *testing.T) {
	var buf bytes.Buffer
	pw := &prefixWriter{
		prefix: []byte("[task] "),
		dest:   &buf,
	}

	pw.Write([]byte("hello world\n"))
	pw.Write([]byte("second line\n"))

	got := buf.String()
	want := "[task] hello world\n[task] second line\n"
	if got != want {
		t.Errorf("prefixWriter output = %q, want %q", got, want)
	}
}

func TestPrefixWriter_PartialLines(t *testing.T) {
	var buf bytes.Buffer
	pw := &prefixWriter{
		prefix: []byte("[x] "),
		dest:   &buf,
	}

	// Write partial line, then complete it
	pw.Write([]byte("hel"))
	pw.Write([]byte("lo\n"))

	got := buf.String()
	want := "[x] hello\n"
	if got != want {
		t.Errorf("prefixWriter partial output = %q, want %q", got, want)
	}
}

func TestPrefixWriter_MultipleLines(t *testing.T) {
	var buf bytes.Buffer
	pw := &prefixWriter{
		prefix: []byte("[t] "),
		dest:   &buf,
	}

	// Write multiple lines in a single call
	pw.Write([]byte("line1\nline2\nline3\n"))

	got := buf.String()
	want := "[t] line1\n[t] line2\n[t] line3\n"
	if got != want {
		t.Errorf("prefixWriter multi-line output = %q, want %q", got, want)
	}
}

func TestSnapshot(t *testing.T) {
	runsDir := t.TempDir()
	srcDir := filepath.Join("testdata", "sample_project")

	snapshotDir, logDir, dataDir, err := Snapshot(srcDir, runsDir, "test_run_001")
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	// Check snapshot dir was created with pit.toml
	pitToml := filepath.Join(snapshotDir, "pit.toml")
	if _, err := os.Stat(pitToml); err != nil {
		t.Errorf("snapshot missing pit.toml: %v", err)
	}

	// Check task script was copied
	script := filepath.Join(snapshotDir, "tasks", "hello.sh")
	if _, err := os.Stat(script); err != nil {
		t.Errorf("snapshot missing tasks/hello.sh: %v", err)
	}

	// Check log dir was created
	if _, err := os.Stat(logDir); err != nil {
		t.Errorf("log dir not created: %v", err)
	}

	// Check data dir was created
	if _, err := os.Stat(dataDir); err != nil {
		t.Errorf("data dir not created: %v", err)
	}

	// Verify data dir is under the run directory
	if !strings.Contains(dataDir, "test_run_001") {
		t.Errorf("dataDir = %q, want it to contain run ID", dataDir)
	}
}

func TestSnapshot_SkipsDirs(t *testing.T) {
	// Create a source dir with skippable directories
	srcDir := t.TempDir()
	for _, d := range []string{"tasks", ".git", "__pycache__", ".venv", "node_modules"} {
		os.MkdirAll(filepath.Join(srcDir, d), 0o755)
		os.WriteFile(filepath.Join(srcDir, d, "file.txt"), []byte("content"), 0o644)
	}
	os.WriteFile(filepath.Join(srcDir, "pit.toml"), []byte("[dag]\nname = \"test\"\n"), 0o644)

	runsDir := t.TempDir()
	snapshotDir, _, _, err := Snapshot(srcDir, runsDir, "skip_test")
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	// tasks/ should be copied
	if _, err := os.Stat(filepath.Join(snapshotDir, "tasks", "file.txt")); err != nil {
		t.Error("snapshot should contain tasks/file.txt")
	}

	// Skip dirs should NOT be copied
	for _, d := range []string{".git", "__pycache__", ".venv", "node_modules"} {
		if _, err := os.Stat(filepath.Join(snapshotDir, d)); err == nil {
			t.Errorf("snapshot should not contain %s", d)
		}
	}
}

func TestCopyFile_PreservesPermissions(t *testing.T) {
	src := filepath.Join(t.TempDir(), "script.sh")
	if err := os.WriteFile(src, []byte("#!/bin/bash\necho hi"), 0o755); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(t.TempDir(), "script.sh")
	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile() error: %v", err)
	}

	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Errorf("copied file mode = %o, want 755", info.Mode().Perm())
	}
}

func TestCopyDirContents(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create some files in srcDir
	os.WriteFile(filepath.Join(srcDir, "file1.csv"), []byte("data1"), 0o644)
	os.WriteFile(filepath.Join(srcDir, "file2.csv"), []byte("data2"), 0o644)
	os.MkdirAll(filepath.Join(srcDir, "subdir"), 0o755)
	os.WriteFile(filepath.Join(srcDir, "subdir", "nested.txt"), []byte("nested"), 0o644)

	if err := copyDirContents(srcDir, dstDir); err != nil {
		t.Fatalf("copyDirContents() error: %v", err)
	}

	// Check files were copied
	for _, name := range []string{"file1.csv", "file2.csv"} {
		data, err := os.ReadFile(filepath.Join(dstDir, name))
		if err != nil {
			t.Errorf("missing %s: %v", name, err)
			continue
		}
		if name == "file1.csv" && string(data) != "data1" {
			t.Errorf("%s content = %q, want %q", name, data, "data1")
		}
	}

	// Check nested file
	data, err := os.ReadFile(filepath.Join(dstDir, "subdir", "nested.txt"))
	if err != nil {
		t.Fatalf("missing subdir/nested.txt: %v", err)
	}
	if string(data) != "nested" {
		t.Errorf("nested.txt content = %q, want %q", data, "nested")
	}
}

func TestCopyDirContents_EmptySource(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	if err := copyDirContents(srcDir, dstDir); err != nil {
		t.Fatalf("copyDirContents() error: %v", err)
	}

	entries, _ := os.ReadDir(dstDir)
	if len(entries) != 0 {
		t.Errorf("dstDir has %d entries, want 0", len(entries))
	}
}

// taskNames extracts names from a slice of TaskInstances.
func taskNames(tasks []*TaskInstance) []string {
	names := make([]string, len(tasks))
	for i, t := range tasks {
		names[i] = t.Name
	}
	return names
}
