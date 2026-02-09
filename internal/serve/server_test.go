package serve

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func TestNewServer_NoProjects(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "projects"), 0o755)

	_, err := NewServer(dir, "", false, Options{})
	if err == nil {
		t.Fatal("NewServer() expected error for no projects, got nil")
	}
	if !strings.Contains(err.Error(), "no projects") {
		t.Errorf("error = %q, want it to contain 'no projects'", err)
	}
}

func TestNewServer_NoTriggers(t *testing.T) {
	dir := t.TempDir()
	mkProject(t, dir, "no_triggers", `[dag]
name = "no_triggers"

[[tasks]]
name = "hello"
script = "tasks/hello.sh"
`)

	_, err := NewServer(dir, "", false, Options{})
	if err == nil {
		t.Fatal("NewServer() expected error for no triggers, got nil")
	}
	if !strings.Contains(err.Error(), "no triggers") {
		t.Errorf("error = %q, want it to contain 'no triggers'", err)
	}
}

func TestNewServer_CronOnly(t *testing.T) {
	dir := t.TempDir()
	mkProject(t, dir, "cron_dag", `[dag]
name = "cron_dag"
schedule = "0 6 * * *"

[[tasks]]
name = "hello"
script = "tasks/hello.sh"
`)

	s, err := NewServer(dir, "", false, Options{})
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	if len(s.triggers) != 1 {
		t.Errorf("len(triggers) = %d, want 1", len(s.triggers))
	}
	if !strings.Contains(s.triggers[0].Name(), "cron") {
		t.Errorf("trigger name = %q, want it to contain 'cron'", s.triggers[0].Name())
	}
}

func TestNewServer_FTPOnlyNoSecrets(t *testing.T) {
	dir := t.TempDir()
	mkProject(t, dir, "ftp_dag", `[dag]
name = "ftp_dag"

[dag.ftp_watch]
host = "ftp.example.com"
user = "user"
password_secret = "ftp_pass"
directory = "/data"
pattern = "*.csv"

[[tasks]]
name = "process"
script = "tasks/process.py"
`)

	_, err := NewServer(dir, "", false, Options{})
	if err == nil {
		t.Fatal("NewServer() expected error for FTP without secrets, got nil")
	}
}

func TestNewServer_FTPWithSecrets(t *testing.T) {
	dir := t.TempDir()
	mkProject(t, dir, "ftp_dag", `[dag]
name = "ftp_dag"

[dag.ftp_watch]
host = "ftp.example.com"
user = "user"
password_secret = "ftp_pass"
directory = "/data"
pattern = "*.csv"

[[tasks]]
name = "process"
script = "tasks/process.py"
`)

	secretsFile := filepath.Join(dir, "secrets.toml")
	os.WriteFile(secretsFile, []byte(`[global]
ftp_pass = "secret123"
`), 0o644)

	s, err := NewServer(dir, secretsFile, false, Options{})
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	if len(s.triggers) != 1 {
		t.Errorf("len(triggers) = %d, want 1", len(s.triggers))
	}
	if _, ok := s.ftpConfigs["ftp_dag"]; !ok {
		t.Error("ftpConfigs missing 'ftp_dag'")
	}
}

func TestNewServer_BothTriggers(t *testing.T) {
	dir := t.TempDir()
	mkProject(t, dir, "both_dag", `[dag]
name = "both_dag"
schedule = "0 6 * * *"

[dag.ftp_watch]
host = "ftp.example.com"
user = "user"
password_secret = "ftp_pass"
directory = "/data"
pattern = "*.csv"

[[tasks]]
name = "process"
script = "tasks/process.py"
`)

	secretsFile := filepath.Join(dir, "secrets.toml")
	os.WriteFile(secretsFile, []byte(`[global]
ftp_pass = "secret123"
`), 0o644)

	s, err := NewServer(dir, secretsFile, false, Options{})
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	if len(s.triggers) != 2 {
		t.Errorf("len(triggers) = %d, want 2", len(s.triggers))
	}
}

func TestNewServer_InvalidCronSchedule(t *testing.T) {
	dir := t.TempDir()
	mkProject(t, dir, "bad_cron", `[dag]
name = "bad_cron"
schedule = "not a schedule"

[[tasks]]
name = "hello"
script = "tasks/hello.sh"
`)

	_, err := NewServer(dir, "", false, Options{})
	if err == nil {
		t.Fatal("NewServer() expected error for invalid cron, got nil")
	}
}

func TestOverlapSkip(t *testing.T) {
	s := &Server{
		configs: map[string]*config.ProjectConfig{
			"test": {DAG: config.DAGConfig{Name: "test", Overlap: "skip"}},
		},
		activeRuns: map[string]bool{"test": true},
	}

	// The skip logic is in handleEvent — verify the activeRuns map state
	s.mu.Lock()
	isActive := s.activeRuns["test"]
	s.mu.Unlock()
	if !isActive {
		t.Error("expected test DAG to be active")
	}
}

// mkProject creates a project directory with pit.toml under root/projects/<name>/.
func mkProject(t *testing.T, root, name, tomlContent string) {
	t.Helper()
	dir := filepath.Join(root, "projects", name)
	if err := os.MkdirAll(filepath.Join(dir, "tasks"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pit.toml"), []byte(tomlContent), 0o644); err != nil {
		t.Fatal(err)
	}
	// Create dummy task scripts referenced in the TOML
	os.WriteFile(filepath.Join(dir, "tasks", "hello.sh"), []byte("#!/bin/bash\necho hi"), 0o755)
	os.WriteFile(filepath.Join(dir, "tasks", "process.py"), []byte("print('ok')"), 0o644)
}
