//go:build integration

package gitrepo

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// mkBareRepo creates an initialised bare git repo with one commit on branch "main"
// and returns its path. Used as the "remote" in tests.
func mkBareRepo(t *testing.T, fileName, fileContent string) string {
	t.Helper()

	// Create a working repo, commit a file, then clone it as bare.
	work := t.TempDir()
	mustGit(t, "", "init", "-b", "main", work)
	mustGit(t, work, "config", "user.email", "test@example.com")
	mustGit(t, work, "config", "user.name", "Test")

	if err := os.WriteFile(filepath.Join(work, fileName), []byte(fileContent), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	mustGit(t, work, "add", ".")
	mustGit(t, work, "commit", "-m", "initial commit")

	bare := t.TempDir()
	mustGit(t, "", "clone", "--bare", work, bare)
	return bare
}

// addCommit adds a new file to a non-bare working repo and commits it.
func addCommit(t *testing.T, repoDir, fileName, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repoDir, fileName), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	mustGit(t, repoDir, "add", ".")
	mustGit(t, repoDir, "commit", "-m", "add "+fileName)
}

// mustGit runs a git command, failing the test if it errors.
func mustGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	if dir != "" {
		args = append([]string{"-C", dir}, args...)
	}
	cmd := exec.Command("git", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func TestPrepare_Clone(t *testing.T) {
	remote := mkBareRepo(t, "hello.txt", "hello world\n")
	cacheDir := filepath.Join(t.TempDir(), "cache")

	if err := Prepare(remote, "main", cacheDir); err != nil {
		t.Fatalf("Prepare() error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(cacheDir, "hello.txt"))
	if err != nil {
		t.Fatalf("expected hello.txt to exist after clone: %v", err)
	}
	if string(data) != "hello world\n" {
		t.Errorf("hello.txt = %q, want %q", data, "hello world\n")
	}
}

func TestPrepare_FetchUpdates(t *testing.T) {
	remote := mkBareRepo(t, "v1.txt", "version 1\n")
	cacheDir := filepath.Join(t.TempDir(), "cache")

	// Initial clone.
	if err := Prepare(remote, "main", cacheDir); err != nil {
		t.Fatalf("initial Prepare() error: %v", err)
	}

	// Push a new commit to the bare remote via a temp working clone.
	work := t.TempDir()
	mustGit(t, "", "clone", remote, work)
	mustGit(t, work, "config", "user.email", "test@example.com")
	mustGit(t, work, "config", "user.name", "Test")
	addCommit(t, work, "v2.txt", "version 2\n")
	mustGit(t, work, "push", "origin", "main")

	// Prepare again — should fetch and advance.
	if err := Prepare(remote, "main", cacheDir); err != nil {
		t.Fatalf("second Prepare() error: %v", err)
	}

	if _, err := os.Stat(filepath.Join(cacheDir, "v2.txt")); err != nil {
		t.Errorf("expected v2.txt to exist after fetch update: %v", err)
	}
}

func TestPrepare_InvalidURL(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	err := Prepare("/no/such/repo/exists", "main", cacheDir)
	if err == nil {
		t.Fatal("Prepare() expected error for invalid URL, got nil")
	}
	if !strings.Contains(err.Error(), "git clone") {
		t.Errorf("error = %q, want it to mention 'git clone'", err)
	}
}

func TestPrepare_InvalidRef(t *testing.T) {
	remote := mkBareRepo(t, "file.txt", "content\n")
	cacheDir := filepath.Join(t.TempDir(), "cache")

	err := Prepare(remote, "nonexistent-branch-xyz", cacheDir)
	if err == nil {
		t.Fatal("Prepare() expected error for invalid ref, got nil")
	}
	if !strings.Contains(err.Error(), "git checkout") {
		t.Errorf("error = %q, want it to mention 'git checkout'", err)
	}
}
