package secrets

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveIdentity_EnvVarFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key.txt")
	if err := os.WriteFile(keyFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PIT_AGE_KEY_FILE", keyFile)
	t.Setenv("PIT_AGE_KEY", "")

	got, err := ResolveIdentityPath("")
	if err != nil {
		t.Fatalf("ResolveIdentityPath() error: %v", err)
	}
	if got != keyFile {
		t.Errorf("ResolveIdentityPath() = %q, want %q", got, keyFile)
	}
}

func TestResolveIdentity_ConfigOverride(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "custom.txt")
	if err := os.WriteFile(keyFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PIT_AGE_KEY", "")
	t.Setenv("PIT_AGE_KEY_FILE", "")

	got, err := ResolveIdentityPath(keyFile)
	if err != nil {
		t.Fatalf("ResolveIdentityPath() error: %v", err)
	}
	if got != keyFile {
		t.Errorf("ResolveIdentityPath() = %q, want %q", got, keyFile)
	}
}

func TestResolveIdentity_MissingFile(t *testing.T) {
	t.Setenv("PIT_AGE_KEY", "")
	t.Setenv("PIT_AGE_KEY_FILE", "")

	_, err := ResolveIdentityPath("/nonexistent/path.txt")
	if err == nil {
		t.Error("ResolveIdentityPath() expected error for missing file, got nil")
	}
}

func TestResolveIdentityRaw_EnvVar(t *testing.T) {
	t.Setenv("PIT_AGE_KEY", "AGE-SECRET-KEY-1SOMETHING")

	got := ResolveIdentityRaw()
	if got != "AGE-SECRET-KEY-1SOMETHING" {
		t.Errorf("ResolveIdentityRaw() = %q, want env value", got)
	}
}

func TestResolveIdentityRaw_Empty(t *testing.T) {
	t.Setenv("PIT_AGE_KEY", "")

	got := ResolveIdentityRaw()
	if got != "" {
		t.Errorf("ResolveIdentityRaw() = %q, want empty", got)
	}
}
