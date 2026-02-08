package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validTOML = `
[global]
smtp_password = "global_smtp"
shared_key = "global_shared"

[claims_pipeline]
claims_db = "Server=claims;User Id=sa;Password=secret"
shared_key = "project_shared"
`

func writeSecretsFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "secrets.toml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("writing secrets file: %v", err)
	}
	return path
}

func TestLoad_Valid(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("Load() returned nil store for valid file")
	}
}

func TestLoad_EmptyPath(t *testing.T) {
	store, err := Load("")
	if err != nil {
		t.Fatalf("Load('') unexpected error: %v", err)
	}
	if store != nil {
		t.Error("Load('') should return nil store")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/secrets.toml")
	if err == nil {
		t.Error("Load() expected error for missing file, got nil")
	}
}

func TestLoad_InvalidTOML(t *testing.T) {
	path := writeSecretsFile(t, "not valid toml [[[")
	_, err := Load(path)
	if err == nil {
		t.Error("Load() expected error for invalid TOML, got nil")
	}
}

func TestResolve_ProjectScoped(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("claims_pipeline", "claims_db")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if val != "Server=claims;User Id=sa;Password=secret" {
		t.Errorf("Resolve() = %q, want claims connection string", val)
	}
}

func TestResolve_GlobalFallback(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("claims_pipeline", "smtp_password")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if val != "global_smtp" {
		t.Errorf("Resolve() = %q, want %q", val, "global_smtp")
	}
}

func TestResolve_ProjectOverridesGlobal(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("claims_pipeline", "shared_key")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if val != "project_shared" {
		t.Errorf("Resolve() = %q, want %q (project should override global)", val, "project_shared")
	}
}

func TestResolve_UnknownProjectFallsToGlobal(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("unknown_project", "smtp_password")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if val != "global_smtp" {
		t.Errorf("Resolve() = %q, want %q", val, "global_smtp")
	}
}

func TestResolve_MissingKey(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	_, err = store.Resolve("claims_pipeline", "nonexistent")
	if err == nil {
		t.Error("Resolve() expected error for missing key, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error = %q, want it to contain %q", err, "nonexistent")
	}
}

func TestResolve_EmptyProjectSection(t *testing.T) {
	tomlContent := `
[global]
api_key = "global_api"

[empty_project]
`
	path := writeSecretsFile(t, tomlContent)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("empty_project", "api_key")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}
	if val != "global_api" {
		t.Errorf("Resolve() = %q, want %q (empty project should fall through to global)", val, "global_api")
	}
}
