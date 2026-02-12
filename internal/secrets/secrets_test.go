package secrets

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validTOML = `
[global]
smtp_password = "global_smtp"
shared_key = "global_shared"

[global.shared_db]
host = "global-db.example.com"
port = "5432"
user = "admin"
password = "global_secret"

[claims_pipeline]
claims_db = "Server=claims;User Id=sa;Password=secret"
shared_key = "project_shared"

[claims_pipeline.ftp_creds]
host = "ftp.claims.example.com"
user = "claims_ftp"
password = "ftp_secret"
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

func TestLoad_NonStringField(t *testing.T) {
	path := writeSecretsFile(t, `
[global.bad_secret]
host = "ok"
port = 1433
`)
	_, err := Load(path)
	if err == nil {
		t.Error("Load() expected error for non-string field, got nil")
	}
	if !strings.Contains(err.Error(), "must be a string") {
		t.Errorf("error = %q, want it to mention 'must be a string'", err)
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

func TestResolve_StructuredSecretReturnsJSON(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("claims_pipeline", "ftp_creds")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}

	var fields map[string]string
	if err := json.Unmarshal([]byte(val), &fields); err != nil {
		t.Fatalf("Resolve() returned non-JSON for structured secret: %v", err)
	}
	if fields["host"] != "ftp.claims.example.com" {
		t.Errorf("fields[host] = %q, want %q", fields["host"], "ftp.claims.example.com")
	}
	if fields["user"] != "claims_ftp" {
		t.Errorf("fields[user] = %q, want %q", fields["user"], "claims_ftp")
	}
	if fields["password"] != "ftp_secret" {
		t.Errorf("fields[password] = %q, want %q", fields["password"], "ftp_secret")
	}
}

func TestResolve_StructuredSecretGlobalFallback(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	val, err := store.Resolve("unknown_project", "shared_db")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}

	var fields map[string]string
	if err := json.Unmarshal([]byte(val), &fields); err != nil {
		t.Fatalf("Resolve() returned non-JSON for structured secret: %v", err)
	}
	if fields["host"] != "global-db.example.com" {
		t.Errorf("fields[host] = %q, want %q", fields["host"], "global-db.example.com")
	}
}

func TestResolveField_ProjectScoped(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	host, err := store.ResolveField("claims_pipeline", "ftp_creds", "host")
	if err != nil {
		t.Fatalf("ResolveField() unexpected error: %v", err)
	}
	if host != "ftp.claims.example.com" {
		t.Errorf("ResolveField() = %q, want %q", host, "ftp.claims.example.com")
	}
}

func TestResolveField_GlobalFallback(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	host, err := store.ResolveField("unknown_project", "shared_db", "host")
	if err != nil {
		t.Fatalf("ResolveField() unexpected error: %v", err)
	}
	if host != "global-db.example.com" {
		t.Errorf("ResolveField() = %q, want %q", host, "global-db.example.com")
	}
}

func TestResolveField_MissingField(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	_, err = store.ResolveField("claims_pipeline", "ftp_creds", "nonexistent")
	if err == nil {
		t.Error("ResolveField() expected error for missing field, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error = %q, want it to contain %q", err, "nonexistent")
	}
}

func TestResolveField_MissingSecret(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	_, err = store.ResolveField("claims_pipeline", "no_such_secret", "host")
	if err == nil {
		t.Error("ResolveField() expected error for missing secret, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want it to contain 'not found'", err)
	}
}

func TestResolveField_PlainSecretErrors(t *testing.T) {
	path := writeSecretsFile(t, validTOML)
	store, err := Load(path)
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	_, err = store.ResolveField("claims_pipeline", "claims_db", "host")
	if err == nil {
		t.Error("ResolveField() expected error for plain secret, got nil")
	}
	if !strings.Contains(err.Error(), "plain value") {
		t.Errorf("error = %q, want it to mention 'plain value'", err)
	}
}
