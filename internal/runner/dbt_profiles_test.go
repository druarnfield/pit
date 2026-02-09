package runner

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// mockResolver implements SecretsResolver for testing.
type mockResolver struct {
	secrets map[string]string
}

func (m *mockResolver) Resolve(project, key string) (string, error) {
	v, ok := m.secrets[key]
	if !ok {
		return "", fmt.Errorf("secret %q not found", key)
	}
	return v, nil
}

func TestGenerateProfiles(t *testing.T) {
	resolver := &mockResolver{secrets: map[string]string{
		"dbt_host":     "sql-server.example.com",
		"dbt_port":     "1433",
		"dbt_database": "analytics",
		"dbt_schema":   "dbo",
		"dbt_user":     "dbt_user",
		"dbt_password": "secret123",
	}}

	input := &DBTProfilesInput{
		DAGName: "my_dbt_dag",
		Profile: "analytics",
		Target:  "prod",
	}

	dir, cleanup, err := GenerateProfiles(input, resolver)
	if err != nil {
		t.Fatalf("GenerateProfiles() error: %v", err)
	}
	defer cleanup()

	// Read the generated profiles.yml
	data, err := os.ReadFile(dir + "/profiles.yml")
	if err != nil {
		t.Fatalf("reading profiles.yml: %v", err)
	}
	content := string(data)

	// Verify content
	checks := []struct {
		label string
		want  string
	}{
		{"profile name", "analytics:"},
		{"target", "target: prod"},
		{"host", `server: "sql-server.example.com"`},
		{"port", "port: 1433"},
		{"database", `database: "analytics"`},
		{"schema", `schema: "dbo"`},
		{"user", `user: "dbt_user"`},
		{"password", `password: "secret123"`},
		{"type", "type: sqlserver"},
		{"default driver", `driver: "ODBC Driver 17 for SQL Server"`},
	}
	for _, c := range checks {
		if !strings.Contains(content, c.want) {
			t.Errorf("profiles.yml missing %s (%q)\n  got: %s", c.label, c.want, content)
		}
	}
}

func TestGenerateProfiles_DefaultProfileAndTarget(t *testing.T) {
	resolver := &mockResolver{secrets: map[string]string{
		"dbt_host":     "host",
		"dbt_port":     "1433",
		"dbt_database": "db",
		"dbt_schema":   "dbo",
		"dbt_user":     "user",
		"dbt_password": "pass",
	}}

	input := &DBTProfilesInput{
		DAGName: "my_dag",
		// Profile and Target left empty to test defaults
	}

	dir, cleanup, err := GenerateProfiles(input, resolver)
	if err != nil {
		t.Fatalf("GenerateProfiles() error: %v", err)
	}
	defer cleanup()

	data, err := os.ReadFile(dir + "/profiles.yml")
	if err != nil {
		t.Fatalf("reading profiles.yml: %v", err)
	}
	content := string(data)

	// Default profile name should be DAG name
	if !strings.Contains(content, "my_dag:") {
		t.Errorf("profiles.yml should use dag name as profile, got: %s", content)
	}
	// Default target should be "prod"
	if !strings.Contains(content, "target: prod") {
		t.Errorf("profiles.yml should default target to prod, got: %s", content)
	}
}

func TestGenerateProfiles_MissingSecret(t *testing.T) {
	resolver := &mockResolver{secrets: map[string]string{
		"dbt_host": "host",
		// Missing other secrets
	}}

	input := &DBTProfilesInput{DAGName: "test"}

	_, cleanup, err := GenerateProfiles(input, resolver)
	defer cleanup()

	if err == nil {
		t.Fatal("GenerateProfiles() expected error for missing secret, got nil")
	}
	if !strings.Contains(err.Error(), "dbt_port") {
		t.Errorf("error = %q, want it to mention missing secret", err)
	}
}

func TestGenerateProfiles_NilResolver(t *testing.T) {
	input := &DBTProfilesInput{DAGName: "test"}

	_, cleanup, err := GenerateProfiles(input, nil)
	defer cleanup()

	if err == nil {
		t.Fatal("GenerateProfiles() expected error for nil resolver, got nil")
	}
	if !strings.Contains(err.Error(), "secrets resolver") {
		t.Errorf("error = %q, want it to mention secrets resolver", err)
	}
}

func TestGenerateProfiles_InvalidPort(t *testing.T) {
	resolver := &mockResolver{secrets: map[string]string{
		"dbt_host":     "host",
		"dbt_port":     "not_a_number",
		"dbt_database": "db",
		"dbt_schema":   "dbo",
		"dbt_user":     "user",
		"dbt_password": "pass",
	}}

	input := &DBTProfilesInput{DAGName: "test"}

	_, cleanup, err := GenerateProfiles(input, resolver)
	defer cleanup()

	if err == nil {
		t.Fatal("GenerateProfiles() expected error for invalid port, got nil")
	}
	if !strings.Contains(err.Error(), "not a valid integer") {
		t.Errorf("error = %q, want it to mention invalid integer", err)
	}
}

func TestGenerateProfiles_CustomDriver(t *testing.T) {
	resolver := &mockResolver{secrets: map[string]string{
		"dbt_host":     "host",
		"dbt_port":     "1433",
		"dbt_database": "db",
		"dbt_schema":   "dbo",
		"dbt_user":     "user",
		"dbt_password": "pass",
	}}

	input := &DBTProfilesInput{
		DAGName: "test",
		Driver:  "ODBC Driver 18 for SQL Server",
	}

	dir, cleanup, err := GenerateProfiles(input, resolver)
	if err != nil {
		t.Fatalf("GenerateProfiles() error: %v", err)
	}
	defer cleanup()

	data, err := os.ReadFile(dir + "/profiles.yml")
	if err != nil {
		t.Fatalf("reading profiles.yml: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, `driver: "ODBC Driver 18 for SQL Server"`) {
		t.Errorf("profiles.yml should use custom driver, got: %s", content)
	}
}

func TestGenerateProfiles_CustomProfileAndTarget(t *testing.T) {
	resolver := &mockResolver{secrets: map[string]string{
		"dbt_host":     "host",
		"dbt_port":     "1433",
		"dbt_database": "db",
		"dbt_schema":   "dbo",
		"dbt_user":     "user",
		"dbt_password": "pass",
	}}

	input := &DBTProfilesInput{
		DAGName: "my_dag",
		Profile: "custom_profile",
		Target:  "dev",
	}

	dir, cleanup, err := GenerateProfiles(input, resolver)
	if err != nil {
		t.Fatalf("GenerateProfiles() error: %v", err)
	}
	defer cleanup()

	data, err := os.ReadFile(dir + "/profiles.yml")
	if err != nil {
		t.Fatalf("reading profiles.yml: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "custom_profile:") {
		t.Errorf("profiles.yml should use custom profile name, got: %s", content)
	}
	if !strings.Contains(content, "target: dev") {
		t.Errorf("profiles.yml should use custom target, got: %s", content)
	}
}
