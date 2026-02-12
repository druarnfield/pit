package runner

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// mockResolver implements SecretsResolver for testing.
type mockResolver struct {
	secrets map[string]string            // flat key→value for Resolve
	fields  map[string]map[string]string // secret→field→value for ResolveField
}

func (m *mockResolver) Resolve(project, key string) (string, error) {
	if m.secrets == nil {
		return "", fmt.Errorf("secret %q not found", key)
	}
	v, ok := m.secrets[key]
	if !ok {
		return "", fmt.Errorf("secret %q not found", key)
	}
	return v, nil
}

func (m *mockResolver) ResolveField(project, secret, field string) (string, error) {
	if m.fields == nil {
		return "", fmt.Errorf("secret %q not found", secret)
	}
	sec, ok := m.fields[secret]
	if !ok {
		return "", fmt.Errorf("secret %q not found", secret)
	}
	val, ok := sec[field]
	if !ok {
		return "", fmt.Errorf("field %q not found in secret %q", field, secret)
	}
	return val, nil
}

func TestGenerateProfiles(t *testing.T) {
	resolver := &mockResolver{fields: map[string]map[string]string{
		"my_db": {
			"host":     "sql-server.example.com",
			"port":     "1433",
			"database": "analytics",
			"schema":   "dbo",
			"user":     "dbt_user",
			"password": "secret123",
		},
	}}

	input := &DBTProfilesInput{
		DAGName:    "my_dbt_dag",
		Profile:    "analytics",
		Target:     "prod",
		Connection: "my_db",
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
	resolver := &mockResolver{fields: map[string]map[string]string{
		"my_db": {
			"host":     "host",
			"port":     "1433",
			"database": "db",
			"schema":   "dbo",
			"user":     "user",
			"password": "pass",
		},
	}}

	input := &DBTProfilesInput{
		DAGName:    "my_dag",
		Connection: "my_db",
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

func TestGenerateProfiles_MissingField(t *testing.T) {
	resolver := &mockResolver{fields: map[string]map[string]string{
		"my_db": {
			"host": "host",
			// Missing port and other fields
		},
	}}

	input := &DBTProfilesInput{DAGName: "test", Connection: "my_db"}

	_, cleanup, err := GenerateProfiles(input, resolver)
	defer cleanup()

	if err == nil {
		t.Fatal("GenerateProfiles() expected error for missing field, got nil")
	}
	if !strings.Contains(err.Error(), "port") {
		t.Errorf("error = %q, want it to mention missing field", err)
	}
}

func TestGenerateProfiles_NilResolver(t *testing.T) {
	input := &DBTProfilesInput{DAGName: "test", Connection: "my_db"}

	_, cleanup, err := GenerateProfiles(input, nil)
	defer cleanup()

	if err == nil {
		t.Fatal("GenerateProfiles() expected error for nil resolver, got nil")
	}
	if !strings.Contains(err.Error(), "secrets resolver") {
		t.Errorf("error = %q, want it to mention secrets resolver", err)
	}
}

func TestGenerateProfiles_MissingConnection(t *testing.T) {
	resolver := &mockResolver{}

	input := &DBTProfilesInput{DAGName: "test"}

	_, cleanup, err := GenerateProfiles(input, resolver)
	defer cleanup()

	if err == nil {
		t.Fatal("GenerateProfiles() expected error for missing connection, got nil")
	}
	if !strings.Contains(err.Error(), "connection secret name is required") {
		t.Errorf("error = %q, want it to mention connection", err)
	}
}

func TestGenerateProfiles_InvalidPort(t *testing.T) {
	resolver := &mockResolver{fields: map[string]map[string]string{
		"my_db": {
			"host":     "host",
			"port":     "not_a_number",
			"database": "db",
			"schema":   "dbo",
			"user":     "user",
			"password": "pass",
		},
	}}

	input := &DBTProfilesInput{DAGName: "test", Connection: "my_db"}

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
	resolver := &mockResolver{fields: map[string]map[string]string{
		"my_db": {
			"host":     "host",
			"port":     "1433",
			"database": "db",
			"schema":   "dbo",
			"user":     "user",
			"password": "pass",
		},
	}}

	input := &DBTProfilesInput{
		DAGName:    "test",
		Driver:     "ODBC Driver 18 for SQL Server",
		Connection: "my_db",
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
	resolver := &mockResolver{fields: map[string]map[string]string{
		"my_db": {
			"host":     "host",
			"port":     "1433",
			"database": "db",
			"schema":   "dbo",
			"user":     "user",
			"password": "pass",
		},
	}}

	input := &DBTProfilesInput{
		DAGName:    "my_dag",
		Profile:    "custom_profile",
		Target:     "dev",
		Connection: "my_db",
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
