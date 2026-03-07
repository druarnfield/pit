package secrets

import (
	"strings"
	"testing"
)

func TestSetPlainSecret(t *testing.T) {
	input := []byte("[global]\nsmtp_password = \"old_value\"\n")
	result, err := SetSecret(input, "global", "smtp_password", "new_value", nil)
	if err != nil {
		t.Fatalf("SetSecret() error: %v", err)
	}

	store, err := LoadFromBytes(result)
	if err != nil {
		t.Fatalf("LoadFromBytes() error: %v", err)
	}
	val, err := store.Resolve("global", "smtp_password")
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
	if val != "new_value" {
		t.Errorf("Resolve() = %q, want %q", val, "new_value")
	}
}

func TestSetPlainSecret_NewProject(t *testing.T) {
	input := []byte("[global]\nkey = \"val\"\n")
	result, err := SetSecret(input, "new_project", "api_key", "abc123", nil)
	if err != nil {
		t.Fatalf("SetSecret() error: %v", err)
	}

	store, err := LoadFromBytes(result)
	if err != nil {
		t.Fatalf("LoadFromBytes() error: %v", err)
	}
	val, err := store.Resolve("new_project", "api_key")
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
	if val != "abc123" {
		t.Errorf("Resolve() = %q, want %q", val, "abc123")
	}
}

func TestSetStructuredSecret(t *testing.T) {
	input := []byte("[global]\nkey = \"val\"\n")
	fields := map[string]string{
		"host":     "db.example.com",
		"user":     "admin",
		"password": "secret",
	}
	result, err := SetSecret(input, "global", "db_creds", "", fields)
	if err != nil {
		t.Fatalf("SetSecret() error: %v", err)
	}

	store, err := LoadFromBytes(result)
	if err != nil {
		t.Fatalf("LoadFromBytes() error: %v", err)
	}
	val, err := store.ResolveField("global", "db_creds", "host")
	if err != nil {
		t.Fatalf("ResolveField() error: %v", err)
	}
	if val != "db.example.com" {
		t.Errorf("ResolveField() = %q, want %q", val, "db.example.com")
	}
}

func TestRemoveSecret(t *testing.T) {
	input := []byte("[global]\nkeep_this = \"yes\"\nremove_this = \"bye\"\n")
	result, err := RemoveSecret(input, "global", "remove_this")
	if err != nil {
		t.Fatalf("RemoveSecret() error: %v", err)
	}

	store, err := LoadFromBytes(result)
	if err != nil {
		t.Fatalf("LoadFromBytes() error: %v", err)
	}

	val, err := store.Resolve("global", "keep_this")
	if err != nil {
		t.Fatalf("Resolve(keep_this) error: %v", err)
	}
	if val != "yes" {
		t.Errorf("Resolve(keep_this) = %q, want %q", val, "yes")
	}

	_, err = store.Resolve("global", "remove_this")
	if err == nil {
		t.Error("Resolve(remove_this) expected error, got nil")
	}
}

func TestRemoveSecret_NotFound(t *testing.T) {
	input := []byte("[global]\nkey = \"val\"\n")
	_, err := RemoveSecret(input, "global", "nonexistent")
	if err == nil {
		t.Error("RemoveSecret() expected error for missing key, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want it to contain 'not found'", err)
	}
}

func TestListSecrets(t *testing.T) {
	input := []byte("[global]\nkey_a = \"val\"\nkey_b = \"val\"\n\n[my_project]\nproj_key = \"val\"\n")
	store, err := LoadFromBytes(input)
	if err != nil {
		t.Fatalf("LoadFromBytes() error: %v", err)
	}

	keys := store.ListKeys("")
	if _, ok := keys["global"]; !ok {
		t.Error("ListKeys() missing 'global' section")
	}
	if _, ok := keys["my_project"]; !ok {
		t.Error("ListKeys() missing 'my_project' section")
	}
	if len(keys["global"]) != 2 {
		t.Errorf("len(global keys) = %d, want 2", len(keys["global"]))
	}

	projKeys := store.ListKeys("my_project")
	if len(projKeys) != 1 {
		t.Errorf("len(ListKeys(my_project)) = %d, want 1", len(projKeys))
	}
}

func TestSetSecret_EmptyInput(t *testing.T) {
	result, err := SetSecret([]byte{}, "global", "new_key", "new_val", nil)
	if err != nil {
		t.Fatalf("SetSecret() error: %v", err)
	}

	store, err := LoadFromBytes(result)
	if err != nil {
		t.Fatalf("LoadFromBytes() error: %v", err)
	}
	val, err := store.Resolve("global", "new_key")
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
	if val != "new_val" {
		t.Errorf("Resolve() = %q, want %q", val, "new_val")
	}
}
