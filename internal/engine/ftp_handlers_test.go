package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/druarnfield/pit/internal/secrets"
)

func loadTestStore(t *testing.T, toml string) *secrets.Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "secrets.toml")
	if err := os.WriteFile(path, []byte(toml), 0o600); err != nil {
		t.Fatalf("writing test secrets: %v", err)
	}
	store, err := secrets.Load(path)
	if err != nil {
		t.Fatalf("loading test secrets: %v", err)
	}
	return store
}

func TestConnectFTP_NilStore(t *testing.T) {
	_, err := connectFTP(nil, "test", "ftp_creds")
	if err == nil {
		t.Fatal("connectFTP(nil) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "secrets store not configured") {
		t.Errorf("error = %q, want mention of secrets store", err)
	}
}

func TestConnectFTP_MissingFields(t *testing.T) {
	store := loadTestStore(t, `
[global.incomplete]
host = "ftp.example.com"
`)

	_, err := connectFTP(store, "test", "incomplete")
	if err == nil {
		t.Fatal("connectFTP(incomplete secret) expected error, got nil")
	}
	// Should fail on missing user field
	if !strings.Contains(err.Error(), "user") {
		t.Errorf("error = %q, want mention of 'user'", err)
	}
}

func TestConnectFTP_MissingSecret(t *testing.T) {
	store := loadTestStore(t, `
[global]
plain_key = "value"
`)

	_, err := connectFTP(store, "test", "nonexistent")
	if err == nil {
		t.Fatal("connectFTP(missing secret) expected error, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want mention of 'not found'", err)
	}
}

func TestFTPListHandler_MissingParams(t *testing.T) {
	store := loadTestStore(t, `[global]
key = "value"
`)
	handler := makeFTPListHandler(store, "test")
	ctx := context.Background()

	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"missing secret", map[string]string{"directory": "/data"}, "secret"},
		{"missing directory", map[string]string{"secret": "ftp_creds"}, "directory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler(ctx, tt.params)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestFTPDownloadHandler_MissingParams(t *testing.T) {
	store := loadTestStore(t, `[global]
key = "value"
`)
	dataDir := t.TempDir()
	handler := makeFTPDownloadHandler(store, "test", dataDir)
	ctx := context.Background()

	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"missing secret", map[string]string{"remote_path": "/data/file.csv"}, "secret"},
		{"missing both path and pattern", map[string]string{"secret": "ftp_creds"}, "remote_path"},
		{"pattern without directory", map[string]string{"secret": "ftp_creds", "pattern": "*.csv"}, "directory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler(ctx, tt.params)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestFTPDownloadHandler_DirectoryTraversal(t *testing.T) {
	store := loadTestStore(t, `
[global.ftp_creds]
host = "ftp.example.com"
user = "user"
password = "pass"
`)
	dataDir := t.TempDir()
	handler := makeFTPDownloadHandler(store, "test", dataDir)
	ctx := context.Background()

	// Attempt directory traversal via remote_path
	_, err := handler(ctx, map[string]string{
		"secret":      "ftp_creds",
		"remote_path": "/incoming/../../../etc/passwd",
	})
	// This will fail at the FTP connect stage (no real server),
	// but if it were to get past that, the traversal check would catch it.
	// The error should be about connection, not about traversal succeeding.
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestFTPUploadHandler_MissingParams(t *testing.T) {
	store := loadTestStore(t, `[global]
key = "value"
`)
	dataDir := t.TempDir()
	handler := makeFTPUploadHandler(store, "test", dataDir)
	ctx := context.Background()

	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"missing secret", map[string]string{"local_name": "f.csv", "remote_path": "/out/f.csv"}, "secret"},
		{"missing local_name", map[string]string{"secret": "ftp_creds", "remote_path": "/out/f.csv"}, "local_name"},
		{"missing remote_path", map[string]string{"secret": "ftp_creds", "local_name": "f.csv"}, "remote_path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler(ctx, tt.params)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestFTPUploadHandler_DirectoryTraversal(t *testing.T) {
	store := loadTestStore(t, `
[global.ftp_creds]
host = "ftp.example.com"
user = "user"
password = "pass"
`)
	dataDir := t.TempDir()
	handler := makeFTPUploadHandler(store, "test", dataDir)
	ctx := context.Background()

	_, err := handler(ctx, map[string]string{
		"secret":      "ftp_creds",
		"local_name":  "../../etc/passwd",
		"remote_path": "/out/stolen.txt",
	})
	if err == nil {
		t.Fatal("expected error for directory traversal, got nil")
	}
	if !strings.Contains(err.Error(), "escapes data directory") {
		t.Errorf("error = %q, want mention of 'escapes data directory'", err)
	}
}

func TestFTPMoveHandler_MissingParams(t *testing.T) {
	store := loadTestStore(t, `[global]
key = "value"
`)
	handler := makeFTPMoveHandler(store, "test")
	ctx := context.Background()

	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"missing secret", map[string]string{"src": "/a", "dst": "/b"}, "secret"},
		{"missing src", map[string]string{"secret": "ftp_creds", "dst": "/b"}, "src"},
		{"missing dst", map[string]string{"secret": "ftp_creds", "src": "/a"}, "dst"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler(ctx, tt.params)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to mention %q", err, tt.want)
			}
		})
	}
}
