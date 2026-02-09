package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// mockStore implements SecretsResolver for testing.
type mockStore struct {
	data map[string]map[string]string
}

func (m *mockStore) Resolve(project, key string) (string, error) {
	if section, ok := m.data[project]; ok {
		if val, ok := section[key]; ok {
			return val, nil
		}
	}
	if section, ok := m.data["global"]; ok {
		if val, ok := section[key]; ok {
			return val, nil
		}
	}
	return "", fmt.Errorf("secret %q not found for project %q", key, project)
}

// testNetwork returns the network type used by the SDK server on the current platform.
func testNetwork() string {
	if runtime.GOOS == "windows" {
		return "tcp"
	}
	return "unix"
}

func startTestServer(t *testing.T, store SecretsResolver, dagName string) (string, context.CancelFunc) {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	srv, err := NewServer(sockPath, store, dagName)
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx)
	}()

	addr := srv.Addr()
	network := testNetwork()

	// Wait briefly for socket to be ready
	for i := 0; i < 50; i++ {
		conn, err := net.Dial(network, addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		cancel()
		srv.Shutdown()
	})

	return addr, cancel
}

func sendRequest(t *testing.T, addr string, req Request) Response {
	t.Helper()
	conn, err := net.Dial(testNetwork(), addr)
	if err != nil {
		t.Fatalf("connecting to socket: %v", err)
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		t.Fatalf("encoding request: %v", err)
	}

	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return resp
}

func TestGetSecret_RoundTrip(t *testing.T) {
	store := &mockStore{data: map[string]map[string]string{
		"my_dag": {"db_conn": "Server=localhost;Database=test"},
	}}
	sockPath, _ := startTestServer(t, store, "my_dag")

	resp := sendRequest(t, sockPath, Request{
		Method: "get_secret",
		Params: map[string]string{"key": "db_conn"},
	})

	if resp.Error != "" {
		t.Fatalf("get_secret returned error: %s", resp.Error)
	}
	if resp.Result != "Server=localhost;Database=test" {
		t.Errorf("get_secret result = %q, want %q", resp.Result, "Server=localhost;Database=test")
	}
}

func TestGetSecret_MissingKey(t *testing.T) {
	store := &mockStore{data: map[string]map[string]string{}}
	sockPath, _ := startTestServer(t, store, "my_dag")

	resp := sendRequest(t, sockPath, Request{
		Method: "get_secret",
		Params: map[string]string{"key": "nonexistent"},
	})

	if resp.Error == "" {
		t.Error("expected error for missing key, got none")
	}
}

func TestGetSecret_EmptyKeyParam(t *testing.T) {
	store := &mockStore{data: map[string]map[string]string{}}
	sockPath, _ := startTestServer(t, store, "my_dag")

	resp := sendRequest(t, sockPath, Request{
		Method: "get_secret",
		Params: map[string]string{},
	})

	if resp.Error == "" {
		t.Error("expected error for missing key parameter, got none")
	}
	if !strings.Contains(resp.Error, "key") {
		t.Errorf("error = %q, want it to mention 'key'", resp.Error)
	}
}

func TestUnknownMethod(t *testing.T) {
	store := &mockStore{data: map[string]map[string]string{}}
	sockPath, _ := startTestServer(t, store, "my_dag")

	resp := sendRequest(t, sockPath, Request{
		Method: "bogus_method",
		Params: map[string]string{},
	})

	if resp.Error == "" {
		t.Error("expected error for unknown method, got none")
	}
	if !strings.Contains(resp.Error, "unknown method") {
		t.Errorf("error = %q, want it to contain 'unknown method'", resp.Error)
	}
}

func TestMalformedJSON(t *testing.T) {
	store := &mockStore{data: map[string]map[string]string{}}
	sockPath, _ := startTestServer(t, store, "my_dag")

	conn, err := net.Dial(testNetwork(), sockPath)
	if err != nil {
		t.Fatalf("connecting to socket: %v", err)
	}
	defer conn.Close()

	// Send invalid JSON
	conn.Write([]byte("not json at all\n"))

	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Error == "" {
		t.Error("expected error for malformed JSON, got none")
	}
	if !strings.Contains(resp.Error, "invalid request") {
		t.Errorf("error = %q, want it to contain 'invalid request'", resp.Error)
	}
}

func TestContextCancellation(t *testing.T) {
	store := &mockStore{data: map[string]map[string]string{}}
	sockPath, cancel := startTestServer(t, store, "my_dag")

	// Verify server is running
	resp := sendRequest(t, sockPath, Request{
		Method: "get_secret",
		Params: map[string]string{"key": "x"},
	})
	if resp.Error == "" {
		t.Error("expected error (missing key), but got none — server is running though")
	}

	// Cancel context — server should shut down
	cancel()

	// Give shutdown a moment
	time.Sleep(50 * time.Millisecond)

	// Connection should now fail
	_, err := net.Dial(testNetwork(), sockPath)
	if err == nil {
		t.Error("expected connection to fail after shutdown")
	}
}

func TestAddr(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	store := &mockStore{data: map[string]map[string]string{}}
	srv, err := NewServer(sockPath, store, "test")
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}
	defer srv.Shutdown()

	addr := srv.Addr()
	if addr == "" {
		t.Fatal("Addr() returned empty string")
	}

	if runtime.GOOS == "windows" {
		// On Windows, addr should be a TCP address like 127.0.0.1:PORT
		if !strings.HasPrefix(addr, "127.0.0.1:") {
			t.Errorf("Addr() = %q, want it to start with '127.0.0.1:'", addr)
		}
	} else {
		// On Unix, addr should be the socket path
		if addr != sockPath {
			t.Errorf("Addr() = %q, want %q", addr, sockPath)
		}
	}
}

func TestRegisterHandler(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	srv, err := NewServer(sockPath, nil, "test")
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}

	srv.RegisterHandler("echo", func(_ context.Context, params map[string]string) (string, error) {
		return params["msg"], nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)

	// Wait for server
	network := testNetwork()
	addr := srv.Addr()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial(network, addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
		srv.Shutdown()
	})

	resp := sendRequest(t, addr, Request{
		Method: "echo",
		Params: map[string]string{"msg": "hello"},
	})
	if resp.Error != "" {
		t.Fatalf("echo returned error: %s", resp.Error)
	}
	if resp.Result != "hello" {
		t.Errorf("echo result = %q, want %q", resp.Result, "hello")
	}
}

func TestNewServer_NilStore(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	srv, err := NewServer(sockPath, nil, "test")
	if err != nil {
		t.Fatalf("NewServer(nil store) unexpected error: %v", err)
	}
	defer srv.Shutdown()

	// get_secret should not be registered when store is nil
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx)

	network := testNetwork()
	addr := srv.Addr()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial(network, addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
	})

	resp := sendRequest(t, addr, Request{
		Method: "get_secret",
		Params: map[string]string{"key": "x"},
	})
	if !strings.Contains(resp.Error, "unknown method") {
		t.Errorf("expected 'unknown method' error, got %q", resp.Error)
	}
}
