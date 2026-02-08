package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

// Request is the JSON message sent by a task to the SDK server.
type Request struct {
	Method string            `json:"method"`
	Params map[string]string `json:"params"`
}

// Response is the JSON reply from the SDK server to a task.
type Response struct {
	Result string `json:"result"`
	Error  string `json:"error,omitempty"`
}

// HandlerFunc processes an SDK request and returns a result or error string.
type HandlerFunc func(params map[string]string) (string, error)

// SecretsResolver resolves secrets by project scope.
type SecretsResolver interface {
	Resolve(project, key string) (string, error)
}

// Server is a JSON-over-Unix-socket server for task-to-orchestrator communication.
type Server struct {
	listener   net.Listener
	socketPath string
	dagName    string
	handlers   map[string]HandlerFunc
	wg         sync.WaitGroup
}

// NewServer creates a Unix socket and registers the default handlers.
func NewServer(socketPath string, store SecretsResolver, dagName string) (*Server, error) {
	// Remove stale socket file if it exists
	os.Remove(socketPath)

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("creating SDK socket %q: %w", socketPath, err)
	}

	s := &Server{
		listener:   ln,
		socketPath: socketPath,
		dagName:    dagName,
		handlers:   make(map[string]HandlerFunc),
	}

	if store != nil {
		s.handlers["get_secret"] = func(params map[string]string) (string, error) {
			key := params["key"]
			if key == "" {
				return "", fmt.Errorf("missing required parameter: key")
			}
			return store.Resolve(dagName, key)
		}
	}

	return s, nil
}

// Serve accepts connections until the context is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.listener.Close()
	}()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if shutdown was requested
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				return fmt.Errorf("accepting connection: %w", err)
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// Shutdown closes the listener, waits for in-flight connections, and removes the socket file.
func (s *Server) Shutdown() error {
	err := s.listener.Close()
	s.wg.Wait()
	os.Remove(s.socketPath)
	return err
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		resp := Response{Error: fmt.Sprintf("invalid request: %v", err)}
		json.NewEncoder(conn).Encode(resp)
		return
	}

	handler, ok := s.handlers[req.Method]
	if !ok {
		resp := Response{Error: fmt.Sprintf("unknown method: %s", req.Method)}
		json.NewEncoder(conn).Encode(resp)
		return
	}

	result, err := handler(req.Params)
	var resp Response
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.Result = result
	}
	json.NewEncoder(conn).Encode(resp)
}
