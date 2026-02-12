package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
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
type HandlerFunc func(ctx context.Context, params map[string]string) (string, error)

// SecretsResolver resolves secrets by project scope.
type SecretsResolver interface {
	Resolve(project, key string) (string, error)
	ResolveField(project, secret, field string) (string, error)
}

// Server is a JSON-over-socket server for task-to-orchestrator communication.
// On Unix it uses a Unix domain socket; on Windows it uses TCP on localhost.
type Server struct {
	listener   net.Listener
	socketPath string // non-empty only for Unix sockets (for cleanup)
	addr       string // connection address: socket path (Unix) or host:port (Windows)
	dagName    string
	handlers   map[string]HandlerFunc
	wg         sync.WaitGroup

	mu       sync.Mutex
	serveCtx context.Context // set by Serve(), passed to handlers
}

// NewServer creates a socket listener and registers the default handlers.
// On Unix, it listens on a Unix domain socket at socketPath.
// On Windows, it listens on TCP 127.0.0.1 with an OS-assigned port (socketPath is ignored).
func NewServer(socketPath string, store SecretsResolver, dagName string) (*Server, error) {
	ln, addr, err := listen(socketPath)
	if err != nil {
		return nil, err
	}

	s := &Server{
		listener:   ln,
		socketPath: socketPath,
		addr:       addr,
		dagName:    dagName,
		handlers:   make(map[string]HandlerFunc),
	}

	if store != nil {
		s.handlers["get_secret"] = func(_ context.Context, params map[string]string) (string, error) {
			key := params["key"]
			if key == "" {
				return "", fmt.Errorf("missing required parameter: key")
			}
			return store.Resolve(dagName, key)
		}
		s.handlers["get_secret_field"] = func(_ context.Context, params map[string]string) (string, error) {
			secret := params["secret"]
			if secret == "" {
				return "", fmt.Errorf("missing required parameter: secret")
			}
			field := params["field"]
			if field == "" {
				return "", fmt.Errorf("missing required parameter: field")
			}
			return store.ResolveField(dagName, secret, field)
		}
	}

	return s, nil
}

// RegisterHandler adds or replaces a method handler on the server.
func (s *Server) RegisterHandler(method string, handler HandlerFunc) {
	s.handlers[method] = handler
}

// listen creates a platform-appropriate network listener.
// On Windows, it returns a TCP listener on 127.0.0.1 with an OS-assigned port.
// On other platforms, it returns a Unix domain socket listener at socketPath.
func listen(socketPath string) (net.Listener, string, error) {
	if runtime.GOOS == "windows" {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, "", fmt.Errorf("creating SDK TCP listener: %w", err)
		}
		return ln, ln.Addr().String(), nil
	}

	os.Remove(socketPath)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, "", fmt.Errorf("creating SDK socket %q: %w", socketPath, err)
	}
	return ln, socketPath, nil
}

// Addr returns the address clients should use to connect to this server.
// On Unix this is the socket file path; on Windows it is a host:port string.
func (s *Server) Addr() string {
	return s.addr
}

// Serve accepts connections until the context is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	s.serveCtx = ctx
	s.mu.Unlock()

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
	if s.socketPath != "" && runtime.GOOS != "windows" {
		os.Remove(s.socketPath)
	}
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

	s.mu.Lock()
	ctx := s.serveCtx
	s.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := handler(ctx, req.Params)
	var resp Response
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.Result = result
	}
	json.NewEncoder(conn).Encode(resp)
}
