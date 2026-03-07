package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/loghub"
	"github.com/druarnfield/pit/internal/meta"
)

type handler struct {
	configs map[string]*config.ProjectConfig
	store   meta.Store
	token   string
	hub     *loghub.Hub
	runsDir string
}

// NewHandler returns an http.Handler for the /api/ routes.
func NewHandler(configs map[string]*config.ProjectConfig, store meta.Store, token string, hub *loghub.Hub, runsDir string) http.Handler {
	h := &handler{configs: configs, store: store, token: token, hub: hub, runsDir: runsDir}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", h.handleHealth)
	mux.HandleFunc("GET /api/dags", h.handleListDAGs)
	mux.HandleFunc("GET /api/runs/{id}/logs", h.handleRunLogs)
	mux.HandleFunc("GET /api/dags/{name}/logs", h.handleDAGLogs)
	mux.HandleFunc("GET /api/dags/{name}", h.handleDAGDetail)
	mux.HandleFunc("GET /api/runs", h.handleListRuns)
	mux.HandleFunc("GET /api/runs/{id}", h.handleRunDetail)
	mux.HandleFunc("GET /api/outputs", h.handleListOutputs)

	return h.authMiddleware(mux)
}

func (h *handler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.token != "" && r.URL.Path != "/api/health" {
			authHeader := r.Header.Get("Authorization")
			var provided string
			if strings.HasPrefix(authHeader, "Bearer ") {
				provided = authHeader[len("Bearer "):]
			}
			expected := sha256.Sum256([]byte(h.token))
			got := sha256.Sum256([]byte(provided))
			if subtle.ConstantTimeCompare(expected[:], got[:]) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}


func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: json encode error: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
