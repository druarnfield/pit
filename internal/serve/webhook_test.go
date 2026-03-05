package serve

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/druarnfield/pit/internal/trigger"
)

// newWebhookServer builds a minimal Server suitable for testing webhookHandler.
func newWebhookServer(tokens map[string]string) *Server {
	return &Server{
		webhookTokens: tokens,
		eventCh:       make(chan trigger.Event, 8),
	}
}

func TestWebhookHandler_ValidToken(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})

	req := httptest.NewRequest(http.MethodPost, "/webhook/my_dag", nil)
	req.Header.Set("Authorization", "Bearer supersecret")
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", w.Code, http.StatusAccepted)
	}

	select {
	case ev := <-s.eventCh:
		if ev.DAGName != "my_dag" {
			t.Errorf("event.DAGName = %q, want %q", ev.DAGName, "my_dag")
		}
		if ev.Source != "webhook" {
			t.Errorf("event.Source = %q, want %q", ev.Source, "webhook")
		}
	default:
		t.Error("expected event on channel, got none")
	}
}

func TestWebhookHandler_InvalidToken(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})

	req := httptest.NewRequest(http.MethodPost, "/webhook/my_dag", nil)
	req.Header.Set("Authorization", "Bearer wrongtoken")
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
	if len(s.eventCh) != 0 {
		t.Error("expected no event on channel after invalid token")
	}
}

func TestWebhookHandler_MissingAuthHeader(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})

	req := httptest.NewRequest(http.MethodPost, "/webhook/my_dag", nil)
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestWebhookHandler_UnknownDAG(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})

	req := httptest.NewRequest(http.MethodPost, "/webhook/unknown_dag", nil)
	req.Header.Set("Authorization", "Bearer supersecret")
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestWebhookHandler_WrongMethod(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})

	req := httptest.NewRequest(http.MethodGet, "/webhook/my_dag", nil)
	req.Header.Set("Authorization", "Bearer supersecret")
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestWebhookHandler_ServerBusy(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})
	// Fill the event channel to capacity
	for i := 0; i < cap(s.eventCh); i++ {
		s.eventCh <- trigger.Event{}
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook/my_dag", nil)
	req.Header.Set("Authorization", "Bearer supersecret")
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestWebhookHandler_EmptyDAGName(t *testing.T) {
	s := newWebhookServer(map[string]string{"my_dag": "supersecret"})

	req := httptest.NewRequest(http.MethodPost, "/webhook/", nil)
	req.Header.Set("Authorization", "Bearer supersecret")
	w := httptest.NewRecorder()

	s.webhookHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}
