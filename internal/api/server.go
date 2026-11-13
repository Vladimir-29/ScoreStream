// Package api implements the REST API for submitting score events.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/Vladimir-29/ScoreStream/internal/score"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// Publisher publishes a serialized score event under the given Kafka key.
type Publisher interface {
	Publish(ctx context.Context, key string, value []byte) error
}

// Server holds the HTTP handlers' dependencies.
type Server struct {
	publisher Publisher
	logger    *slog.Logger
}

// NewServer creates an HTTP server handler set backed by publisher.
func NewServer(publisher Publisher, logger *slog.Logger) *Server {
	return &Server{publisher: publisher, logger: logger}
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /api/v1/scores", s.createScore)
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) createScore(w http.ResponseWriter, r *http.Request) {
	event, ok := decodeEvent(w, r)
	if !ok {
		return
	}
	if err := event.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	payload, err := event.Marshal()
	if err != nil {
		s.logger.Error("marshal score event", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if err := s.publisher.Publish(r.Context(), event.Key(), payload); err != nil {
		s.logger.Error("publish score event",
			"event_id", event.EventID, "error", err)
		writeError(w, http.StatusBadGateway, "failed to publish event")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"event_id": event.EventID,
		"status":   "accepted",
	})
}

func decodeEvent(w http.ResponseWriter, r *http.Request) (score.Event, bool) {
	var event score.Event
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&event); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
		}
		return score.Event{}, false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return score.Event{}, false
	}
	return event, true
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
