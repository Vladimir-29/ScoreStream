package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakePublisher struct {
	key   string
	value []byte
	err   error
	calls int
}

func (f *fakePublisher) Publish(_ context.Context, key string, value []byte) error {
	f.calls++
	f.key = key
	f.value = value
	return f.err
}

func newRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestCreateScore_Valid(t *testing.T) {
	pub := &fakePublisher{}
	srv := NewServer(pub, testLogger()).Handler()

	body := `{"event_id":"evt-1","leaderboard_id":"weekly","user_id":"user-1","points":100}`
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newRequest(t, http.MethodPost, "/api/v1/scores", body))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	var resp struct {
		EventID string `json:"event_id"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID != "evt-1" {
		t.Fatalf("event_id = %q, want evt-1", resp.EventID)
	}
	if pub.calls != 1 {
		t.Fatalf("publisher calls = %d, want 1", pub.calls)
	}
	if pub.key != "weekly:user-1" {
		t.Fatalf("key = %q, want weekly:user-1", pub.key)
	}
	var event map[string]any
	if err := json.Unmarshal(pub.value, &event); err != nil {
		t.Fatalf("published value is not JSON: %v", err)
	}
	if event["points"].(float64) != 100 {
		t.Fatalf("published points = %v, want 100", event["points"])
	}
}

func TestCreateScore_MissingFields(t *testing.T) {
	cases := []string{
		`{"leaderboard_id":"weekly","user_id":"u","points":10}`,
		`{"event_id":"e","user_id":"u","points":10}`,
		`{"event_id":"e","leaderboard_id":"weekly","points":10}`,
	}
	for _, body := range cases {
		pub := &fakePublisher{}
		srv := NewServer(pub, testLogger()).Handler()
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, newRequest(t, http.MethodPost, "/api/v1/scores", body))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d, want 400", body, rec.Code)
		}
		if pub.calls != 0 {
			t.Fatalf("body %s: publisher called %d times, want 0", body, pub.calls)
		}
	}
}

func TestCreateScore_InvalidPoints(t *testing.T) {
	for _, points := range []string{"0", "-5"} {
		body := `{"event_id":"e","leaderboard_id":"l","user_id":"u","points":` + points + `}`
		pub := &fakePublisher{}
		srv := NewServer(pub, testLogger()).Handler()
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, newRequest(t, http.MethodPost, "/api/v1/scores", body))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("points %s: status = %d, want 400", points, rec.Code)
		}
		if pub.calls != 0 {
			t.Fatalf("points %s: publisher called", points)
		}
	}
}

func TestCreateScore_MalformedJSON(t *testing.T) {
	pub := &fakePublisher{}
	srv := NewServer(pub, testLogger()).Handler()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newRequest(t, http.MethodPost, "/api/v1/scores", `{not json`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if pub.calls != 0 {
		t.Fatal("publisher called on malformed JSON")
	}
}

func TestCreateScore_PublisherFailure(t *testing.T) {
	pub := &fakePublisher{err: errors.New("kafka unavailable")}
	srv := NewServer(pub, testLogger()).Handler()
	rec := httptest.NewRecorder()
	body := `{"event_id":"e","leaderboard_id":"l","user_id":"u","points":10}`
	srv.ServeHTTP(rec, newRequest(t, http.MethodPost, "/api/v1/scores", body))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestCreateScore_BodyTooLarge(t *testing.T) {
	pub := &fakePublisher{}
	srv := NewServer(pub, testLogger()).Handler()
	rec := httptest.NewRecorder()
	big := `{"event_id":"e","leaderboard_id":"` + strings.Repeat("x", maxBodyBytes) + `","user_id":"u","points":1}`
	srv.ServeHTTP(rec, newRequest(t, http.MethodPost, "/api/v1/scores", big))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if pub.calls != 0 {
		t.Fatal("publisher called on oversized body")
	}
}

func TestHealthz(t *testing.T) {
	srv := NewServer(&fakePublisher{}, testLogger()).Handler()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
