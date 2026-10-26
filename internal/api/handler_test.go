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

	"xray/internal/model"
	"xray/internal/store"
)

type fakeStore struct {
	allow    bool
	storeErr error
}

func (f fakeStore) CustomerID(_ context.Context, key string) (string, error) {
	if f.storeErr != nil {
		return "", f.storeErr
	}
	if key != "good" {
		return "", store.ErrUnknownKey
	}
	return "c1", nil
}
func (f fakeStore) Allow(context.Context, string, int) (bool, error) { return f.allow, nil }
func (f fakeStore) Customer(_ context.Context, id string) (model.Customer, error) {
	return model.Customer{ID: id, Level: model.T4, RateLimit: 10}, nil
}

type nop struct{}

func (nop) Received()           {}
func (nop) Accepted()           {}
func (nop) BadRequest()         {}
func (nop) IngressFailOpen()    {}
func (nop) Publish(model.Event) {}
func (nop) Close() error        { return nil }

func TestInspect(t *testing.T) {
	const good = `{"message_id":"m1","text":"visit https://Bit.ly/abc now"}`
	tests := []struct {
		name       string
		st         fakeStore
		queue      int
		key, body  string
		wantStatus int
		wantInBody string
	}{
		{"accepted", fakeStore{allow: true}, 1, "good", good, 202, `"ACCEPTED"`},
		{"no key", fakeStore{allow: true}, 1, "", good, 401, "missing"},
		{"unknown key", fakeStore{allow: true}, 1, "bad", good, 401, "unknown"},
		{"bad json", fakeStore{allow: true}, 1, "good", `{`, 400, "JSON"},
		{"missing text", fakeStore{allow: true}, 1, "good", `{"message_id":"m1"}`, 400, "text"},
		{"over limit", fakeStore{allow: false}, 1, "good", good, 200, "over rate limit"},
		{"queue full", fakeStore{allow: true}, 0, "good", good, 200, "queue full"},
		{"redis down", fakeStore{storeErr: errors.New("down")}, 1, "good", good, 200, "FAIL_OPEN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs := make(chan model.Job, tt.queue)
			h := NewHandler(tt.st, jobs, nop{}, nop{}, slog.New(slog.NewTextHandler(io.Discard, nil)), 1<<16)
			req := httptest.NewRequest(http.MethodPost, "/inspect", strings.NewReader(tt.body))
			if tt.key != "" {
				req.Header.Set("X-API-Key", tt.key)
			}
			rec := httptest.NewRecorder()
			h.Inspect(rec, req)
			if rec.Code != tt.wantStatus || !strings.Contains(rec.Body.String(), tt.wantInBody) {
				t.Fatalf("got %d %s, want %d containing %q", rec.Code, rec.Body, tt.wantStatus, tt.wantInBody)
			}
			if tt.wantStatus == 202 {
				j := <-jobs
				if j.Customer.Level != model.T4 || len(j.URLs) != 1 || j.URLs[0] != "https://bit.ly/abc" {
					t.Fatalf("job = %+v", j)
				}
			}
		})
	}
}

func TestReadyDraining(t *testing.T) {
	h := NewHandler(fakeStore{}, nil, nop{}, nop{}, slog.New(slog.NewTextHandler(io.Discard, nil)), 1<<16)
	ready := h.Ready(func(context.Context) error { return nil })
	rec := httptest.NewRecorder()
	ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != 200 {
		t.Fatalf("ready = %d", rec.Code)
	}
	h.SetDraining()
	rec = httptest.NewRecorder()
	ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var body map[string]string
	json.NewDecoder(rec.Body).Decode(&body)
	if rec.Code != 503 || body["error"] != "draining" {
		t.Fatalf("draining = %d %v", rec.Code, body)
	}
}
