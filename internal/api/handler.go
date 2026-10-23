// Package api is the HTTP side: POST /inspect runs steps 1–4 in the request
// goroutine, pushes the job onto the channel and answers 202 in ~2 ms.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"xray/internal/events"
	"xray/internal/model"
	"xray/internal/store"
	"xray/internal/urls"
)

const (
	maxIDLen   = 128
	maxTextLen = 10000 // characters
)

type CustomerStore interface {
	CustomerID(ctx context.Context, apiKey string) (string, error)
	Allow(ctx context.Context, customerID string, limit int) (bool, error)
	Customer(ctx context.Context, id string) (model.Customer, error)
}

type Metrics interface {
	Received()
	Accepted()
	BadRequest()
	IngressFailOpen()
}

type Handler struct {
	store     CustomerStore
	jobs      chan<- model.Job
	publisher events.Publisher
	metrics   Metrics
	log       *slog.Logger
	maxBody   int64
	draining  atomic.Bool
}

func NewHandler(st CustomerStore, jobs chan<- model.Job, pub events.Publisher, m Metrics, log *slog.Logger, maxBody int64) *Handler {
	return &Handler{store: st, jobs: jobs, publisher: pub, metrics: m, log: log, maxBody: maxBody}
}

type acceptedResponse struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
}

type failOpenResponse struct {
	MessageID string       `json:"message_id"`
	Verdict   model.Action `json:"verdict"`
	Reason    string       `json:"reason"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Inspect is POST /inspect.
func (h *Handler) Inspect(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.metrics.Received()
	ctx := r.Context()

	// 1 Receive + validate.
	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		h.reject(w, http.StatusUnauthorized, "missing X-API-Key")
		return
	}
	var msg model.Message
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody)).Decode(&msg); err != nil {
		h.reject(w, http.StatusBadRequest, "body must be a JSON object: "+err.Error())
		return
	}
	if err := validate(msg); err != nil {
		h.reject(w, http.StatusBadRequest, err.Error())
		return
	}
	customerID, err := h.store.CustomerID(ctx, apiKey)
	if errors.Is(err, store.ErrUnknownKey) {
		h.reject(w, http.StatusUnauthorized, "unknown API key")
		return
	}
	if err != nil {
		h.failOpen(w, msg.ID, "", "store unavailable")
		h.log.Error("api key lookup failed", "component", "api", "message_id", msg.ID, "err", err)
		return
	}

	// 3 Find level T1–T5 (customer copy, policies). Read before step 2
	// because the per-second limit is part of the copy; it is cached in memory.
	cust, err := h.store.Customer(ctx, customerID)
	if err != nil {
		h.failOpen(w, msg.ID, customerID, "customer policy unavailable")
		h.log.Error("customer lookup failed", "component", "api", "customer_id", customerID, "err", err)
		return
	}

	// 2 Rate limit. A Redis error does not stop the message.
	if ok, err := h.store.Allow(ctx, customerID, cust.RateLimit); err != nil {
		h.log.Warn("rate limit check failed, letting message through", "component", "api", "customer_id", customerID, "err", err)
	} else if !ok {
		h.failOpen(w, msg.ID, customerID, "over rate limit")
		return
	}

	// 4 Extract URLs.
	job := model.Job{Message: msg, Customer: cust, URLs: urls.Extract(msg.Text), ReceivedAt: start}

	// Push job → channel, or FAIL_OPEN if the buffer is full.
	select {
	case h.jobs <- job:
		h.metrics.Accepted()
		writeJSON(w, http.StatusAccepted, acceptedResponse{MessageID: msg.ID, Status: "ACCEPTED"})
	default:
		h.failOpen(w, msg.ID, customerID, "queue full")
	}
}

func validate(m model.Message) error {
	switch {
	case m.ID == "":
		return errors.New("message_id is required")
	case len(m.ID) > maxIDLen:
		return errors.New("message_id is too long")
	case m.Text == "":
		return errors.New("text is required")
	case !utf8.ValidString(m.Text):
		return errors.New("text must be valid UTF-8")
	case utf8.RuneCountInString(m.Text) > maxTextLen:
		return errors.New("text is too long")
	}
	return nil
}

func (h *Handler) reject(w http.ResponseWriter, status int, msg string) {
	h.metrics.BadRequest()
	writeJSON(w, status, errorResponse{Error: msg})
}

// failOpen answers at once with FAIL_OPEN: the relay delivers the message
// and no callback follows. The event is still published for audit.
func (h *Handler) failOpen(w http.ResponseWriter, messageID, customerID, reason string) {
	h.metrics.IngressFailOpen()
	h.publisher.Publish(model.Verdict{
		MessageID:  messageID,
		CustomerID: customerID,
		Action:     model.FailOpen,
		Reasons:    []model.Reason{{Step: "ingress", Action: model.FailOpen, Detail: reason}},
		DecidedAt:  time.Now().UTC(),
	})
	writeJSON(w, http.StatusOK, failOpenResponse{MessageID: messageID, Verdict: model.FailOpen, Reason: reason})
}

// SetDraining makes /readyz fail so the load balancer stops sending traffic.
func (h *Handler) SetDraining() { h.draining.Store(true) }

// Ready is GET /readyz.
func (h *Handler) Ready(ping func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "draining"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "redis: " + err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
