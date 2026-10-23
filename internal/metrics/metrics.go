// Package metrics counts traffic and verdicts and, once a second, turns the
// counters into TPS, latency and queue figures served on /metrics (for S-09).
package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"xray/internal/model"
)

type Metrics struct {
	queueLen func() int
	queueCap int

	received, accepted, ingressFailOpen, badRequests atomic.Int64
	allow, hold, block, failOpen                     atomic.Int64
	callbackErrors                                   atomic.Int64

	// reset every second by Run
	windowCount, windowSumUS, windowMaxUS atomic.Int64

	mu   sync.RWMutex
	snap Snapshot
}

type Snapshot struct {
	TPS             int64            `json:"tps"`
	AvgLatencyMS    float64          `json:"avg_latency_ms"`
	MaxLatencyMS    float64          `json:"max_latency_ms"`
	QueueLen        int              `json:"queue_len"`
	QueueCap        int              `json:"queue_cap"`
	Received        int64            `json:"received_total"`
	Accepted        int64            `json:"accepted_total"`
	BadRequests     int64            `json:"bad_requests_total"`
	IngressFailOpen int64            `json:"ingress_fail_open_total"`
	Verdicts        map[string]int64 `json:"verdicts_total"`
	CallbackErrors  int64            `json:"callback_errors_total"`
	At              time.Time        `json:"at"`
}

func New(queueLen func() int, queueCap int) *Metrics {
	return &Metrics{queueLen: queueLen, queueCap: queueCap}
}

func (m *Metrics) Received()        { m.received.Add(1) }
func (m *Metrics) Accepted()        { m.accepted.Add(1) }
func (m *Metrics) BadRequest()      { m.badRequests.Add(1) }
func (m *Metrics) IngressFailOpen() { m.ingressFailOpen.Add(1) }
func (m *Metrics) CallbackError()   { m.callbackErrors.Add(1) }

// ObserveVerdict records one finished message and its end-to-end latency.
func (m *Metrics) ObserveVerdict(a model.Action, d time.Duration) {
	switch a {
	case model.Allow:
		m.allow.Add(1)
	case model.Hold:
		m.hold.Add(1)
	case model.Block:
		m.block.Add(1)
	default:
		m.failOpen.Add(1)
	}
	us := d.Microseconds()
	m.windowCount.Add(1)
	m.windowSumUS.Add(us)
	for {
		cur := m.windowMaxUS.Load()
		if us <= cur || m.windowMaxUS.CompareAndSwap(cur, us) {
			break
		}
	}
}

// Run refreshes the snapshot every second until ctx is cancelled.
func (m *Metrics) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			m.tick(now)
		}
	}
}

func (m *Metrics) tick(now time.Time) {
	n := m.windowCount.Swap(0)
	sum := m.windowSumUS.Swap(0)
	maxUS := m.windowMaxUS.Swap(0)
	s := m.totals()
	s.TPS = n
	if n > 0 {
		s.AvgLatencyMS = float64(sum) / float64(n) / 1000
	}
	s.MaxLatencyMS = float64(maxUS) / 1000
	s.At = now.UTC()
	m.mu.Lock()
	m.snap = s
	m.mu.Unlock()
}

func (m *Metrics) totals() Snapshot {
	return Snapshot{
		QueueLen:        m.queueLen(),
		QueueCap:        m.queueCap,
		Received:        m.received.Load(),
		Accepted:        m.accepted.Load(),
		BadRequests:     m.badRequests.Load(),
		IngressFailOpen: m.ingressFailOpen.Load(),
		Verdicts: map[string]int64{
			string(model.Allow):    m.allow.Load(),
			string(model.Hold):     m.hold.Load(),
			string(model.Block):    m.block.Load(),
			string(model.FailOpen): m.failOpen.Load(),
		},
		CallbackErrors: m.callbackErrors.Load(),
	}
}

// Snapshot returns the last per-second figures with live totals.
func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	s := m.snap
	m.mu.RUnlock()
	live := m.totals()
	live.TPS, live.AvgLatencyMS, live.MaxLatencyMS, live.At = s.TPS, s.AvgLatencyMS, s.MaxLatencyMS, s.At
	return live
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m.Snapshot())
}
