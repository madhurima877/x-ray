package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"xray/internal/ai"
	"xray/internal/keywords"
	"xray/internal/model"
	"xray/internal/urls"
)

type fakeStore struct {
	mu     sync.Mutex
	cache  map[string]model.URLVerdict
	black  map[string]bool
	white  map[string]bool
	cached map[string]model.URLVerdict // what CacheVerdicts wrote
}

func (f *fakeStore) CachedVerdicts(_ context.Context, us []string) (map[string]model.URLVerdict, error) {
	out := map[string]model.URLVerdict{}
	for _, u := range us {
		if v, ok := f.cache[u]; ok {
			out[u] = v
		}
	}
	return out, nil
}

func (f *fakeStore) CacheVerdicts(_ context.Context, m map[string]model.URLVerdict) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cached == nil {
		f.cached = map[string]model.URLVerdict{}
	}
	for k, v := range m {
		f.cached[k] = v
	}
	return nil
}

func (f *fakeStore) CheckLists(_ context.Context, _ string, items []string) (model.ListHits, error) {
	h := model.ListHits{Black: map[string]bool{}, White: map[string]bool{}}
	for _, it := range items {
		if f.black[it] {
			h.Black[it] = true
		} else if f.white[it] {
			h.White[it] = true
		}
	}
	return h, nil
}

type fakeExpander map[string][]string

func (f fakeExpander) Expand(ctx context.Context, u string) ([]string, error) {
	if c, ok := f[u]; ok {
		return c, nil
	}
	return []string{u}, errors.New("unreachable")
}

type fakeScorer struct {
	score float64
	delay time.Duration
}

func (f fakeScorer) Score(ctx context.Context, _ string) (ai.Result, error) {
	select {
	case <-time.After(f.delay):
		return ai.Result{Score: f.score, ModelVersion: "test"}, nil
	case <-ctx.Done():
		return ai.Result{}, ctx.Err()
	}
}

type fixedKeywords struct{ m *keywords.Matcher }

func (k fixedKeywords) Matcher() *keywords.Matcher { return k.m }

type recorder struct {
	mu   sync.Mutex
	sent []model.Verdict
}

func (r *recorder) Send(_ context.Context, _ string, v model.Verdict) error {
	r.mu.Lock()
	r.sent = append(r.sent, v)
	r.mu.Unlock()
	return nil
}
func (r *recorder) Publish(model.Verdict) {}
func (r *recorder) Close() error          { return nil }

type nopMetrics struct{}

func (nopMetrics) ObserveVerdict(model.Action, time.Duration) {}
func (nopMetrics) CallbackError()                             {}

func newTestPipeline(st *fakeStore, ex fakeExpander, sc Scorer, timeout time.Duration) (*Pipeline, *recorder) {
	rec := &recorder{}
	p := New(Config{Workers: 2, JobTimeout: timeout, URLTimeout: time.Second, URLConcurrency: 4, CallbackTimeout: time.Second},
		st, ex, sc, fixedKeywords{keywords.Build([]string{"free money"})}, rec, rec, nopMetrics{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	return p, rec
}

func job(level model.Level, sender, text string) model.Job {
	return model.Job{
		Message:    model.Message{ID: "m-1", Sender: sender, Text: text},
		Customer:   model.Customer{ID: "c", Level: level, CallbackURL: "http://relay/cb", KeywordAction: model.Hold, AIBlock: 0.9, AIHold: 0.7},
		URLs:       urls.Extract(text),
		ReceivedAt: time.Now(),
	}
}

func TestProcess(t *testing.T) {
	st := &fakeStore{
		cache: map[string]model.URLVerdict{"http://cached.example/": {Action: model.Block, Detail: "cached"}},
		black: map[string]bool{"evil.example": true, "+100": true},
		white: map[string]bool{"+999": true, "good.example": true},
	}
	ex := fakeExpander{"http://bit.ly/x": {"http://bit.ly/x", "http://evil.example/landing"}}

	tests := []struct {
		name   string
		level  model.Level
		sender string
		text   string
		scorer Scorer
		want   model.Action
	}{
		{"clean T1", model.T1, "+1", "hello", nil, model.Allow},
		{"cached block", model.T1, "+1", "see cached.example/", nil, model.Block},
		{"blacklisted host", model.T1, "+1", "go http://evil.example/a", nil, model.Block},
		{"blacklisted sender", model.T1, "+100", "hello", nil, model.Block},
		{"whitelisted sender skips keywords", model.T5, "+999", "free money", fakeScorer{score: 1}, model.Allow},
		{"keyword ignored at T1", model.T1, "+1", "free money", nil, model.Allow},
		{"keyword hold at T2", model.T2, "+1", "free money", nil, model.Hold},
		{"ai hold at T3", model.T3, "+1", "hi", fakeScorer{score: 0.75}, model.Hold},
		{"ai block at T3", model.T3, "+1", "hi", fakeScorer{score: 0.95}, model.Block},
		{"short link not expanded at T3", model.T3, "+1", "bit.ly/x", fakeScorer{}, model.Allow},
		{"short link to blacklist at T4", model.T4, "+1", "bit.ly/x", fakeScorer{}, model.Block},
		{"unreachable url allowed at T4", model.T4, "+1", "http://down.example/", nil, model.Allow},
		{"unreachable url held at T5", model.T5, "+1", "http://down.example/", nil, model.Hold},
		{"whitelisted url host", model.T5, "+1", "http://good.example/", nil, model.Allow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, rec := newTestPipeline(st, ex, tt.scorer, time.Second)
			v := p.Process(job(tt.level, tt.sender, tt.text))
			if v.Action != tt.want {
				t.Fatalf("verdict %s, want %s; reasons %+v", v.Action, tt.want, v.Reasons)
			}
			if len(rec.sent) != 1 || rec.sent[0].MessageID != "m-1" {
				t.Fatalf("callback not sent once: %+v", rec.sent)
			}
		})
	}
	if v := st.cached["http://bit.ly/x"]; v.Action != model.Block {
		t.Errorf("expanded URL verdict not cached: %+v", st.cached)
	}
}

func TestProcessDeadlineFailsOpen(t *testing.T) {
	p, rec := newTestPipeline(&fakeStore{}, nil, fakeScorer{score: 0.95, delay: time.Second}, 50*time.Millisecond)
	start := time.Now()
	v := p.Process(job(model.T3, "+1", "hi"))
	if v.Action != model.FailOpen {
		t.Fatalf("verdict %s, want FAIL_OPEN", v.Action)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("hard stop not respected: %s", time.Since(start))
	}
	if len(rec.sent) != 1 {
		t.Fatal("FAIL_OPEN callback not sent")
	}
}

func TestCombine(t *testing.T) {
	rs := []model.Reason{{Action: model.Allow}, {Action: model.Hold}, {Action: model.Block}, {Action: model.Allow}}
	if got := Combine(rs); got != model.Block {
		t.Fatalf("Combine = %s", got)
	}
	if got := Combine(nil); got != model.Allow {
		t.Fatalf("Combine(nil) = %s", got)
	}
}
