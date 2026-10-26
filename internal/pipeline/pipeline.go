// Package pipeline is the worker pool: each worker takes a job from the
// channel, runs steps 5–10 under a hard deadline, then calls back the relay
// (11) and publishes the result event (12).
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"xray/internal/ai"
	"xray/internal/events"
	"xray/internal/keywords"
	"xray/internal/model"
	"xray/internal/urls"
)

type Store interface {
	CachedVerdicts(ctx context.Context, urls []string) (map[string]model.URLVerdict, error)
	CacheVerdicts(ctx context.Context, verdicts map[string]model.URLVerdict) error
	CheckLists(ctx context.Context, customerID string, items []string) (model.ListHits, error)
}

type Expander interface {
	Expand(ctx context.Context, url string) ([]string, error)
}

type Scorer interface {
	Score(ctx context.Context, text string) (ai.Result, error)
}

type Keywords interface {
	Matcher() *keywords.Matcher
}

type Notifier interface {
	Send(ctx context.Context, url string, v model.Verdict) error
}

type Metrics interface {
	ObserveVerdict(a model.Action, d time.Duration)
	CallbackError()
}

type Config struct {
	Workers         int
	JobTimeout      time.Duration
	URLTimeout      time.Duration
	URLConcurrency  int
	CallbackTimeout time.Duration
}

type Pipeline struct {
	cfg       Config
	store     Store
	expander  Expander
	scorer    Scorer // nil → AI step skipped
	keywords  Keywords
	notifier  Notifier
	publisher events.Publisher
	metrics   Metrics
	log       *slog.Logger
}

func New(cfg Config, store Store, expander Expander, scorer Scorer, kw Keywords,
	notifier Notifier, publisher events.Publisher, metrics Metrics, log *slog.Logger) *Pipeline {
	return &Pipeline{cfg: cfg, store: store, expander: expander, scorer: scorer, keywords: kw,
		notifier: notifier, publisher: publisher, metrics: metrics, log: log}
}

// Start launches the pool of workers. They stop once jobs is closed and
// drained; the returned function waits for that.
func (p *Pipeline) Start(jobs <-chan model.Job) (wait func()) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				p.Process(job)
			}
		}()
	}
	return wg.Wait
}

// Process handles one job end to end and returns the verdict it sent.
func (p *Pipeline) Process(job model.Job) model.Verdict {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.JobTimeout)
	defer cancel()

	type result struct {
		out outcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := p.evaluate(ctx, job)
		done <- result{out, err}
	}()

	// 10 Combine verdict, or FAIL_OPEN when the hard stop is reached.
	var v model.Verdict
	select {
	case r := <-done:
		if r.err != nil {
			v = failOpen(fmt.Sprintf("inspection aborted: %v", r.err))
		} else {
			v = model.Verdict{Action: Combine(r.out.reasons), Reasons: r.out.reasons,
				AIScore: r.out.aiScore, ModelVersion: r.out.modelVersion}
		}
	case <-ctx.Done():
		v = failOpen(fmt.Sprintf("no verdict within %s", p.cfg.JobTimeout))
	}
	v.MessageID = job.Message.ID
	v.CustomerID = job.Customer.ID
	v.Level = job.Customer.Level
	v.DecidedAt = time.Now().UTC()
	elapsed := time.Since(job.ReceivedAt)
	v.LatencyMS = float64(elapsed.Microseconds()) / 1000
	p.metrics.ObserveVerdict(v.Action, elapsed)

	// 11 Callback to relay. It gets its own deadline: a FAIL_OPEN after the
	// job deadline still has to reach the relay.
	if cb := job.Customer.CallbackURL; cb != "" {
		cctx, ccancel := context.WithTimeout(context.Background(), p.cfg.CallbackTimeout)
		if err := p.notifier.Send(cctx, cb, v); err != nil {
			p.metrics.CallbackError()
			p.log.Warn("callback failed", "component", "callback", "message_id", v.MessageID,
				"customer_id", v.CustomerID, "err", err)
		}
		ccancel()
	}

	// 12 Publish result event (non-blocking).
	p.publisher.Publish(model.Event{Verdict: v, Sender: job.Message.Sender, URLs: job.URLs})
	return v
}

func failOpen(detail string) model.Verdict {
	return model.Verdict{
		Action:  model.FailOpen,
		Reasons: []model.Reason{{Step: "deadline", Action: model.FailOpen, Detail: detail}},
	}
}

// Combine picks the most severe action: BLOCK > HOLD > ALLOW.
func Combine(reasons []model.Reason) model.Action {
	out := model.Allow
	for _, r := range reasons {
		if r.Action.Severity() > out.Severity() {
			out = r.Action
		}
	}
	return out
}

type outcome struct {
	reasons      []model.Reason
	aiScore      *float64
	modelVersion string
}

func (o *outcome) add(step string, a model.Action, detail string) {
	o.reasons = append(o.reasons, model.Reason{Step: step, Action: a, Detail: detail})
}

func (o *outcome) blocked() bool { return Combine(o.reasons) == model.Block }

// errDecided cancels the parallel checks once one of them has found a BLOCK.
var errDecided = errors.New("verdict decided")

// evaluate runs steps 5–9. It returns an error only when the job deadline
// cut it short before anything decisive was found.
func (p *Pipeline) evaluate(ctx context.Context, job model.Job) (outcome, error) {
	var out outcome
	level := job.Customer.Level
	fresh := map[string]model.URLVerdict{} // URL verdicts worked out now, cached on return
	defer p.cacheURLs(ctx, fresh)

	// 5 Verdict cache: one MGET for all URLs.
	pending := job.URLs
	if len(job.URLs) > 0 {
		cached, err := p.store.CachedVerdicts(ctx, job.URLs)
		if err != nil {
			p.log.Warn("verdict cache read failed", "component", "pipeline", "err", err)
		}
		pending = make([]string, 0, len(job.URLs))
		for _, u := range job.URLs {
			if v, ok := cached[u]; ok {
				out.add("cache", v.Action, u+" "+v.Detail)
				continue
			}
			pending = append(pending, u)
		}
		if out.blocked() {
			return out, nil
		}
	}

	// 6 Black List, then White List: sender ID and every URL host, one pipeline.
	sender := job.Message.Sender
	items := make([]string, 0, len(pending)+1)
	if sender != "" {
		items = append(items, sender)
	}
	for _, u := range pending {
		items = append(items, urls.Host(u))
	}
	hits, err := p.store.CheckLists(ctx, job.Customer.ID, items)
	if err != nil {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		p.log.Warn("list check failed", "component", "pipeline", "err", err)
	}
	if hits.Black[sender] {
		out.add("blacklist", model.Block, "sender "+sender)
		return out, nil
	}
	remaining := make([]string, 0, len(pending))
	for _, u := range pending {
		switch h := urls.Host(u); {
		case hits.Black[h]:
			out.add("blacklist", model.Block, u)
			fresh[u] = model.URLVerdict{Action: model.Block, Detail: "host " + h + " on Black List"}
		case hits.White[h]:
			out.add("whitelist", model.Allow, u)
			fresh[u] = model.URLVerdict{Action: model.Allow, Detail: "host " + h + " on White List"}
		default:
			remaining = append(remaining, u)
		}
	}
	if out.blocked() {
		return out, nil
	}
	if hits.White[sender] {
		out.add("whitelist", model.Allow, "sender "+sender)
		return out, nil
	}

	// 7 Keywords (T2+).
	if level >= model.T2 && p.keywords != nil {
		if found := p.keywords.Matcher().Find(job.Message.Text); len(found) > 0 {
			out.add("keywords", job.Customer.KeywordAction, strings.Join(found, ", "))
			if out.blocked() {
				return out, nil
			}
		}
	}

	// 8 + 9 in parallel: one goroutine per URL (T4+) and one for AI (T3+).
	doURLs := level >= model.T4 && len(remaining) > 0
	doAI := level >= model.T3 && p.scorer != nil
	if doURLs || doAI {
		g, gctx := errgroup.WithContext(ctx)
		var mu sync.Mutex
		if doAI {
			g.Go(func() error {
				res, err := p.scorer.Score(gctx, job.Message.Text)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if gctx.Err() == nil {
						out.add("ai", model.Allow, "AI unavailable, skipped: "+err.Error())
					}
					return nil
				}
				a := model.Allow
				switch {
				case res.Score >= job.Customer.AIBlock:
					a = model.Block
				case res.Score >= job.Customer.AIHold:
					a = model.Hold
				}
				out.aiScore, out.modelVersion = &res.Score, res.ModelVersion
				out.add("ai", a, fmt.Sprintf("score %.3f (block ≥ %.2f, hold ≥ %.2f)", res.Score, job.Customer.AIBlock, job.Customer.AIHold))
				if a == model.Block {
					return errDecided
				}
				return nil
			})
		}
		if doURLs {
			sem := make(chan struct{}, p.cfg.URLConcurrency)
			for _, u := range remaining {
				g.Go(func() error {
					select {
					case sem <- struct{}{}:
					case <-gctx.Done():
						return nil
					}
					defer func() { <-sem }()
					v, err := p.inspectURL(gctx, job.Customer.ID, u)
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						if gctx.Err() != nil {
							return nil
						}
						if level >= model.T5 {
							out.add("url", model.Hold, u+" could not be resolved: "+err.Error())
						} else {
							out.add("url", model.Allow, u+" could not be resolved, skipped: "+err.Error())
						}
						return nil
					}
					out.add("url", v.Action, u+" "+v.Detail)
					fresh[u] = v
					if v.Action == model.Block {
						return errDecided
					}
					return nil
				})
			}
		}
		if err := g.Wait(); err != nil && !errors.Is(err, errDecided) {
			return out, err
		}
	}

	if ctx.Err() != nil && !out.blocked() {
		return out, ctx.Err()
	}
	return out, nil
}

// inspectURL expands one URL and re-checks every host it redirects to
// against the Black List.
func (p *Pipeline) inspectURL(ctx context.Context, customerID, u string) (model.URLVerdict, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.URLTimeout)
	defer cancel()
	chain, err := p.expander.Expand(ctx, u)
	if err != nil {
		return model.URLVerdict{}, err
	}
	if len(chain) == 1 {
		return model.URLVerdict{Action: model.Allow, Detail: "no redirect"}, nil
	}
	hosts := make([]string, 0, len(chain)-1)
	for _, c := range chain[1:] {
		hosts = append(hosts, urls.Host(c))
	}
	hits, err := p.store.CheckLists(ctx, customerID, hosts)
	if err != nil {
		return model.URLVerdict{}, err
	}
	for _, h := range hosts {
		if hits.Black[h] {
			return model.URLVerdict{Action: model.Block, Detail: "redirects to blacklisted host " + h}, nil
		}
	}
	return model.URLVerdict{Action: model.Allow, Detail: "resolves to " + chain[len(chain)-1]}, nil
}

func (p *Pipeline) cacheURLs(ctx context.Context, fresh map[string]model.URLVerdict) {
	if len(fresh) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 200*time.Millisecond)
	defer cancel()
	if err := p.store.CacheVerdicts(ctx, fresh); err != nil {
		p.log.Warn("verdict cache write failed", "component", "pipeline", "err", err)
	}
}
