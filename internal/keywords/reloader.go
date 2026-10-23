package keywords

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

type Source interface {
	Keywords(ctx context.Context) ([]string, error)
}

// Reloader rebuilds the Matcher on a ticker and swaps it in atomically, so
// workers never wait for a reload.
type Reloader struct {
	src      Source
	interval time.Duration
	log      *slog.Logger
	cur      atomic.Pointer[Matcher]
}

func NewReloader(src Source, interval time.Duration, log *slog.Logger) *Reloader {
	return &Reloader{src: src, interval: interval, log: log}
}

// Matcher returns the current automaton (nil before the first load).
func (r *Reloader) Matcher() *Matcher { return r.cur.Load() }

// Load reads the keyword list once and swaps the new automaton in.
func (r *Reloader) Load(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	kws, err := r.src.Keywords(ctx)
	if err != nil {
		return err
	}
	m := Build(kws)
	r.cur.Store(m)
	r.log.Info("keywords loaded", "component", "keywords", "count", m.Len())
	return nil
}

// Run reloads until ctx is cancelled. A failed reload keeps the old list.
func (r *Reloader) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Load(ctx); err != nil {
				r.log.Warn("keyword reload failed, keeping previous list", "component", "keywords", "err", err)
			}
		}
	}
}
