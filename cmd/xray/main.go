// Command xray runs the X-Ray inspection API: HTTP handler, job channel,
// worker pool and the background goroutines (keyword reloader, Kafka
// producer, health + metrics, graceful shutdown).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"xray/internal/ai"
	"xray/internal/api"
	"xray/internal/callback"
	"xray/internal/config"
	"xray/internal/events"
	"xray/internal/keywords"
	"xray/internal/metrics"
	"xray/internal/model"
	"xray/internal/pipeline"
	"xray/internal/store"
	"xray/internal/urls"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("x-ray stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize,
	})
	defer rdb.Close()
	st := store.NewRedis(rdb, cfg.URLCacheTTL, cfg.CustomerCacheTTL)

	var publisher events.Publisher = events.NewLog(log)
	if len(cfg.KafkaBrokers) > 0 {
		publisher = events.NewKafka(cfg.KafkaBrokers, cfg.KafkaTopic, log)
	}

	// Background goroutines live until the workers have drained.
	bg, stopBG := context.WithCancel(context.Background())
	defer stopBG()

	kw := keywords.NewReloader(st, cfg.KeywordReload, log)
	if err := kw.Load(ctx); err != nil {
		log.Warn("initial keyword load failed, starting with none", "component", "keywords", "err", err)
	}
	go kw.Run(bg)

	jobs := make(chan model.Job, cfg.QueueSize)
	m := metrics.New(func() int { return len(jobs) }, cap(jobs))
	go m.Run(bg)

	var scorer pipeline.Scorer
	if cfg.AIURL != "" {
		scorer = ai.New(cfg.AIURL, cfg.AITimeout)
	}
	p := pipeline.New(pipeline.Config{
		Workers:         cfg.Workers,
		JobTimeout:      cfg.JobTimeout,
		URLTimeout:      cfg.URLTimeout,
		URLConcurrency:  cfg.URLConcurrency,
		CallbackTimeout: cfg.CallbackTimeout,
	}, st, urls.NewExpander(cfg.URLTimeout, cfg.URLMaxHops, cfg.AllowPrivate), scorer, kw,
		callback.New(cfg.CallbackRetries), publisher, m, log)
	waitWorkers := p.Start(jobs)

	h := api.NewHandler(st, jobs, publisher, m, log, cfg.MaxBodyBytes)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /inspect", h.Inspect)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", h.Ready(func(ctx context.Context) error { return rdb.Ping(ctx).Err() }))
	mux.Handle("GET /metrics", m)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Info("x-ray listening", "addr", cfg.Addr, "workers", cfg.Workers, "queue", cfg.QueueSize)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}

	// Graceful shutdown: fail readiness, stop taking requests, then drain the
	// job channel so every accepted message still gets its callback.
	log.Info("shutting down", "queued", len(jobs))
	h.SetDraining()
	sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	close(jobs) // safe: Shutdown has waited for every handler to return
	drained := make(chan struct{})
	go func() { waitWorkers(); close(drained) }()
	select {
	case <-drained:
		log.Info("job channel drained")
	case <-sctx.Done():
		log.Warn("shutdown timeout, jobs left undecided", "queued", len(jobs))
	}
	stopBG()
	if err := publisher.Close(); err != nil {
		log.Warn("kafka flush", "err", err)
	}
	return nil
}
