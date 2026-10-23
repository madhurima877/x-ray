// Package config reads all settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr string

	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisPoolSize int

	KafkaBrokers []string // empty → events are logged instead of published
	KafkaTopic   string

	AIURL     string // empty → AI step is skipped
	AITimeout time.Duration

	Workers        int
	QueueSize      int
	JobTimeout     time.Duration // hard stop per message (RFP max is 5 s)
	URLTimeout     time.Duration
	URLConcurrency int
	URLMaxHops     int
	AllowPrivate   bool // let URL expansion reach private IPs (tests/local only)

	CallbackTimeout time.Duration
	CallbackRetries int

	KeywordReload    time.Duration
	URLCacheTTL      time.Duration
	CustomerCacheTTL time.Duration
	MaxBodyBytes     int64
	ShutdownTimeout  time.Duration
}

func Load() (Config, error) {
	p := parser{}
	c := Config{
		Addr:             p.str("XRAY_ADDR", ":8080"),
		RedisAddr:        p.str("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    p.str("REDIS_PASSWORD", ""),
		RedisDB:          p.int("REDIS_DB", 0),
		RedisPoolSize:    p.int("REDIS_POOL_SIZE", 200),
		KafkaTopic:       p.str("KAFKA_TOPIC", "xray.verdicts"),
		AIURL:            p.str("AI_URL", ""),
		AITimeout:        p.dur("AI_TIMEOUT", 300*time.Millisecond),
		Workers:          p.int("WORKERS", 512),
		QueueSize:        p.int("QUEUE_SIZE", 10000),
		JobTimeout:       p.dur("JOB_TIMEOUT", 4500*time.Millisecond),
		URLTimeout:       p.dur("URL_TIMEOUT", time.Second),
		URLConcurrency:   p.int("URL_CONCURRENCY", 4),
		URLMaxHops:       p.int("URL_MAX_HOPS", 5),
		AllowPrivate:     p.bool("ALLOW_PRIVATE_URLS", false),
		CallbackTimeout:  p.dur("CALLBACK_TIMEOUT", 2*time.Second),
		CallbackRetries:  p.int("CALLBACK_RETRIES", 2),
		KeywordReload:    p.dur("KEYWORD_RELOAD", 30*time.Second),
		URLCacheTTL:      p.dur("URL_CACHE_TTL", 10*time.Minute),
		CustomerCacheTTL: p.dur("CUSTOMER_CACHE_TTL", 10*time.Second),
		MaxBodyBytes:     int64(p.int("MAX_BODY_BYTES", 64<<10)),
		ShutdownTimeout:  p.dur("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
	if b := p.str("KAFKA_BROKERS", ""); b != "" {
		c.KafkaBrokers = strings.Split(b, ",")
	}
	if p.err != nil {
		return c, p.err
	}
	if c.Workers < 1 || c.QueueSize < 1 || c.URLConcurrency < 1 {
		return c, fmt.Errorf("WORKERS, QUEUE_SIZE and URL_CONCURRENCY must be ≥ 1")
	}
	return c, nil
}

// parser keeps the first error so Load can read every variable in one go.
type parser struct{ err error }

func (p *parser) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func (p *parser) int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s: %w", key, err)
	}
	return n
}

func (p *parser) dur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s: %w", key, err)
	}
	return d
}

func (p *parser) bool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s: %w", key, err)
	}
	return b
}
