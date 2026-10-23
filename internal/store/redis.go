// Package store is the Redis side of X-Ray: API keys, customer copy and
// policies, rate counters, Black/White List, keywords and the URL verdict cache.
//
// Key layout (prefix "xray:"):
//
//	apikey:{sha256(key)}   string  → customer ID
//	customer:{id}          hash    level, rate_limit, callback_url, keyword_action, ai_block, ai_hold
//	rl:{id}:{unix second}  counter per customer per second
//	bl, wl                 sets    global Black/White List (hosts and sender IDs)
//	bl:{id}, wl:{id}       sets    per-customer Black/White List
//	keywords               set     keyword list
//	urlv:{sha256(url)}     string  cached URLVerdict JSON
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"xray/internal/model"
)

const prefix = "xray:"

var ErrUnknownKey = errors.New("unknown api key")

type Redis struct {
	rdb       *redis.Client
	urlTTL    time.Duration
	keys      *ttlCache[string]
	customers *ttlCache[model.Customer]
}

// NewRedis builds the store. API keys and customer copies are kept in memory
// for customerTTL so the hot path does not hit Redis for them on every message.
func NewRedis(rdb *redis.Client, urlTTL, customerTTL time.Duration) *Redis {
	return &Redis{
		rdb:       rdb,
		urlTTL:    urlTTL,
		keys:      newTTLCache[string](customerTTL),
		customers: newTTLCache[model.Customer](customerTTL),
	}
}

func HashKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

// CustomerID resolves an API key (step 1).
func (s *Redis) CustomerID(ctx context.Context, apiKey string) (string, error) {
	h := HashKey(apiKey)
	if id, ok := s.keys.get(h); ok {
		return id, nil
	}
	id, err := s.rdb.Get(ctx, prefix+"apikey:"+h).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrUnknownKey
	}
	if err != nil {
		return "", err
	}
	s.keys.set(h, id)
	return id, nil
}

// Allow counts one message against the customer's per-second limit (step 2).
func (s *Redis) Allow(ctx context.Context, customerID string, limit int) (bool, error) {
	if limit <= 0 {
		return true, nil
	}
	key := fmt.Sprintf("%srl:%s:%d", prefix, customerID, time.Now().Unix())
	pipe := s.rdb.Pipeline()
	n := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return true, err
	}
	return n.Val() <= int64(limit), nil
}

// Customer loads the customer copy and policies (step 3).
func (s *Redis) Customer(ctx context.Context, id string) (model.Customer, error) {
	if c, ok := s.customers.get(id); ok {
		return c, nil
	}
	h, err := s.rdb.HGetAll(ctx, prefix+"customer:"+id).Result()
	if err != nil {
		return model.Customer{}, err
	}
	if len(h) == 0 {
		return model.Customer{}, ErrUnknownKey
	}
	c := parseCustomer(id, h)
	s.customers.set(id, c)
	return c, nil
}

func parseCustomer(id string, h map[string]string) model.Customer {
	c := model.Customer{
		ID:            id,
		Level:         model.T1,
		CallbackURL:   h["callback_url"],
		KeywordAction: model.ParseAction(h["keyword_action"], model.Hold),
		AIBlock:       0.9,
		AIHold:        0.7,
	}
	if n, err := strconv.Atoi(h["level"]); err == nil && n >= int(model.T1) && n <= int(model.T5) {
		c.Level = model.Level(n)
	}
	if n, err := strconv.Atoi(h["rate_limit"]); err == nil && n >= 0 {
		c.RateLimit = n
	}
	if f, err := strconv.ParseFloat(h["ai_block"], 64); err == nil {
		c.AIBlock = f
	}
	if f, err := strconv.ParseFloat(h["ai_hold"], 64); err == nil {
		c.AIHold = f
	}
	return c
}

func urlKey(u string) string {
	sum := sha256.Sum256([]byte(u))
	return prefix + "urlv:" + hex.EncodeToString(sum[:16])
}

// CachedVerdicts reads cached URL verdicts in one MGET (step 5).
func (s *Redis) CachedVerdicts(ctx context.Context, urls []string) (map[string]model.URLVerdict, error) {
	out := make(map[string]model.URLVerdict, len(urls))
	if len(urls) == 0 {
		return out, nil
	}
	keys := make([]string, len(urls))
	for i, u := range urls {
		keys[i] = urlKey(u)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return out, err
	}
	for i, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue
		}
		var uv model.URLVerdict
		if json.Unmarshal([]byte(str), &uv) == nil {
			out[urls[i]] = uv
		}
	}
	return out, nil
}

// CacheVerdicts stores URL verdicts for urlTTL.
func (s *Redis) CacheVerdicts(ctx context.Context, verdicts map[string]model.URLVerdict) error {
	if len(verdicts) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	for u, v := range verdicts {
		b, _ := json.Marshal(v)
		pipe.Set(ctx, urlKey(u), b, s.urlTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// CheckLists looks every item up in the global and customer Black and White
// Lists in one pipeline (step 6). Black wins over White.
func (s *Redis) CheckLists(ctx context.Context, customerID string, items []string) (model.ListHits, error) {
	hits := model.ListHits{Black: map[string]bool{}, White: map[string]bool{}}
	if len(items) == 0 {
		return hits, nil
	}
	// SISMEMBER rather than SMISMEMBER so Redis < 6.2 works too; it is
	// still a single round trip.
	sets := [4]string{prefix + "bl", prefix + "bl:" + customerID, prefix + "wl", prefix + "wl:" + customerID}
	pipe := s.rdb.Pipeline()
	cmds := make([][4]*redis.BoolCmd, len(items))
	for i, it := range items {
		for j, set := range sets {
			cmds[i][j] = pipe.SIsMember(ctx, set, it)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return hits, err
	}
	for i, it := range items {
		c := cmds[i]
		switch {
		case c[0].Val() || c[1].Val():
			hits.Black[it] = true
		case c[2].Val() || c[3].Val():
			hits.White[it] = true
		}
	}
	return hits, nil
}

// Keywords returns the keyword list for the reloader.
func (s *Redis) Keywords(ctx context.Context) ([]string, error) {
	return s.rdb.SMembers(ctx, prefix+"keywords").Result()
}

type ttlEntry[T any] struct {
	v   T
	exp time.Time
}

type ttlCache[T any] struct {
	mu  sync.RWMutex
	m   map[string]ttlEntry[T]
	ttl time.Duration
}

func newTTLCache[T any](ttl time.Duration) *ttlCache[T] {
	return &ttlCache[T]{m: map[string]ttlEntry[T]{}, ttl: ttl}
}

func (c *ttlCache[T]) get(k string) (T, bool) {
	c.mu.RLock()
	e, ok := c.m[k]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.exp) {
		var zero T
		return zero, false
	}
	return e.v, true
}

func (c *ttlCache[T]) set(k string, v T) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	c.m[k] = ttlEntry[T]{v: v, exp: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}
