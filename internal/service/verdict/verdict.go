// Package verdict collects the 3 engine results, decides the final verdict,
// sends it to the API through Redis and saves it in Postgres.
package verdict

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"xray-go/internal/model"
	"xray-go/internal/repo"
)

// If an engine is missing, wait this long after the first result, then decide anyway.
const waitForRest = 3 * time.Second

type pending struct {
	first   time.Time
	results map[string]model.EngineResult
}

type Service struct {
	DB    *pgxpool.Pool
	Redis *goredis.Client

	mu      sync.Mutex
	waiting map[string]*pending // by inspection id
}

func New(db *pgxpool.Pool, rdb *goredis.Client) *Service {
	return &Service{DB: db, Redis: rdb, waiting: map[string]*pending{}}
}

// Add stores one engine result. When all 3 are in, it finishes the inspection.
func (s *Service) Add(ctx context.Context, res model.EngineResult) {
	s.mu.Lock()
	p, ok := s.waiting[res.InspectionID]
	if !ok {
		p = &pending{first: time.Now(), results: map[string]model.EngineResult{}}
		s.waiting[res.InspectionID] = p
	}
	p.results[res.Engine] = res
	done := len(p.results) == len(model.Engines)
	if done {
		delete(s.waiting, res.InspectionID)
	}
	s.mu.Unlock()

	if done {
		s.finish(ctx, p.results)
	}
}

// FinishLate decides inspections where an engine never answered (fail-open).
// Run it in a goroutine.
func (s *Service) FinishLate(ctx context.Context) {
	for range time.Tick(100 * time.Millisecond) {
		var late []map[string]model.EngineResult
		s.mu.Lock()
		for id, p := range s.waiting {
			if time.Since(p.first) > waitForRest {
				late = append(late, p.results)
				delete(s.waiting, id)
			}
		}
		s.mu.Unlock()
		for _, results := range late {
			s.finish(ctx, results)
		}
	}
}

// finish sends the verdict to the API (Redis) and saves it (Postgres).
func (s *Service) finish(ctx context.Context, results map[string]model.EngineResult) {
	v := Decide(results)

	body, _ := json.Marshal(v)
	key := model.VerdictKey(v.InspectionID)
	if err := s.Redis.RPush(ctx, key, body).Err(); err != nil {
		log.Println("redis:", err)
		return
	}
	s.Redis.Expire(ctx, key, time.Minute)

	if err := repo.SaveVerdict(ctx, s.DB, v); err != nil {
		log.Println("save verdict:", err)
	}
	log.Printf("%s -> %s (%s)", v.InspectionID, v.Verdict, v.Reason)
}

// Decide: any BLOCK wins, then HOLD, otherwise ALLOW.
// Missing or timed-out engines never block (fail-open).
func Decide(results map[string]model.EngineResult) model.Verdict {
	v := model.Verdict{
		Verdict: model.Allow,
		Reason:  "No issues found",
		Details: map[string]model.EngineResult{},
	}
	var notChecked []string // engines that didn't give a real answer

	for _, name := range model.Engines {
		r, ok := results[name]
		if !ok {
			notChecked = append(notChecked, name+" no response")
			continue
		}
		if r.Result == model.Timeout || r.Result == model.Error {
			notChecked = append(notChecked, name+" "+strings.ToLower(r.Result))
		}
		v.InspectionID, v.MsgKey, v.TraceID = r.InspectionID, r.MsgKey, r.TraceID
		v.Details[strings.ToLower(name)+"Result"] = r

		if r.Result == model.Block && v.Verdict != model.Block {
			v.Verdict, v.Reason = model.Block, r.Reason
		}
		if r.Result == model.Hold && v.Verdict == model.Allow {
			v.Verdict, v.Reason = model.Hold, r.Reason
		}
	}

	if len(notChecked) > 0 {
		v.Reason += " (not checked: " + strings.Join(notChecked, ", ") + ")"
	}
	return v
}
