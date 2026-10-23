// Package model holds the types shared by the handler, the workers and the
// outside systems (relay callback, Kafka).
package model

import (
	"strings"
	"time"
)

// Action is the verdict for one message or one URL.
type Action string

const (
	Allow    Action = "ALLOW"
	Hold     Action = "HOLD"
	Block    Action = "BLOCK"
	FailOpen Action = "FAIL_OPEN" // X-Ray could not decide in time; the relay lets the message through
)

// Severity orders actions for combining: BLOCK > HOLD > ALLOW.
func (a Action) Severity() int {
	switch a {
	case Block:
		return 2
	case Hold:
		return 1
	}
	return 0
}

// ParseAction reads ALLOW/HOLD/BLOCK, falling back to def for anything else.
func ParseAction(s string, def Action) Action {
	switch a := Action(strings.ToUpper(strings.TrimSpace(s))); a {
	case Allow, Hold, Block:
		return a
	}
	return def
}

// Level is the inspection level T1–T5 a customer pays for. Levels are
// cumulative: each one adds checks on top of the one below.
//
//	T1  verdict cache + Black/White List
//	T2  + keywords
//	T3  + AI text score
//	T4  + URL expansion (short links, redirects)
//	T5  + strict: a URL that cannot be resolved → HOLD
type Level int

const (
	T1 Level = iota + 1
	T2
	T3
	T4
	T5
)

// Customer is the per-customer copy and policy read from Redis.
type Customer struct {
	ID            string  `json:"id"`
	Level         Level   `json:"level"`
	RateLimit     int     `json:"rate_limit"` // messages per second, 0 = unlimited
	CallbackURL   string  `json:"callback_url"`
	KeywordAction Action  `json:"keyword_action"` // what a keyword hit means: HOLD or BLOCK
	AIBlock       float64 `json:"ai_block"`       // AI score at or above this → BLOCK
	AIHold        float64 `json:"ai_hold"`        // AI score at or above this → HOLD
}

// Message is the body of POST /inspect.
type Message struct {
	ID        string `json:"message_id"`
	Sender    string `json:"sender,omitempty"`
	Recipient string `json:"recipient,omitempty"`
	Text      string `json:"text"`
}

// Job is what the handler puts on the job channel.
type Job struct {
	Message    Message
	Customer   Customer
	URLs       []string // normalised, deduplicated
	ReceivedAt time.Time
}

// Reason records why a step chose an action.
type Reason struct {
	Step   string `json:"step"`
	Action Action `json:"action"`
	Detail string `json:"detail"`
}

// Verdict is sent to the relay callback and published to Kafka.
type Verdict struct {
	MessageID    string    `json:"message_id"`
	CustomerID   string    `json:"customer_id"`
	Action       Action    `json:"verdict"`
	Level        Level     `json:"level"`
	Reasons      []Reason  `json:"reasons"`
	AIScore      *float64  `json:"ai_score,omitempty"`
	ModelVersion string    `json:"model_version,omitempty"`
	LatencyMS    float64   `json:"latency_ms"`
	DecidedAt    time.Time `json:"decided_at"`
}

// URLVerdict is the cached result for one normalised URL.
type URLVerdict struct {
	Action Action `json:"action"`
	Detail string `json:"detail,omitempty"`
}

// ListHits says which checked items are on the Black or White List.
type ListHits struct {
	Black map[string]bool
	White map[string]bool
}
