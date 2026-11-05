package model

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Kafka topics.
const (
	TopicRequests = "inspection.requests"
	TopicResults  = "inspection.results"
)

// Engine names.
const (
	EngineURL     = "URL"
	EngineKeyword = "KEYWORD"
	EngineAI      = "AI"
)

var Engines = []string{EngineURL, EngineKeyword, EngineAI}

// Results an engine or the verdict can return.
const (
	Allow   = "ALLOW"
	Block   = "BLOCK"
	Hold    = "HOLD"
	Fail    = "FAIL"
	Timeout = "TIMEOUT"
	Error   = "ERROR"
)

// InspectRequest is sent from the API to every engine.
type InspectRequest struct {
	InspectionID string    `json:"inspectionId"`
	MsgKey       string    `json:"msgKey"`
	TraceID      string    `json:"traceId"`
	Content      string    `json:"content"`
	Timestamp    time.Time `json:"timestamp"`
}

// EngineResult is the common result every engine sends to Verdict.
type EngineResult struct {
	InspectionID string         `json:"inspectionId"`
	MsgKey       string         `json:"msgKey"`
	TraceID      string         `json:"traceId"`
	Engine       string         `json:"engine"`
	Result       string         `json:"result"`
	Reason       string         `json:"reason"`
	Details      map[string]any `json:"details"`
	Timestamp    time.Time      `json:"timestamp"`
}

// Verdict is the final response returned to the caller.
type Verdict struct {
	InspectionID string                  `json:"inspectionId"`
	MsgKey       string                  `json:"msgKey"`
	TraceID      string                  `json:"traceId"`
	Verdict      string                  `json:"verdict"`
	Reason       string                  `json:"reason"`
	Details      map[string]EngineResult `json:"details"`
}

// VerdictKey is the Redis key where Verdict pushes the final result.
func VerdictKey(inspectionID string) string {
	return "verdict:" + inspectionID
}

// NewID returns an id like "INS-1a2b3c4d5e6f".
func NewID(prefix string) string {
	b := make([]byte, 6)
	rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}
