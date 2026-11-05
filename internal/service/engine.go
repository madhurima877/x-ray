// Package service holds the shared engine loop. Each engine's check lives in its own folder.
package service

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"xray-go/internal/kafka"
	"xray-go/internal/model"
)

// Check looks at the content and returns result, reason and extra details.
type Check func(ctx context.Context, content string) (string, string, map[string]any)

// RunEngine reads requests from Kafka, runs the check, and sends the result back.
func RunEngine(name string, budget time.Duration, check Check) {
	ctx := context.Background()
	if err := kafka.CreateTopics(model.TopicRequests, model.TopicResults); err != nil {
		log.Fatal("kafka: ", err)
	}
	// Each engine has its own group, so every engine gets every request.
	kr := kafka.NewReader(model.TopicRequests, "engine-"+strings.ToLower(name))
	kw := kafka.NewWriter(model.TopicResults)

	log.Println(name, "engine started")
	for {
		m, err := kr.ReadMessage(ctx)
		if err != nil {
			log.Println("read:", err)
			continue
		}

		var req model.InspectRequest
		if err := json.Unmarshal(m.Value, &req); err != nil {
			log.Println("bad message:", err)
			continue
		}

		res := Inspect(ctx, name, budget, check, req)
		if err := kafka.Publish(ctx, kw, req.InspectionID, res); err != nil {
			log.Println("publish:", err)
		}
		log.Printf("%s %s -> %s", req.InspectionID, name, res.Result)
	}
}

// Inspect runs the check, but gives up with TIMEOUT if it takes longer than budget.
func Inspect(ctx context.Context, name string, budget time.Duration, check Check, req model.InspectRequest) model.EngineResult {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	res := model.EngineResult{
		InspectionID: req.InspectionID,
		MsgKey:       req.MsgKey,
		TraceID:      req.TraceID,
		Engine:       name,
		Result:       model.Timeout,
		Reason:       "engine took too long",
		Details:      map[string]any{},
	}

	type answer struct {
		result, reason string
		details        map[string]any
	}
	done := make(chan answer, 1)
	go func() {
		result, reason, details := check(ctx, req.Content)
		// Design rule: one quick retry on failure, then mark the engine unavailable.
		if result == model.Error && ctx.Err() == nil {
			result, reason, details = check(ctx, req.Content)
			if result == model.Error {
				reason = "engine unavailable: " + reason
			}
		}
		done <- answer{result, reason, details}
	}()

	select {
	case a := <-done:
		res.Result, res.Reason = a.result, a.reason
		if a.details != nil {
			res.Details = a.details
		}
	case <-ctx.Done():
		// keep TIMEOUT
	}
	res.Timestamp = time.Now().UTC()
	return res
}
