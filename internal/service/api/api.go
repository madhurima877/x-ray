// Package api handles one inspection: save it, send it to the engines, wait for the verdict.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"

	"xray-go/internal/kafka"
	"xray-go/internal/model"
	"xray-go/internal/repo"
)

var (
	ErrSave    = errors.New("could not save inspection")
	ErrSend    = errors.New("could not send inspection")
	ErrTimeout = errors.New("no verdict within 5 seconds")
)

type Service struct {
	DB    *pgxpool.Pool
	Redis *goredis.Client
	Kafka *kafkago.Writer
}

// Inspect returns the final verdict. If something goes wrong it returns
// a FAIL verdict together with the error.
func (s *Service) Inspect(ctx context.Context, msgKey, content string) (model.Verdict, error) {
	// Whole inspection must finish within 5 seconds.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req := model.InspectRequest{
		InspectionID: model.NewID("INS"),
		MsgKey:       msgKey,
		TraceID:      model.NewID("TRACE"),
		Content:      content,
		Timestamp:    time.Now().UTC(),
	}

	// 1. Save to Postgres.
	if err := repo.SaveRequest(ctx, s.DB, req); err != nil {
		return fail(req, ErrSave), errors.Join(ErrSave, err)
	}

	// 2. Send to the engines through Kafka.
	if err := kafka.Publish(ctx, s.Kafka, req.InspectionID, req); err != nil {
		return s.saveFail(req, ErrSend), errors.Join(ErrSend, err)
	}

	// 3. Wait for Verdict to push the result to Redis.
	res, err := s.Redis.BLPop(ctx, 5*time.Second, model.VerdictKey(req.InspectionID)).Result()
	if err != nil {
		return s.saveFail(req, ErrTimeout), ErrTimeout
	}

	var v model.Verdict
	if err := json.Unmarshal([]byte(res[1]), &v); err != nil { // res = [key, value]
		return s.saveFail(req, err), err
	}
	return v, nil
}

func fail(req model.InspectRequest, reason error) model.Verdict {
	return model.Verdict{
		InspectionID: req.InspectionID,
		MsgKey:       req.MsgKey,
		TraceID:      req.TraceID,
		Verdict:      model.Fail,
		Reason:       reason.Error(),
		Details:      map[string]model.EngineResult{},
	}
}

// saveFail records the FAIL in Postgres too, so every inspection has a result.
func (s *Service) saveFail(req model.InspectRequest, reason error) model.Verdict {
	v := fail(req, reason)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := repo.SaveVerdict(ctx, s.DB, v); err != nil {
		log.Println("save fail verdict:", err)
	}
	return v
}
