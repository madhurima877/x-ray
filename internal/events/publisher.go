// Package events publishes one result event per message (step 12). Publish
// never blocks the worker: the Kafka writer batches in the background.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"xray/internal/model"
)

type Publisher interface {
	Publish(e model.Event)
	Close() error
}

type Kafka struct {
	w   *kafka.Writer
	log *slog.Logger
}

func NewKafka(brokers []string, topic string, log *slog.Logger) *Kafka {
	k := &Kafka{log: log}
	k.w = &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // same customer → same partition
		Async:                  true,
		BatchSize:              500,
		BatchTimeout:           50 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		Completion: func(msgs []kafka.Message, err error) {
			if err != nil {
				log.Warn("kafka batch failed", "component", "events", "messages", len(msgs), "err", err)
			}
		},
	}
	return k
}

func (k *Kafka) Publish(e model.Event) {
	v := e.Verdict
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	// With Async set, WriteMessages only queues the message and returns.
	if err := k.w.WriteMessages(context.Background(), kafka.Message{Key: []byte(v.CustomerID), Value: b}); err != nil {
		k.log.Warn("kafka publish failed", "component", "events", "message_id", v.MessageID, "err", err)
	}
}

// Close flushes pending batches.
func (k *Kafka) Close() error { return k.w.Close() }

// Log writes events to the log instead; used when no brokers are configured.
type Log struct{ log *slog.Logger }

func NewLog(log *slog.Logger) *Log { return &Log{log: log} }

func (l *Log) Publish(e model.Event) {
	v := e.Verdict
	l.log.Info("verdict", "component", "events", "message_id", v.MessageID, "customer_id", v.CustomerID,
		"verdict", v.Action, "latency_ms", v.LatencyMS)
}

func (l *Log) Close() error { return nil }
