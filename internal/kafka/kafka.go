// Package kafka connects to Kafka and sends/receives JSON messages.
package kafka

import (
	"context"
	"encoding/json"
	"os"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func broker() string {
	if b := os.Getenv("KAFKA_BROKER"); b != "" {
		return b
	}
	return "localhost:9092"
}

// CreateTopics makes sure the topics exist before anyone reads or writes.
// Without this, the first messages after a fresh start can be slow or lost.
func CreateTopics(topics ...string) error {
	conn, err := kafkago.Dial("tcp", broker())
	if err != nil {
		return err
	}
	defer conn.Close()

	var configs []kafkago.TopicConfig
	for _, t := range topics {
		configs = append(configs, kafkago.TopicConfig{Topic: t, NumPartitions: 1, ReplicationFactor: 1})
	}
	return conn.CreateTopics(configs...) // no error if they already exist
}

// NewWriter returns a producer for the given topic.
func NewWriter(topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(broker()),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond, // default 1s is too slow for our 5s budget
	}
}

// NewReader returns a consumer for the given topic and group.
func NewReader(topic, groupID string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{broker()},
		Topic:   topic,
		GroupID: groupID,
	})
}

// Publish sends v as JSON. On failure it retries once, then gives up.
func Publish(ctx context.Context, w *kafkago.Writer, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	msg := kafkago.Message{Key: []byte(key), Value: body}
	if err = w.WriteMessages(ctx, msg); err == nil {
		return nil
	}
	time.Sleep(100 * time.Millisecond)
	return w.WriteMessages(ctx, msg)
}
