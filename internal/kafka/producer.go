// Package kafka contains the franz-go based producer and consumer.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes score events to Kafka.
type Producer struct {
	client *kgo.Client
	topic  string
}

// NewProducer creates a Kafka producer for the given brokers and topic.
// Publication requires acknowledgement from all in-sync replicas and uses
// producer idempotence.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20), // 1 MiB
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client, topic: topic}, nil
}

// Publish sends key/value to the topic and waits for the broker
// acknowledgement. It returns an error if publication fails or the
// context is cancelled.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	results := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: p.topic,
		Key:   []byte(key),
		Value: value,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish to %s: %w", p.topic, err)
	}
	return nil
}

// Close shuts down the producer and flushes pending records.
func (p *Producer) Close() {
	p.client.Close()
}
