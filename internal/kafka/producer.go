// Package kafka содержит продюсер и потребителя на основе franz-go.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer публикует события очков в Kafka.
type Producer struct {
	client *kgo.Client
	topic  string
}

// NewProducer создаёт продюсер Kafka для заданных брокеров и топика.
// Публикация требует подтверждения от всех синхронных реплик и использует
// идемпотентность продюсера.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20), // 1 МиБ
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client, topic: topic}, nil
}

// Publish отправляет key/value в топик и ждёт подтверждения от брокера.
// Возвращает ошибку, если публикация не удалась или контекст отменён.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	results := p.client.ProduceSync(ctx, &kgo.Record{
		Key:   []byte(key),
		Value: value,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish to %s: %w", p.topic, err)
	}
	return nil
}

// Close завершает работу продюсера и сбрасывает ожидающие записи.
func (p *Producer) Close() {
	p.client.Close()
}
