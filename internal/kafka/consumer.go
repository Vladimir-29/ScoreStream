package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer потребляет события очков из Kafka в составе группы потребителей.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer создаёт потребителя Kafka, подписанного на topic в заданной
// группе потребителей. Автоматический коммит офсетов отключён; офсеты
// коммитятся явно через Commit.
func NewConsumer(brokers []string, topic, groupID string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// PollFetches ждёт и возвращает следующую пачку полученных записей.
// Блокируется до появления записей или завершения ctx.
func (c *Consumer) PollFetches(ctx context.Context) kgo.Fetches {
	return c.client.PollFetches(ctx)
}

// Commit помечает офсет записи как закоммиченный. Вызывать следует только
// после успешной обработки записи.
func (c *Consumer) Commit(ctx context.Context, rec *kgo.Record) error {
	if err := c.client.CommitRecords(ctx, rec); err != nil {
		return fmt.Errorf("commit offset for %s:%d@%d: %w",
			rec.Topic, rec.Partition, rec.Offset, err)
	}
	return nil
}

// Close покидает группу потребителей и завершает работу клиента.
func (c *Consumer) Close() {
	c.client.Close()
}
