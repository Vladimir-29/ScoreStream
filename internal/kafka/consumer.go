package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer consumes score events from Kafka as part of a consumer group.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer creates a Kafka consumer subscribed to topic within the
// given consumer group. Automatic offset commits are disabled; offsets are
// committed explicitly via Commit.
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

// PollFetches waits for and returns the next batch of fetched records.
// It blocks until records are available or ctx is done.
func (c *Consumer) PollFetches(ctx context.Context) kgo.Fetches {
	return c.client.PollFetches(ctx)
}

// Commit marks the record's offset as committed. Only call this after the
// record has been processed successfully.
func (c *Consumer) Commit(ctx context.Context, rec *kgo.Record) error {
	if err := c.client.CommitRecords(ctx, rec); err != nil {
		return fmt.Errorf("commit offset for %s:%d@%d: %w",
			rec.Topic, rec.Partition, rec.Offset, err)
	}
	return nil
}

// Close leaves the consumer group and shuts down the client.
func (c *Consumer) Close() {
	c.client.Close()
}
