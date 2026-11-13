// Command worker consumes score events from Kafka and logs them.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Vladimir-29/ScoreStream/internal/config"
	"github.com/Vladimir-29/ScoreStream/internal/kafka"
	"github.com/Vladimir-29/ScoreStream/internal/score"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	if err != nil {
		return err
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("worker started",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", cfg.KafkaGroupID)

	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, ferr := range errs {
				logger.Error("kafka fetch error",
					"topic", ferr.Topic, "partition", ferr.Partition, "error", ferr.Err)
			}
		}

		for _, rec := range fetches.Records() {
			if err := processRecord(ctx, consumer, logger, rec); err != nil {
				return fmt.Errorf("process record at %s:%d@%d: %w",
					rec.Topic, rec.Partition, rec.Offset, err)
			}
		}
	}
}

func processRecord(ctx context.Context, consumer *kafka.Consumer,
	logger *slog.Logger, rec *kgo.Record) error {

	event, err := decodeEvent(rec.Value)
	if err != nil {
		return err
	}
	logger.Info("received score event",
		"event_id", event.EventID,
		"leaderboard_id", event.LeaderboardID,
		"user_id", event.UserID,
		"points", event.Points,
		"partition", rec.Partition,
		"offset", rec.Offset)
	return consumer.Commit(ctx, rec)
}

func decodeEvent(data []byte) (score.Event, error) {
	var event score.Event
	if err := json.Unmarshal(data, &event); err != nil {
		return score.Event{}, fmt.Errorf("decode event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return score.Event{}, fmt.Errorf("invalid event: %w", err)
	}
	return event, nil
}
