// Command worker потребляет события очков из Kafka, сохраняет их в
// PostgreSQL и подтверждает обработанные offset-ы.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Vladimir-29/ScoreStream/internal/config"
	"github.com/Vladimir-29/ScoreStream/internal/kafka"
	"github.com/Vladimir-29/ScoreStream/internal/postgres"
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

	store, err := postgres.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()

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
			if err := processRecord(ctx, consumer, store, logger, rec); err != nil {
				return fmt.Errorf("process record at %s:%d@%d: %w",
					rec.Topic, rec.Partition, rec.Offset, err)
			}
		}
	}
}

func processRecord(ctx context.Context, consumer *kafka.Consumer,
	store *postgres.Store, logger *slog.Logger, rec *kgo.Record) error {

	event, err := decodeEvent(rec.Value)
	if err != nil {
		// Некорректное сообщение: логируем и подтверждаем offset,
		// чтобы оно не блокировало partition.
		logger.Error("skip invalid score event",
			"topic", rec.Topic, "partition", rec.Partition,
			"offset", rec.Offset, "error", err)
		return consumer.Commit(ctx, rec)
	}

	totalScore, err := store.ApplyEvent(ctx, event)
	if err != nil {
		if errors.Is(err, postgres.ErrEventIDConflict) {
			// Конфликт event_id: данные не изменяются, offset подтверждаем.
			logger.Error("event_id conflict, skipping record",
				"event_id", event.EventID,
				"leaderboard_id", event.LeaderboardID,
				"user_id", event.UserID,
				"topic", rec.Topic, "partition", rec.Partition,
				"offset", rec.Offset, "error", err)
			return consumer.Commit(ctx, rec)
		}
		// Инфраструктурная ошибка: offset не подтверждаем, worker завершается.
		return fmt.Errorf("apply event %s: %w", event.EventID, err)
	}

	logger.Info("processed score event",
		"event_id", event.EventID,
		"leaderboard_id", event.LeaderboardID,
		"user_id", event.UserID,
		"points", event.Points,
		"total_score", totalScore,
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
