// Package postgres хранит события очков и текущие результаты рейтингов.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Vladimir-29/ScoreStream/internal/score"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrEventIDConflict возвращается, когда существующий event_id используется
// с другим содержимым события.
var ErrEventIDConflict = errors.New("event_id already exists with different content")

// Store хранит connection pool PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore создаёт пул подключений к базе по connString. Проверяет
// доступность PostgreSQL и возвращает ошибку, если соединение не удалось.
func NewStore(ctx context.Context, connString string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// ApplyEvent атомарно записывает событие и начисляет очки. Возвращает
// актуальный total_score участника после обработки.
//
// Повторная доставка того же события (идентичное содержимое) считается
// безопасным дубликатом и не увеличивает счёт повторно. Тот же event_id
// с другим содержимым возвращает ErrEventIDConflict.
func (s *Store) ApplyEvent(ctx context.Context, event score.Event) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var stored score.Event
	err = tx.QueryRow(ctx,
		`INSERT INTO score_events (event_id, leaderboard_id, user_id, points)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id, leaderboard_id, user_id, points`,
		event.EventID, event.LeaderboardID, event.UserID, event.Points,
	).Scan(&stored.EventID, &stored.LeaderboardID, &stored.UserID, &stored.Points)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("insert score event: %w", err)
	}

	var totalScore int64
	if errors.Is(err, pgx.ErrNoRows) {
		// Событие уже существует: проверяем, что содержимое совпадает.
		if err := tx.QueryRow(ctx,
			`SELECT leaderboard_id, user_id, points
			 FROM score_events WHERE event_id = $1`,
			event.EventID,
		).Scan(&stored.LeaderboardID, &stored.UserID, &stored.Points); err != nil {
			return 0, fmt.Errorf("load existing event: %w", err)
		}
		stored.EventID = event.EventID
		if stored != event {
			return 0, ErrEventIDConflict
		}
		// Дубликат: счёт не меняется, читаем актуальный total_score.
		if err := tx.QueryRow(ctx,
			`SELECT total_score FROM leaderboard_scores
			 WHERE leaderboard_id = $1 AND user_id = $2`,
			event.LeaderboardID, event.UserID,
		).Scan(&totalScore); err != nil {
			return 0, fmt.Errorf("load leaderboard score: %w", err)
		}
	} else {
		// Новое событие: увеличиваем счёт участника.
		if err := tx.QueryRow(ctx,
			`INSERT INTO leaderboard_scores (leaderboard_id, user_id, total_score)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (leaderboard_id, user_id)
			 DO UPDATE SET
			     total_score = leaderboard_scores.total_score + EXCLUDED.total_score,
			     updated_at  = NOW()
			 RETURNING total_score`,
			event.LeaderboardID, event.UserID, event.Points,
		).Scan(&totalScore); err != nil {
			return 0, fmt.Errorf("upsert leaderboard score: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}
	return totalScore, nil
}

// Close закрывает пул соединений.
func (s *Store) Close() {
	s.pool.Close()
}
