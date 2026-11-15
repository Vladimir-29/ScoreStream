// Package score определяет событие очков, общее для API и worker.
package score

import (
	"errors"
	"fmt"
)

// Event — полезная нагрузка, публикуемая в топик Kafka score-events.
type Event struct {
	EventID       string `json:"event_id"`
	LeaderboardID string `json:"leaderboard_id"`
	UserID        string `json:"user_id"`
	Points        int64  `json:"points"`
}

// Key возвращает ключ сообщения Kafka, по которому события пары
// «лидерборд/пользователь» попадают в один раздел.
func (e Event) Key() string {
	return e.LeaderboardID + ":" + e.UserID
}

// Validate проверяет, что все обязательные поля заполнены, а points положителен.
func (e Event) Validate() error {
	if e.EventID == "" {
		return errors.New("event_id is required")
	}
	if e.LeaderboardID == "" {
		return errors.New("leaderboard_id is required")
	}
	if e.UserID == "" {
		return errors.New("user_id is required")
	}
	if e.Points <= 0 {
		return fmt.Errorf("points must be a positive integer, got %d", e.Points)
	}
	return nil
}
