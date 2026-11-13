// Package score defines the score event shared by the API and the worker.
package score

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Event is the payload published to the score-events Kafka topic.
type Event struct {
	EventID       string `json:"event_id"`
	LeaderboardID string `json:"leaderboard_id"`
	UserID        string `json:"user_id"`
	Points        int64  `json:"points"`
}

// Key returns the Kafka message key used to partition events for a
// leaderboard/user pair to the same partition.
func (e Event) Key() string {
	return e.LeaderboardID + ":" + e.UserID
}

// Validate checks that all required fields are present and points is positive.
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

// Marshal serializes the event as JSON.
func (e Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// Unmarshal parses a JSON-encoded score event.
func Unmarshal(data []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, err
	}
	return e, nil
}
