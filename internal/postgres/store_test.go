package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Vladimir-29/ScoreStream/internal/score"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; skipping postgres integration test")
	}
	return url
}

// testPrefix возвращает уникальный префикс идентификаторов для теста,
// чтобы тесты не зависели друг от друга.
func testPrefix(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '-'
		}
	}, t.Name())
	return fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
}

// setupStore создаёт Store и «сырой» пул для проверочных запросов.
// После теста удаляет все строки с тестовым префиксом.
func setupStore(t *testing.T) (*Store, *pgxpool.Pool, string) {
	t.Helper()
	url := testDatabaseURL(t)
	ctx := context.Background()

	store, err := NewStore(ctx, url)
	if err != nil {
		t.Fatalf("NewStore() = %v", err)
	}
	t.Cleanup(store.Close)

	raw, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("create raw pool: %v", err)
	}
	t.Cleanup(raw.Close)

	prefix := testPrefix(t)
	t.Cleanup(func() {
		_, _ = raw.Exec(context.Background(),
			`DELETE FROM leaderboard_scores WHERE leaderboard_id LIKE $1`,
			prefix+"%")
		_, _ = raw.Exec(context.Background(),
			`DELETE FROM score_events WHERE leaderboard_id LIKE $1`,
			prefix+"%")
	})
	return store, raw, prefix
}

func countRows(t *testing.T, raw *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := raw.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}

func TestApplyEvent_NewEvent(t *testing.T) {
	store, raw, prefix := setupStore(t)
	event := score.Event{
		EventID:       prefix + "-evt-1",
		LeaderboardID: prefix + "-lb",
		UserID:        prefix + "-user-1",
		Points:        100,
	}

	total, err := store.ApplyEvent(context.Background(), event)
	if err != nil {
		t.Fatalf("ApplyEvent() = %v", err)
	}
	if total != 100 {
		t.Fatalf("total = %d, want 100", total)
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM score_events WHERE event_id = $1`,
		event.EventID); n != 1 {
		t.Fatalf("score_events rows = %d, want 1", n)
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM leaderboard_scores
		 WHERE leaderboard_id = $1 AND user_id = $2 AND total_score = 100`,
		event.LeaderboardID, event.UserID); n != 1 {
		t.Fatalf("leaderboard_scores rows = %d, want 1", n)
	}
}

func TestApplyEvent_TwoEventsSum(t *testing.T) {
	store, _, prefix := setupStore(t)
	ctx := context.Background()
	lb, user := prefix+"-lb", prefix+"-user-1"

	for i, points := range []int64{100, 50} {
		total, err := store.ApplyEvent(ctx, score.Event{
			EventID:       fmt.Sprintf("%s-evt-%d", prefix, i),
			LeaderboardID: lb,
			UserID:        user,
			Points:        points,
		})
		if err != nil {
			t.Fatalf("ApplyEvent(%d) = %v", i, err)
		}
		if want := []int64{100, 150}[i]; total != want {
			t.Fatalf("ApplyEvent(%d) total = %d, want %d", i, total, want)
		}
	}
}

func TestApplyEvent_DuplicateDoesNotDoubleCount(t *testing.T) {
	store, raw, prefix := setupStore(t)
	ctx := context.Background()
	event := score.Event{
		EventID:       prefix + "-evt-dup",
		LeaderboardID: prefix + "-lb",
		UserID:        prefix + "-user-1",
		Points:        100,
	}

	if _, err := store.ApplyEvent(ctx, event); err != nil {
		t.Fatalf("first ApplyEvent() = %v", err)
	}
	total, err := store.ApplyEvent(ctx, event)
	if err != nil {
		t.Fatalf("second ApplyEvent() = %v", err)
	}
	if total != 100 {
		t.Fatalf("total after duplicate = %d, want 100", total)
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM score_events WHERE event_id = $1`,
		event.EventID); n != 1 {
		t.Fatalf("score_events rows = %d, want 1", n)
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM leaderboard_scores
		 WHERE leaderboard_id = $1 AND user_id = $2 AND total_score = 100`,
		event.LeaderboardID, event.UserID); n != 1 {
		t.Fatalf("leaderboard_scores rows = %d, want 1", n)
	}
}

func TestApplyEvent_EventIDConflict(t *testing.T) {
	store, raw, prefix := setupStore(t)
	ctx := context.Background()
	base := score.Event{
		EventID:       prefix + "-evt-conflict",
		LeaderboardID: prefix + "-lb",
		UserID:        prefix + "-user-1",
		Points:        100,
	}
	if _, err := store.ApplyEvent(ctx, base); err != nil {
		t.Fatalf("first ApplyEvent() = %v", err)
	}

	variants := map[string]score.Event{
		"different points":      {EventID: base.EventID, LeaderboardID: base.LeaderboardID, UserID: base.UserID, Points: 200},
		"different user_id":     {EventID: base.EventID, LeaderboardID: base.LeaderboardID, UserID: prefix + "-user-2", Points: 100},
		"different leaderboard": {EventID: base.EventID, LeaderboardID: prefix + "-lb2", UserID: base.UserID, Points: 100},
	}
	for name, ev := range variants {
		_, err := store.ApplyEvent(ctx, ev)
		if !errors.Is(err, ErrEventIDConflict) {
			t.Fatalf("%s: ApplyEvent() error = %v, want ErrEventIDConflict", name, err)
		}
	}
	// Данные не должны измениться: один event, счёт = 100.
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM score_events WHERE event_id = $1`,
		base.EventID); n != 1 {
		t.Fatalf("score_events rows = %d, want 1", n)
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM leaderboard_scores
		 WHERE leaderboard_id = $1 AND user_id = $2 AND total_score = 100`,
		base.LeaderboardID, base.UserID); n != 1 {
		t.Fatalf("leaderboard_scores rows = %d, want 1", n)
	}
}

func TestApplyEvent_LeaderboardsAreIndependent(t *testing.T) {
	store, _, prefix := setupStore(t)
	ctx := context.Background()
	user := prefix + "-user-1"

	total1, err := store.ApplyEvent(ctx, score.Event{
		EventID: prefix + "-evt-a", LeaderboardID: prefix + "-lb-a",
		UserID: user, Points: 100,
	})
	if err != nil {
		t.Fatalf("ApplyEvent(lb-a) = %v", err)
	}
	total2, err := store.ApplyEvent(ctx, score.Event{
		EventID: prefix + "-evt-b", LeaderboardID: prefix + "-lb-b",
		UserID: user, Points: 30,
	})
	if err != nil {
		t.Fatalf("ApplyEvent(lb-b) = %v", err)
	}
	if total1 != 100 || total2 != 30 {
		t.Fatalf("totals = %d, %d; want 100, 30", total1, total2)
	}
}

func TestApplyEvent_FailedTransactionLeavesNoPartialData(t *testing.T) {
	store, raw, prefix := setupStore(t)
	ctx := context.Background()

	// points = -5 нарушает CHECK (points > 0): вставка события падает,
	// транзакция откатывается, leaderboard_scores не трогается.
	bad := score.Event{
		EventID:       prefix + "-evt-bad",
		LeaderboardID: prefix + "-lb",
		UserID:        prefix + "-user-1",
		Points:        -5,
	}
	if _, err := store.ApplyEvent(ctx, bad); err == nil {
		t.Fatal("ApplyEvent() with negative points = nil error, want error")
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM score_events WHERE event_id = $1`,
		bad.EventID); n != 0 {
		t.Fatalf("score_events rows = %d, want 0", n)
	}
	if n := countRows(t, raw,
		`SELECT COUNT(*) FROM leaderboard_scores WHERE leaderboard_id = $1`,
		bad.LeaderboardID); n != 0 {
		t.Fatalf("leaderboard_scores rows = %d, want 0", n)
	}
}
