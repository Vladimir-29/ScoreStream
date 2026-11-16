-- Применение:
--   docker exec -i scorestream-postgres psql -U scorestream -d scorestream \
--     < migrations/001_create_score_tables.sql

CREATE TABLE IF NOT EXISTS score_events (
    event_id       TEXT PRIMARY KEY,
    leaderboard_id TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    points         BIGINT NOT NULL CHECK (points > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS leaderboard_scores (
    leaderboard_id TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    total_score    BIGINT NOT NULL CHECK (total_score >= 0),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (leaderboard_id, user_id)
);
