CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_weekly_rivalries (
    id BIGSERIAL PRIMARY KEY,
    season_year INTEGER,
    week_num INTEGER NOT NULL,
    stage INTEGER NOT NULL,
    game_id BIGINT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_user_id BIGINT,
    away_user_id BIGINT,
    priority_score NUMERIC NOT NULL DEFAULT 0,
    reason TEXT,
    banner_posted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (season_year, week_num, stage, game_id)
);

CREATE TABLE IF NOT EXISTS bot_game_recaps (
    id BIGSERIAL PRIMARY KEY,
    game_id BIGINT NOT NULL UNIQUE,
    season_index INTEGER,
    stage_index INTEGER NOT NULL,
    week INTEGER NOT NULL,
    away_team_name TEXT NOT NULL,
    home_team_name TEXT NOT NULL,
    headline TEXT NOT NULL,
    body TEXT NOT NULL,
    used_ai BOOLEAN NOT NULL DEFAULT FALSE,
    posted_channel_id BIGINT,
    posted_message_id BIGINT,
    created_by BIGINT,
    status TEXT NOT NULL DEFAULT 'posted',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_game_channels (
    id BIGSERIAL PRIMARY KEY,
    season_index INTEGER,
    stage_index INTEGER NOT NULL,
    week INTEGER NOT NULL,
    game_id BIGINT NOT NULL,
    channel_id BIGINT NOT NULL,
    channel_name TEXT NOT NULL,
    home_team_name TEXT NOT NULL,
    away_team_name TEXT NOT NULL,
    created_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (game_id, channel_id)
);
