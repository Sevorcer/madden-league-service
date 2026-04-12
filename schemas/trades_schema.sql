CREATE TABLE IF NOT EXISTS bot_trades (
    id BIGSERIAL PRIMARY KEY,
    submitted_by BIGINT NOT NULL,
    submitted_username TEXT NOT NULL,
    coach_one_user_id BIGINT,
    coach_two_user_id BIGINT,
    team_one_name TEXT NOT NULL,
    team_two_name TEXT NOT NULL,
    team_one_gets TEXT NOT NULL,
    team_two_gets TEXT NOT NULL,
    notes TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    approve_count INTEGER NOT NULL DEFAULT 0,
    deny_count INTEGER NOT NULL DEFAULT 0,
    review_channel_id BIGINT,
    review_message_id BIGINT,
    announcement_channel_id BIGINT,
    announcement_message_id BIGINT,
    finalized_by BIGINT,
    finalized_reason TEXT,
    finalized_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_trade_votes (
    id BIGSERIAL PRIMARY KEY,
    trade_id BIGINT NOT NULL REFERENCES bot_trades(id) ON DELETE CASCADE,
    voter_user_id BIGINT NOT NULL,
    voter_username TEXT NOT NULL,
    vote TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (trade_id, voter_user_id)
);
