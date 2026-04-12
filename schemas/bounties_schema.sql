CREATE TABLE IF NOT EXISTS bot_bounties (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    reward DOUBLE PRECISION NOT NULL,
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    claimed_by BIGINT,
    claimed_at TIMESTAMPTZ
);
