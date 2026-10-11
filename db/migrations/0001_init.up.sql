CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(id),
    destination_url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'succeeded', 'dead')),
    attempts_count INTEGER NOT NULL DEFAULT 0
        CHECK (attempts_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_deliveries_pending
    ON deliveries (next_attempt_at, created_at)
    WHERE status = 'pending';

CREATE INDEX idx_deliveries_expired
    ON deliveries (locked_until)
    WHERE status = 'processing';

CREATE INDEX idx_deliveries_event_id
    ON deliveries (event_id);

CREATE TABLE delivery_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    http_status INTEGER,
    error TEXT,
    duration_ms BIGINT,
    UNIQUE (delivery_id, attempt_number)
);
