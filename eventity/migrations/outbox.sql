CREATE TABLE IF NOT EXISTS outbox
(
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       uuid        NOT NULL UNIQUE,
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    event_type     text        NOT NULL,
    exchange       text        NOT NULL,
    payload        jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    processed_at   timestamptz,
    attempt_count  integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error     text
);

CREATE INDEX IF NOT EXISTS outbox_retry_pending_idx
    ON outbox (next_attempt_at, id)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_aggregate_order_idx
    ON outbox (aggregate_type, aggregate_id, id)
    WHERE processed_at IS NULL;
