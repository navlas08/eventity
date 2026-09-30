CREATE TABLE IF NOT EXISTS inbox
(
    id           uuid        NOT NULL,
    consumer     text        NOT NULL,
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, id)
);

CREATE INDEX IF NOT EXISTS inbox_processed_at_idx ON inbox (processed_at);
