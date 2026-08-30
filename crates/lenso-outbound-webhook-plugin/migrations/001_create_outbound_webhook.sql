CREATE TABLE webhook_deliveries (
    delivery_id text PRIMARY KEY,
    event_id text UNIQUE NOT NULL,
    event_type text NOT NULL,
    endpoint_url text NOT NULL,
    endpoint_origin text NOT NULL,
    payload_snapshot bytea NOT NULL,
    payload_sha256 text NOT NULL,
    queue_name text NOT NULL,
    status text NOT NULL,
    available_at timestamptz NOT NULL,
    lease_expires_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    replay_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT webhook_delivery_status_valid CHECK (
        status IN ('queued', 'delivering', 'retry_scheduled', 'delivered', 'dead_letter')
    ),
    CONSTRAINT webhook_delivery_attempts_valid CHECK (
        attempts BETWEEN 0 AND max_attempts AND max_attempts BETWEEN 1 AND 100
    ),
    CONSTRAINT webhook_delivery_replay_valid CHECK (replay_count BETWEEN 0 AND 1000000),
    CONSTRAINT webhook_delivery_lease_valid CHECK (
        (status = 'delivering') = (lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX webhook_deliveries_due_idx
    ON webhook_deliveries (queue_name, status, available_at, created_at, delivery_id);

CREATE TABLE webhook_delivery_attempts (
    delivery_id text NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
    replay_count integer NOT NULL,
    attempt integer NOT NULL,
    outcome text NOT NULL,
    http_status integer,
    response_sha256 text,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (delivery_id, replay_count, attempt),
    CONSTRAINT webhook_attempt_number_valid CHECK (attempt BETWEEN 1 AND 100),
    CONSTRAINT webhook_attempt_status_valid CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599)
);
