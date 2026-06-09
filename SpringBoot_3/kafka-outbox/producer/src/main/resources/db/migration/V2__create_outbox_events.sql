CREATE TABLE outbox_events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic       VARCHAR(255)  NOT NULL,
    payload     TEXT          NOT NULL,
    status      VARCHAR(20)   NOT NULL DEFAULT 'NEW',
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ,
    retry_count INT           NOT NULL DEFAULT 0
);

CREATE INDEX idx_outbox_status ON outbox_events (status) WHERE status = 'NEW';
