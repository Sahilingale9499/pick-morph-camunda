DROP TABLE IF EXISTS outbox;

CREATE TABLE outbox (
    id              UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           VARCHAR(255)    NOT NULL,
    message_key     VARCHAR(255),
    payload         JSONB           NOT NULL,
    published       BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP,
    published_at    TIMESTAMP
);

CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published = FALSE;
