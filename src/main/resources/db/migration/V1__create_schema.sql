CREATE TABLE transaction_status (
    transaction_id      VARCHAR(255) NOT NULL PRIMARY KEY,
    pick_instruction_id VARCHAR(255),
    status              VARCHAR(50)  NOT NULL,
    payload             JSONB,
    last_updated        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX idx_transaction_status_pick_instruction_id
    ON transaction_status(pick_instruction_id);

CREATE TABLE ae_order (
    id                          BIGSERIAL    PRIMARY KEY,
    external_service_request_id TEXT         NOT NULL UNIQUE,
    type                        TEXT         NOT NULL,
    fulfillment_area            JSONB,
    attributes                  JSONB,
    expectations                JSONB,
    status                      TEXT         NOT NULL DEFAULT 'CREATED',
    actuals                     JSONB        NOT NULL DEFAULT '{}',
    state                       TEXT         NOT NULL DEFAULT 'CREATED',
    sub_state                   TEXT         NOT NULL DEFAULT 'CREATED',
    is_deleted                  BOOLEAN      NOT NULL DEFAULT FALSE,
    stages                      JSONB        NOT NULL DEFAULT '[]',
    on_hold                     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ae_order_state ON ae_order(state);
CREATE INDEX idx_ae_order_type  ON ae_order(type);

CREATE TABLE order_mapping (
    id                                  BIGSERIAL    PRIMARY KEY,
    parent_external_service_request_id  TEXT         NOT NULL REFERENCES ae_order(external_service_request_id),
    child_external_service_request_id   TEXT         NOT NULL REFERENCES ae_order(external_service_request_id),
    created_at                          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at                          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (parent_external_service_request_id, child_external_service_request_id)
);

CREATE INDEX idx_order_mapping_parent ON order_mapping(parent_external_service_request_id);
CREATE INDEX idx_order_mapping_child  ON order_mapping(child_external_service_request_id);

CREATE TABLE outbox (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        VARCHAR(255) NOT NULL,
    message_key  VARCHAR(255),
    payload      JSONB        NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published = FALSE;
