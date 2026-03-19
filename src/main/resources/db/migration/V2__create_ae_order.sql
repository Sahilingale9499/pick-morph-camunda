-- Flyway V2: create ae_order table for persisting AE pick-list order payloads
-- The payload column stores the full AePickListRequest JSON.
-- pick_id = AE externalServiceRequestId = pick_instruction_id from butler_server.
CREATE TABLE IF NOT EXISTS ae_order (
    pick_id    VARCHAR(255) NOT NULL PRIMARY KEY,
    payload    TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
