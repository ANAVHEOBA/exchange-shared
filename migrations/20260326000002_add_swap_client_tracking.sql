ALTER TABLE swaps
    ADD COLUMN client_id VARCHAR(36) NULL AFTER user_id;

CREATE INDEX idx_swaps_client_history
    ON swaps (client_id, created_at DESC, id DESC);
