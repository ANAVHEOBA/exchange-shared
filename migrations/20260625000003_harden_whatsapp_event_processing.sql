ALTER TABLE whatsapp_events
    MODIFY COLUMN processed TINYINT NOT NULL DEFAULT 0;

ALTER TABLE whatsapp_events
    ADD COLUMN attempt_count INT NOT NULL DEFAULT 0;

ALTER TABLE whatsapp_events
    ADD COLUMN processing_started_at TIMESTAMP NULL DEFAULT NULL;

ALTER TABLE whatsapp_events
    ADD COLUMN processed_at TIMESTAMP NULL DEFAULT NULL;

ALTER TABLE whatsapp_events
    ADD COLUMN last_error TEXT NULL;

CREATE INDEX idx_whatsapp_events_queue
    ON whatsapp_events (event_kind, processed, attempt_count, created_at);

CREATE INDEX idx_whatsapp_outbound_created
    ON whatsapp_outbound_messages (created_at);

CREATE INDEX idx_whatsapp_sessions_updated
    ON whatsapp_sessions (updated_at);
