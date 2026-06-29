ALTER TABLE whatsapp_sessions
    ADD COLUMN admin_status VARCHAR(32) NOT NULL DEFAULT 'open' AFTER state;

ALTER TABLE whatsapp_sessions
    ADD COLUMN admin_tag VARCHAR(64) NULL AFTER admin_status;

ALTER TABLE whatsapp_sessions
    ADD COLUMN assigned_to VARCHAR(128) NULL AFTER admin_tag;

ALTER TABLE whatsapp_sessions
    ADD COLUMN internal_note TEXT NULL AFTER assigned_to;

CREATE INDEX idx_whatsapp_sessions_admin_status
    ON whatsapp_sessions (admin_status, updated_at);

CREATE INDEX idx_whatsapp_sessions_admin_tag
    ON whatsapp_sessions (admin_tag, updated_at);
