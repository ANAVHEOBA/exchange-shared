CREATE TABLE whatsapp_sessions (
    id CHAR(36) PRIMARY KEY,
    wa_id VARCHAR(64) NOT NULL,
    phone_number_id VARCHAR(64) NOT NULL,
    locale VARCHAR(16) NOT NULL DEFAULT 'en',
    state VARCHAR(64) NOT NULL DEFAULT 'idle',
    draft JSON NULL,
    last_inbound_message_id VARCHAR(255) NULL,
    last_outbound_message_id VARCHAR(255) NULL,
    last_inbound_at TIMESTAMP NULL,
    last_outbound_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_whatsapp_sessions_wa_phone (wa_id, phone_number_id),
    INDEX idx_whatsapp_sessions_state (state),
    INDEX idx_whatsapp_sessions_updated_at (updated_at)
);

CREATE TABLE whatsapp_events (
    id CHAR(36) PRIMARY KEY,
    dedupe_key CHAR(64) NOT NULL,
    phone_number_id VARCHAR(64) NOT NULL,
    wa_id VARCHAR(64) NULL,
    provider_message_id VARCHAR(255) NULL,
    event_kind VARCHAR(32) NOT NULL,
    message_type VARCHAR(64) NULL,
    event_timestamp VARCHAR(32) NULL,
    text_preview TEXT NULL,
    payload JSON NOT NULL,
    processed TINYINT(1) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_whatsapp_events_dedupe (dedupe_key),
    INDEX idx_whatsapp_events_wa_created (wa_id, created_at),
    INDEX idx_whatsapp_events_provider_message (provider_message_id),
    INDEX idx_whatsapp_events_processed (processed, created_at)
);
