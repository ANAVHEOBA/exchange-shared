CREATE TABLE whatsapp_outbound_messages (
    id CHAR(36) PRIMARY KEY,
    session_id CHAR(36) NULL,
    wa_id VARCHAR(64) NOT NULL,
    phone_number_id VARCHAR(64) NOT NULL,
    provider_message_id VARCHAR(255) NULL,
    message_kind VARCHAR(32) NOT NULL,
    body TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    error_message TEXT NULL,
    sent_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_whatsapp_outbound_provider_message (provider_message_id),
    INDEX idx_whatsapp_outbound_session_created (session_id, created_at),
    INDEX idx_whatsapp_outbound_wa_created (wa_id, created_at)
);
