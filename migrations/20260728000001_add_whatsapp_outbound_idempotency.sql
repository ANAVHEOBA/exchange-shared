ALTER TABLE whatsapp_outbound_messages
    ADD COLUMN idempotency_key CHAR(64) NULL;

CREATE UNIQUE INDEX uq_whatsapp_outbound_idempotency
    ON whatsapp_outbound_messages (idempotency_key);
