ALTER TABLE email_verifications
    ADD COLUMN used_at TIMESTAMP NULL;

CREATE INDEX idx_email_verifications_used_at
    ON email_verifications (used_at);
