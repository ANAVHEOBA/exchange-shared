-- Webhook registrations
CREATE TABLE IF NOT EXISTS webhooks (
    id VARCHAR(36) PRIMARY KEY,
    swap_id VARCHAR(36) NOT NULL,
    url TEXT NOT NULL,
    secret_key VARCHAR(64) NOT NULL,
    events JSON NOT NULL,
    enabled BOOLEAN DEFAULT true,
    rate_limit_per_second INT DEFAULT 10,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_webhooks_swap_id (swap_id),
    INDEX idx_webhooks_enabled (enabled)
);

-- Webhook delivery attempts
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id VARCHAR(36) PRIMARY KEY,
    webhook_id VARCHAR(36) NOT NULL,
    swap_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    idempotency_key VARCHAR(64) UNIQUE NOT NULL,
    
    -- Request details
    payload JSON NOT NULL,
    signature VARCHAR(128) NOT NULL,
    
    -- Delivery tracking
    attempt_number INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 10,
    next_retry_at TIMESTAMP NULL,
    
    -- Response tracking
    delivered_at TIMESTAMP NULL,
    response_status INT NULL,
    response_body TEXT NULL,
    response_time_ms INT NULL,
    
    -- Error tracking
    error_message TEXT NULL,
    is_dlq BOOLEAN DEFAULT false,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE,
    INDEX idx_webhook_deliveries_webhook_id (webhook_id),
    INDEX idx_webhook_deliveries_swap_id (swap_id),
    INDEX idx_webhook_deliveries_idempotency (idempotency_key),
    INDEX idx_webhook_deliveries_next_retry (next_retry_at),
    INDEX idx_webhook_deliveries_dlq (is_dlq)
);

-- Circuit breaker state
CREATE TABLE IF NOT EXISTS webhook_circuit_breakers (
    webhook_id VARCHAR(36) PRIMARY KEY,
    state VARCHAR(20) NOT NULL DEFAULT 'closed',
    failure_count INT NOT NULL DEFAULT 0,
    success_count INT NOT NULL DEFAULT 0,
    total_requests INT NOT NULL DEFAULT 0,
    opened_at TIMESTAMP NULL,
    half_open_attempts INT NOT NULL DEFAULT 0,
    timeout_seconds INT NOT NULL DEFAULT 3600,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE
);

-- Rate limiter state
CREATE TABLE IF NOT EXISTS webhook_rate_limiters (
    webhook_id VARCHAR(36) PRIMARY KEY,
    tokens_available FLOAT NOT NULL DEFAULT 100.0,
    capacity FLOAT NOT NULL DEFAULT 100.0,
    refill_rate FLOAT NOT NULL DEFAULT 10.0,
    last_refill_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (webhook_id) REFERENCES webhooks(id) ON DELETE CASCADE
);
