-- Refunds table
CREATE TABLE IF NOT EXISTS refunds (
    id VARCHAR(36) PRIMARY KEY,
    swap_id VARCHAR(36) NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL UNIQUE,
    
    -- Refund details
    refund_address VARCHAR(255) NOT NULL,
    refund_amount DECIMAL(20, 8) NOT NULL,
    refund_currency VARCHAR(10) NOT NULL,
    refund_network VARCHAR(50) NOT NULL,
    
    -- Transaction details
    tx_hash VARCHAR(255),
    tx_status ENUM('PENDING', 'SUBMITTED', 'CONFIRMED', 'FAILED') DEFAULT 'PENDING',
    confirmations INT DEFAULT 0,
    required_confirmations INT DEFAULT 6,
    
    -- Retry tracking
    attempt_number INT DEFAULT 0,
    max_attempts INT DEFAULT 5,
    next_retry_at TIMESTAMP NULL,
    last_error TEXT,
    
    -- Gas/fee tracking
    gas_price DECIMAL(20, 8),
    gas_used BIGINT,
    total_fee DECIMAL(20, 8),
    
    -- Priority and status
    priority_score DECIMAL(10, 2),
    status ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'MANUAL') DEFAULT 'PENDING',
    
    -- Audit trail
    initiated_by VARCHAR(50) DEFAULT 'SYSTEM',
    initiated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    
    -- Metadata
    failure_reason TEXT,
    metadata JSON,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_swap_id (swap_id),
    INDEX idx_status_priority (status, priority_score DESC),
    INDEX idx_next_retry (next_retry_at),
    INDEX idx_tx_hash (tx_hash),
    INDEX idx_idempotency (idempotency_key)
);

-- Refund history (audit log)
CREATE TABLE IF NOT EXISTS refund_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    refund_id VARCHAR(36) NOT NULL,
    
    -- State change
    from_status VARCHAR(20),
    to_status VARCHAR(20) NOT NULL,
    
    -- Transaction details
    tx_hash VARCHAR(255),
    gas_price DECIMAL(20, 8),
    error_message TEXT,
    
    -- Context
    triggered_by VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_refund_id (refund_id),
    INDEX idx_timestamp (timestamp)
);
