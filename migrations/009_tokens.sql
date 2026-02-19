-- =============================================================================
-- TOKENS TABLE
-- Enhanced token registry with ERC-20 support
-- =============================================================================

CREATE TABLE IF NOT EXISTS tokens (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    network VARCHAR(50) NOT NULL,
    contract_address VARCHAR(42),  -- 0x + 40 hex chars for EVM, NULL for native
    decimals INT NOT NULL,
    token_type ENUM('NATIVE', 'ERC20', 'BEP20', 'TRC20', 'SPL') NOT NULL DEFAULT 'ERC20',
    
    -- Metadata
    logo_url VARCHAR(255),
    coingecko_id VARCHAR(100),
    coinmarketcap_id VARCHAR(100),
    
    -- Status
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Limits
    min_swap_amount DECIMAL(20, 8),
    max_swap_amount DECIMAL(20, 8),
    
    -- Gas configuration
    gas_multiplier DECIMAL(5, 2) DEFAULT 3.0,
    
    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_token_contract_network (contract_address, network),
    INDEX idx_tokens_symbol (symbol),
    INDEX idx_tokens_network (network),
    INDEX idx_tokens_is_active (is_active),
    INDEX idx_tokens_token_type (token_type),
    INDEX idx_tokens_symbol_network (symbol, network)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TOKEN APPROVALS TABLE
-- Track ERC-20 approvals for optimization
-- =============================================================================

CREATE TABLE IF NOT EXISTS token_approvals (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_address VARCHAR(42) NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    spender_address VARCHAR(42) NOT NULL,
    network VARCHAR(50) NOT NULL,
    
    -- Approval details
    approved_amount DECIMAL(30, 0) NOT NULL,  -- Store as base units (integer)
    remaining_amount DECIMAL(30, 0) NOT NULL,
    
    -- Transaction details
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    
    -- Status
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Timestamps
    approved_at TIMESTAMP NOT NULL,
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP,  -- Optional expiry
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_approval (user_address, token_address, spender_address, network),
    INDEX idx_approvals_user (user_address),
    INDEX idx_approvals_token (token_address),
    INDEX idx_approvals_active (is_active),
    INDEX idx_approvals_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- TOKEN TRANSFERS TABLE
-- Track token transfer history
-- =============================================================================

CREATE TABLE IF NOT EXISTS token_transfers (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    swap_id VARCHAR(36),
    token_address VARCHAR(42) NOT NULL,
    network VARCHAR(50) NOT NULL,
    
    -- Transfer details
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    amount DECIMAL(30, 0) NOT NULL,  -- Base units
    decimals INT NOT NULL,
    
    -- Transaction details
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT,
    gas_used BIGINT,
    gas_price DECIMAL(20, 8),
    
    -- Status
    status ENUM('PENDING', 'CONFIRMED', 'FAILED') NOT NULL DEFAULT 'PENDING',
    confirmations INT DEFAULT 0,
    error_message TEXT,
    
    -- Timestamps
    submitted_at TIMESTAMP NOT NULL,
    confirmed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_transfers_swap (swap_id),
    INDEX idx_transfers_token (token_address),
    INDEX idx_transfers_tx_hash (tx_hash),
    INDEX idx_transfers_status (status),
    INDEX idx_transfers_from (from_address),
    INDEX idx_transfers_to (to_address),
    
    FOREIGN KEY (swap_id) REFERENCES swaps(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================================
-- SEED DATA: Migrate existing ERC-20 tokens from currencies table
-- =============================================================================

INSERT INTO tokens (symbol, name, network, contract_address, decimals, token_type, is_active, is_verified, gas_multiplier)
SELECT 
    symbol,
    name,
    network,
    contract_address,
    decimals,
    CASE 
        WHEN network = 'ethereum' AND contract_address IS NOT NULL THEN 'ERC20'
        WHEN network = 'bsc' AND contract_address IS NOT NULL THEN 'BEP20'
        WHEN network = 'tron' AND contract_address IS NOT NULL THEN 'TRC20'
        WHEN network = 'solana' AND contract_address IS NOT NULL THEN 'SPL'
        ELSE 'NATIVE'
    END as token_type,
    is_active,
    TRUE as is_verified,
    CASE 
        WHEN contract_address IS NOT NULL THEN 3.0
        ELSE 1.0
    END as gas_multiplier
FROM currencies
WHERE network IN ('ethereum', 'bsc', 'polygon', 'tron', 'solana')
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- =============================================================================
-- SEED DATA: Add popular ERC-20 tokens
-- =============================================================================

INSERT INTO tokens (symbol, name, network, contract_address, decimals, token_type, is_active, is_verified, gas_multiplier) VALUES
-- Ethereum ERC-20 tokens
('WETH', 'Wrapped Ether', 'ethereum', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 18, 'ERC20', TRUE, TRUE, 3.0),
('WBTC', 'Wrapped Bitcoin', 'ethereum', '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599', 8, 'ERC20', TRUE, TRUE, 3.0),
('UNI', 'Uniswap', 'ethereum', '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984', 18, 'ERC20', TRUE, TRUE, 3.0),
('LINK', 'Chainlink', 'ethereum', '0x514910771AF9Ca656af840dff83E8264EcF986CA', 18, 'ERC20', TRUE, TRUE, 3.0),
('AAVE', 'Aave', 'ethereum', '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9', 18, 'ERC20', TRUE, TRUE, 3.0),

-- BSC BEP-20 tokens
('CAKE', 'PancakeSwap', 'bsc', '0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82', 18, 'BEP20', TRUE, TRUE, 3.0),
('WBNB', 'Wrapped BNB', 'bsc', '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', 18, 'BEP20', TRUE, TRUE, 3.0)

ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;
