-- ============================================================================
-- Migration: Add blockchain listener support
-- Created: 2026-02-17
-- Description: Add new status and columns for blockchain event detection
-- ============================================================================

-- Add new status for when funds are detected on blockchain
ALTER TABLE swaps MODIFY COLUMN status ENUM(
    'waiting',
    'confirming',
    'exchanging',
    'sending',
    'funds_received',
    'completed',
    'failed',
    'refunded',
    'expired'
) NOT NULL DEFAULT 'waiting';

-- Add columns to track actual received amounts (using conditional logic)
SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = 'swap_address_info' AND column_name = 'actual_received' AND table_schema = DATABASE()), 
    'ALTER TABLE swap_address_info ADD COLUMN actual_received DOUBLE DEFAULT NULL AFTER payout_amount');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = 'swap_address_info' AND column_name = 'commission_taken' AND table_schema = DATABASE()), 
    'ALTER TABLE swap_address_info ADD COLUMN commission_taken DOUBLE DEFAULT NULL AFTER actual_received');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = 'swap_address_info' AND column_name = 'last_balance_check' AND table_schema = DATABASE()), 
    'ALTER TABLE swap_address_info ADD COLUMN last_balance_check TIMESTAMP NULL AFTER confirmed_at');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add indexes for blockchain listener queries (ignore if exists)
SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE table_name = 'swaps' AND index_name = 'idx_swaps_funds_pending' AND table_schema = DATABASE()), 
    'CREATE INDEX idx_swaps_funds_pending ON swaps(status, to_network)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE table_name = 'swap_address_info' AND index_name = 'idx_swap_address_pending' AND table_schema = DATABASE()), 
    'CREATE INDEX idx_swap_address_pending ON swap_address_info(status, our_address)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

