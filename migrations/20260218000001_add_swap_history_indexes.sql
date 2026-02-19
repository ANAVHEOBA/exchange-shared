-- ============================================================================
-- Migration: Add optimized indexes for swap history pagination
-- Created: 2026-02-18
-- Description: Composite indexes for keyset pagination performance
-- ============================================================================

-- Primary index for keyset pagination (user + time + id)
-- This is the CRITICAL index for O(1) pagination performance
SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE table_name = 'swaps' AND index_name = 'idx_swaps_user_history' AND table_schema = DATABASE()), 
    'CREATE INDEX idx_swaps_user_history ON swaps (user_id, created_at DESC, id DESC)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Index for status filtering with keyset pagination
SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE table_name = 'swaps' AND index_name = 'idx_swaps_user_status_history' AND table_schema = DATABASE()), 
    'CREATE INDEX idx_swaps_user_status_history ON swaps (user_id, status, created_at DESC, id DESC)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Index for currency filtering with keyset pagination
SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE table_name = 'swaps' AND index_name = 'idx_swaps_user_currency_history' AND table_schema = DATABASE()), 
    'CREATE INDEX idx_swaps_user_currency_history ON swaps (user_id, from_currency, to_currency, created_at DESC, id DESC)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Index for provider filtering with keyset pagination
SET @sql = IFNULL((SELECT 'SELECT 1' FROM INFORMATION_SCHEMA.STATISTICS 
    WHERE table_name = 'swaps' AND index_name = 'idx_swaps_user_provider_history' AND table_schema = DATABASE()), 
    'CREATE INDEX idx_swaps_user_provider_history ON swaps (user_id, provider_id, created_at DESC, id DESC)');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

