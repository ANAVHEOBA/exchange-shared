-- ============================================================================
-- Migration: Update currencies table for Trocador integration
-- Created: 2026-01-21 (REVISED)
-- Description: Add min/max amounts and cache tracking to currencies table
-- ============================================================================

-- TiDB does not reliably allow referencing columns added earlier in the same
-- ALTER statement via AFTER <new_column>, so perform each change separately.

-- Add global min/max amounts (from Trocador /coins endpoint)
-- Using DOUBLE instead of DECIMAL for f64 compatibility
SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'currencies'
          AND column_name = 'min_amount'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE currencies ADD COLUMN min_amount DOUBLE DEFAULT NULL AFTER requires_extra_id'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'currencies'
          AND column_name = 'max_amount'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE currencies ADD COLUMN max_amount DOUBLE DEFAULT NULL AFTER min_amount'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'currencies'
          AND column_name = 'last_synced_at'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE currencies ADD COLUMN last_synced_at TIMESTAMP NULL AFTER max_amount'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Add index for cache freshness checks
SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_name = 'currencies'
          AND index_name = 'idx_currencies_last_synced'
          AND table_schema = DATABASE()
    ),
    'CREATE INDEX idx_currencies_last_synced ON currencies(last_synced_at)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Remove min/max from provider_currencies (Trocador uses global limits)
SET @sql = IFNULL(
    (
        SELECT CONCAT('ALTER TABLE provider_currencies DROP COLUMN ', column_name)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'provider_currencies'
          AND column_name = 'min_amount'
          AND table_schema = DATABASE()
    ),
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL(
    (
        SELECT CONCAT('ALTER TABLE provider_currencies DROP COLUMN ', column_name)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'provider_currencies'
          AND column_name = 'max_amount'
          AND table_schema = DATABASE()
    ),
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Update existing seed data with placeholder values (will be synced from Trocador)
UPDATE currencies SET last_synced_at = NULL WHERE last_synced_at IS NULL;
