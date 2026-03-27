-- =============================================================================
-- ADD TRADE_ID_FROM_RATE COLUMN TO SWAPS TABLE
-- Track the trade_id from the rate quote to prevent duplicate swaps
-- =============================================================================

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'swaps'
          AND column_name = 'trade_id_from_rate'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE swaps ADD COLUMN trade_id_from_rate VARCHAR(100) DEFAULT NULL AFTER provider_swap_id'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_name = 'swaps'
          AND index_name = 'uk_swaps_trade_id_from_rate'
          AND table_schema = DATABASE()
    ),
    'CREATE UNIQUE INDEX uk_swaps_trade_id_from_rate ON swaps(trade_id_from_rate)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Create index for lookups
SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_name = 'swaps'
          AND index_name = 'idx_swaps_trade_id_from_rate'
          AND table_schema = DATABASE()
    ),
    'CREATE INDEX idx_swaps_trade_id_from_rate ON swaps(trade_id_from_rate)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
