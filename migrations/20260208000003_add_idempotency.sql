-- Add payout_idempotency_key to prevent double spending.
-- TiDB does not support adding a column with an inline UNIQUE constraint via ALTER,
-- so add the column and the unique index separately.
SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'swap_address_info'
          AND column_name = 'payout_idempotency_key'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE swap_address_info ADD COLUMN payout_idempotency_key VARCHAR(100) DEFAULT NULL AFTER payout_tx_hash'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_name = 'swap_address_info'
          AND index_name = 'idx_swap_address_payout_idempotency_key'
          AND table_schema = DATABASE()
    ),
    'CREATE UNIQUE INDEX idx_swap_address_payout_idempotency_key ON swap_address_info(payout_idempotency_key)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
