-- ============================================================================
-- Migration: Add payout-side network fee tracking
-- Created: 2026-03-24
-- Description: Persist service fee and network fee separately for settled payouts
-- ============================================================================

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'swap_address_info'
          AND column_name = 'network_fee_paid'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE swap_address_info ADD COLUMN network_fee_paid DOUBLE DEFAULT NULL AFTER commission_taken'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
