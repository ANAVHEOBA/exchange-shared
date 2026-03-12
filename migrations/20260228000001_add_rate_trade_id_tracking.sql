-- =============================================================================
-- ADD TRADE_ID_FROM_RATE COLUMN TO SWAPS TABLE
-- Track the trade_id from the rate quote to prevent duplicate swaps
-- =============================================================================

ALTER TABLE swaps ADD COLUMN trade_id_from_rate VARCHAR(100) UNIQUE DEFAULT NULL AFTER provider_swap_id;

-- Create index for lookups
CREATE INDEX idx_swaps_trade_id_from_rate ON swaps(trade_id_from_rate);
