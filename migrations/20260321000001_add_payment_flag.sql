-- =============================================================================
-- Add payment flag to swaps table
-- Distinguishes payment swaps from standard swaps
-- =============================================================================

ALTER TABLE swaps 
ADD COLUMN is_payment BOOLEAN NOT NULL DEFAULT FALSE 
AFTER is_sandbox;

-- Add index for filtering payment swaps
CREATE INDEX idx_swaps_is_payment ON swaps(is_payment);
