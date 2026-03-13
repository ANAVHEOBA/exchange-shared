-- Add atomic counter table for address index generation
-- This prevents race conditions when multiple swaps are created simultaneously

CREATE TABLE IF NOT EXISTS address_index_counter (
    id INT PRIMARY KEY DEFAULT 1,
    next_index INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT single_row CHECK (id = 1)
) ENGINE=InnoDB;

-- Initialize with the current max index from existing data
INSERT INTO address_index_counter (id, next_index)
SELECT 1, COALESCE(MAX(address_index), 0)
FROM swap_address_info
ON DUPLICATE KEY UPDATE next_index = VALUES(next_index);
