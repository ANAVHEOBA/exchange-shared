ALTER TABLE swap_address_info
MODIFY COLUMN status ENUM('pending', 'processing', 'success', 'failed')
NOT NULL DEFAULT 'pending';
