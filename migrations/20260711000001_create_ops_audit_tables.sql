CREATE TABLE IF NOT EXISTS ops_notes (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(120) NOT NULL,
    admin_id VARCHAR(120) NOT NULL,
    admin_email VARCHAR(320) NOT NULL,
    note TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ops_notes_entity (entity_type, entity_id, created_at),
    INDEX idx_ops_notes_admin (admin_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ops_reveal_events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(120) NOT NULL,
    field_group VARCHAR(64) NOT NULL,
    reason TEXT NOT NULL,
    admin_id VARCHAR(120) NOT NULL,
    admin_email VARCHAR(320) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ops_reveal_entity (entity_type, entity_id, created_at),
    INDEX idx_ops_reveal_admin (admin_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
