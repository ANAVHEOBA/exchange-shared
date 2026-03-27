-- Add username column to users table.
-- TiDB is more reliable when the column and unique index are added separately.
SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'users'
          AND column_name = 'username'
          AND table_schema = DATABASE()
    ),
    'ALTER TABLE users ADD COLUMN username VARCHAR(50) NULL AFTER email'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IFNULL(
    (
        SELECT 'SELECT 1'
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_name = 'users'
          AND index_name = 'idx_users_username'
          AND table_schema = DATABASE()
    ),
    'CREATE UNIQUE INDEX idx_users_username ON users(username)'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
