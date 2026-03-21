-- Add username column to users table
ALTER TABLE users 
ADD COLUMN username VARCHAR(50) NULL AFTER email,
ADD UNIQUE INDEX idx_users_username (username);
