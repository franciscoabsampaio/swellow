-- 1. Create table without defaults
CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    email STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
USING DELTA;

-- 2. Enable defaults feature
ALTER TABLE users
SET TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- 3. Add default
ALTER TABLE users
ALTER COLUMN created_at
SET DEFAULT current_timestamp();