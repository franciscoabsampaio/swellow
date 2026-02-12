-- 1. Create table without defaults
CREATE TABLE orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    user_id BIGINT NOT NULL,
    total_cents INT NOT NULL,
    created_at TIMESTAMP NOT NULL
)
USING DELTA;

-- 2. Enable defaults feature
ALTER TABLE orders
SET TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
);

-- 3. Add default
ALTER TABLE orders
ALTER COLUMN created_at
SET DEFAULT current_timestamp();