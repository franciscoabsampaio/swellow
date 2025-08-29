# Swellow 🐦‍⬛

**Swellow** is a simple, intuitive, Rust-based tool for managing table migrations.

## Features

### Migration Table

- Create table `swellow` if not exist.
- Schema:

```sql
id INTEGER,
oid INTEGER,  -- Object ID, e.g. products
object_name_before VARCHAR(n),  -- Object name before 
object_name_after VARCHAR(n),
version VARCHAR(n),  -- 20251406_00_create_records
version_id INTEGER,  -- 2025140600
status VARCHAR(n),  -- E.g. READY, EXECUTED, TESTED, ROLLED_BACK
checksum INTEGER,
dtm_created_at TIMESTAMP,
dtm_updated_at TIMESTAMP
```

- `checksum` column to detect if someone modified a migration since the last step.

### Directory of Migrations

- The migration directory is expected to have a specific structure.
- ???

### Flow

- ✅ Check what migrations to apply.
- ✅ Validate code syntax against database engine.
- ✅ Warn if a table contains breaking changes (e.g. `DROP`).
- ✅ Locking: check if a `swellow` process / transaction is already under way.
- ✅ Allow only atomic changes (no per-resource tracking).

### API

- `swellow peck`.
  - ✅ Test connection to DB. Can take environment variables or args.
- `swellow [up | down] [migration_version] [--dry-run]` to perform migration up to the target version, or rollback.
  - ✅ `up | down`.
  - ✅ `migration_version`.
  - ✅ `--dry-run` for dry runs.

```sql
-- Would apply migration 20250827_add_users.sql
-- SQL:
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- Would apply migration 20250828_add_index.sql
-- SQL:
CREATE INDEX idx_users_name ON users(name);
```

- `swellow snapshot` to analyse all migrations and create an up-to-date schema of the existing table.
- `swellow squash [migration_version]` to squash all migrations up to the target version into a single `CREATE TABLE` `sql` script.
