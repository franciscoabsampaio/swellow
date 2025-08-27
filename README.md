# Swellow 🐦‍⬛

**Swellow** is a simple, intuitive, Rust-based tool for managing table migrations.

## Features

### Migration Table

- Create table `swellow` if not exist.
- Schema: `id`, `target_table_id` (e.g. `products`), `migration_version`, `status` (e.g. `READY`, `EXECUTED`, `TESTED`, `ROLLED_BACK`), `checksum`, `dtm_created_at`, `dtm_updated_at`.
- `checksum` column to detect if someone modified a migration since the last step.

### Directory of Migrations

- The migration directory is expected to have a specific structure.
- ???

### Flow

- Check what migrations to apply.
- Validate code syntax against database engine.
- Warn if a table contains breaking changes (e.g. `DROP`).
- Locking: check if a `swellow` process / transaction is already under way.

### API

- `swellow peck`.
  - ✅ Test connection to DB. Can take environment variables or args.
  - Check if migration directory is correctly structured.
- `swellow plan` for dry runs.

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

- `swellow migrate [up | down] [--all | --target target_table_id] [migration_version]` to perform migration up to the target version, or rollback.
- `swellow snapshot` to analyse all migrations and create an up-to-date schema of the existing table.
- `swellow squash [target_table_id] [migration_version]` to squash all migrations up to the target version into a single `CREATE TABLE` `sql` script.
