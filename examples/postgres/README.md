# Example: Postgres

This example demonstrates how to use Swellow to manage schema migrations against a PostgreSQL database, using both the CLI and the Python interface.

The example walks through a realistic migration workflow, starting from a fresh database and progressing through planning, execution, validation, and rollback planning.

## What this example shows

This example exercises the most common Swellow workflows in a single, linear script:

1. **Connectivity check.** Verifies that Swellow can connect to the database before performing any migrations.

2. **Greenfield migration.** Applies the initial schema migration to an empty database.

3. **Migration planning.** Generates a migration plan up to a target version without executing it.

4. **Incremental schema evolution.** Applies multiple migrations step by step to evolve the schema.

5. **Dry-run validation.** Executes a migration inside a transaction and rolls it back, allowing you to safely test complex or risky changes.

6. **Applying a tested migration.** Runs the previously dry-tested migration for real.

7. **Schema snapshotting.** Captures the current database schema as a snapshot migration.

8. **Rollback planning.** Generates a rollback plan to a previous version without executing it.

Together, these steps demonstrate how Swellow can be used throughout the full lifecycle of schema changes—from development to validation and recovery.

### Expected result

After running the example successfully:

- The database schema will be migrated up to version 5;
- All migrations up to that version will be marked as applied;
- A new snapshot migration directory will be created (6_snapshot/);
- No rollback will be executed, but a rollback plan to version 4 will be displayed;
- No data will be modified during planning or dry-run steps.

If the database is empty at the start, the example should complete without errors and leave the database in a fully migrated, consistent state.

## Running the Example

Because Swellow supports multiple interfaces, this example is implemented in two ways:

- `main.py`: Uses the Python API to run migrations programmatically;
- `main.sh`: Uses the Swellow CLI to run the same workflow from the command line.

Both approaches run the same engine under the hood and should produce equivalent results.

### Prerequisites

`swellow` uses [`pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html) when taking schema snapshots on PostgreSQL. To use the `snapshot` command, `pg_dump` must be installed locally and its version should match the PostgreSQL server version.

For setting up a PostgreSQL database for testing, you can use Docker:

```bash
docker run --name pg \
    -e POSTGRES_USER=pguser \
    -e POSTGRES_PASSWORD=pgpass \
    -e POSTGRES_DB=mydb \
    -p 5432:5432 -d postgres
```

### Run the Python Example

```bash
export DB_CONNECTION_STRING="postgresql://user:pass@10.0.0.1:5432/mydb"
export MIGRATION_DIRECTORY="./migrations"

python main.py
```

### Run the CLI Example

```bash
export DB_CONNECTION_STRING="postgresql://user:pass@10.0.0.1:5432/dbname"
export MIGRATION_DIRECTORY="./migrations"
bash ./main.sh
```

### Verifying Results

Running the example will quickly output a series of logs indicating the progress of each step. Take your time to read through them to understand what `swellow` is doing at each stage.

You can easily verify the final state of the database using `psql` or any PostgreSQL client. For example, to check the applied migrations:

```bash
psql postgresql://pguser:pgpass@localhost:5432/mydb \
    -P pager=off \
    -c "SELECT * FROM swellow.records ORDER BY version_id;"
```
