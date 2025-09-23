# Swellow 🐦‍⬛

**Swellow** is the simple, SQL-first tool for managing table migrations, written in Rust.

## Getting Started

Swellow comes in two packages: a [Rust CLI](#cli), and [a Python package](#python-module). We've also created a [GitHub Action for quick-and-easy integration in CI pipelines](https://github.com/franciscoabsampaio/action-swellow/).

Behind the scenes, all versions of swellow use the Rust backend, ensuring consistent behaviour across tools.

<details><summary><b>CLI</b></summary>

Go to the [repository's latest release](https://github.com/franciscoabsampaio/swellow/releases/latest) and download the binary, or do it in the terminal:

```bash
curl -L https://github.com/franciscoabsampaio/swellow/releases/latest/download/swellow-x86_64-unknown-linux-gnu.tar.gz | tar -xz
```

Verify the installation:

```bash
swellow --version
```

and you're good to go!

</details>

<details>
<summary><b>Python Module</b></summary>

Just like with any other Python package:

```bash
pip install swellow
```

Now you can import it:

```py
import swellow
import os

DIRECTORY_WITH_MIGRATIONS='./migrations'
DATABASE_CONNECTION_STRING=os.getenv("CONNECTION_STRING")

swellow.up(
  db=DATABASE_CONNECTION_STRING,
  directory=DIRECTORY_WITH_MIGRATIONS,
)
```

Or use it as a CLI:

```bash
swellow --version
```

</details>

<details>
<summary><b>GitHub Action</b></summary>

Simply add it to your workflow:

```yaml
- name: Execute migrations
  use: franciscoabsampaio/action-swellow@v1
  with:
    - command: up
    - connection-string: postgresql://<username>:<password>@<host>:<port>/<database>
```

</details>

## Functionality

`swellow --help` will show you all commands and options available. Here are the most important:

```sh
The simple, intuitive tool for managing table migrations, written in Rust.

Usage: swellow [OPTIONS] --db <DB_CONNECTION_STRING> --dir <MIGRATION_DIRECTORY> <COMMAND>

Commands:
  peck      Test connection to the database.
  up        Generate a migration plan and execute it.
  down      Generate a rollback plan and execute it.
  snapshot  Use pg_dump to take a snapshot of the database schema into a set of CREATE statements.

Options:
      --db <DB_CONNECTION_STRING>  Database connection string. Please follow the following format:
                                       postgresql://<username>:<password>@<host>:<port>/<database>
                                    [env: DB_CONNECTION_STRING]
      --dir <MIGRATION_DIRECTORY>  Directory containing all migrations [env: MIGRATION_DIRECTORY=]
```

### Installing the CLI

## Python Module

The Python module exposes both the CLI

### Installing the Python module

Simply run `pip install swellow` or `uv add swellow` to add Swellow to your project!

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
