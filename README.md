# Swellow 🐦‍⬛

[![PyPI version](https://badge.fury.io/py/swellow.svg)](https://badge.fury.io/py/swellow)
[![Release](https://github.com/franciscoabsampaio/swellow/actions/workflows/release.yaml/badge.svg)](https://github.com/franciscoabsampaio/swellow/actions/workflows/release.yaml)
[![Crates.io](https://img.shields.io/crates/v/swellow.svg)](https://crates.io/crates/swellow)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

![Take flight with `swellow`!](https://raw.githubusercontent.com/franciscoabsampaio/swellow/main/docs/image.jpg)

`swellow` is the simple, SQL-first tool for managing table migrations, written in Rust.

## [Changelog](https://github.com/franciscoabsampaio/swellow/blob/main/CHANGELOG.md)

## Built For Your Data Flock

![The data flock.](https://raw.githubusercontent.com/franciscoabsampaio/swellow/main/docs/data_flock.png)

## Why Swellow?

**If you can write SQL for your database, you already know how to use `swellow`**.

Instead of offering a feature-rich migration DSL, `swellow` acts as a thin wrapper over your database engine. **Migrations are defined as plain SQL files in a directory**, and **swellow tracks their execution using a small metadata table** (`swellow.records`) in the target database.

There is:

- ❌ **No** generated SQL;
- ❌ **No** implicit transformations;
- ❌ **No** migration logic hidden from the user;

✅ What runs is *exactly* what you wrote, using **your database’s native syntax**.

✅ This makes migrations **transparent**, **predictable**, and **safe**, because knowing what runs in production matters more than convenient abstractions.

**If you want a migration tool that stays out of your way and treats SQL as the source of truth, `swellow` is for you.**

## Getting Started

`swellow` comes in many forms, the primary being the Python CLI and package. For the brave, you can try the Rust binary as well.
We've also created a [GitHub Action for quick-and-easy integration in CI pipelines](https://github.com/franciscoabsampaio/action-swellow/).

Behind the scenes, all versions of `swellow` use the same Rust backend, ensuring consistent behaviour across tools, so let's get started!

<details><summary><b>Python CLI</b></summary>

Just like with any other Python package:

```bash
pip install swellow
```

And use it as a CLI:

```bash
swellow --db $DATABASE_CONNECTION_STRING --dir './migrations' peck
```

</details>

<details><summary><b>Python Module</b></summary>

Just like with any other Python package:

```bash
pip install swellow
```

Now you can import it:

```py
import swellow
import os

DIRECTORY_WITH_MIGRATIONS='./migrations'
DATABASE_CONNECTION_STRING=os.getenv("DATABASE_CONNECTION_STRING")

swellow.peck(
  db=DATABASE_CONNECTION_STRING,
  directory=DIRECTORY_WITH_MIGRATIONS,
)
```

</details>

<details><summary><b>GitHub Action</b></summary>

Simply add it to your workflow:

```yaml
- name: Execute migrations
  use: franciscoabsampaio/action-swellow@v1
  with:
    - command: peck
    - connection-string: postgresql://<username>:<password>@<host>:<port>/<database>
```

It's that easy!

</details>

<details><summary><b>Pre-Built Rust Binary</b></summary>

Go to the [repository's latest release](https://github.com/franciscoabsampaio/swellow/releases/latest) and download the binary, or do it in the terminal:

```bash
# We show for linux, but you can find binaries for other platforms as well.
curl -L https://github.com/franciscoabsampaio/swellow/releases/latest/download/release-binary-x86_64-unknown-linux-gnu.tar.gz | tar -xz
```

Install it:

```bash
sudo mv swellow /usr/local/bin/
```

Verify the installation:

```bash
swellow --version
```

And ensure that `swellow` has everything it needs to run with `peck`:

```bash
swellow --db DATABASE_CONNECTION_STRING --dir MIGRATIONS_DIRECTORY peck
```

</details>

### Creating New Migrations

**`swellow` lets you define migrations using plain SQL from your database of choice**.

To create a new migration, the user must define a `directory` where all migration scripts will be housed.

New migrations are defined by a subdirectory in the migrations directory, that must contain an `up.sql` and a `down.sql` script, and must follow the following naming convention:

```bash
# Assuming the migrations directory is "./migrations"
./migration/
├── 000123_this_is_the_first_migration/
│   │   # 123 is the migration version. Versions are zero-padded up to 6 digits.
│   ├── up.sql      # This is the migration script
│   └── down.sql    # This is the rollback script
├── 000242_this_is_the_second/  # Second, because 242 > 123 🥀
│   └── up.sql               # This migration has no rollback script - when attempting to rollback, this will raise an error. Likewise, a missing 'up.sql' script will raise an error.
└── ...
```

Here's what an `up.sql` script may look like:

```sql
-- Create a table of birds 🐦‍⬛
CREATE TABLE flock (
    bird_id SERIAL PRIMARY KEY,
    common_name TEXT NOT NULL,
    latin_name TEXT NOT NULL,
    wingspan_cm INTEGER,
    dtm_hatched_at TIMESTAMP DEFAULT now(),
    dtm_last_seen_at TIMESTAMP DEFAULT now()
);

-- Add a new column to track nest activity 🪺
ALTER TABLE nest ADD COLUMN twigs_collected INTEGER;
```

`swellow` automatically gathers all migrations within the specified range (by default, all that haven't been applied), and executes them.

`up.sql` scripts specify the new migration to be applied, and `down.sql` scripts their respective rollback scripts. Missing `up.sql` scripts and missing `down.sql` scripts will result in errors when migrating and rolling back, respectively.

**If any migration or rollback fails, the transaction will be rolled back, and the database will keep its original state.** Users can also preemptively check the validity of transactions by passing the `--dry-run` flag, which automatically cancels (and rolls back) the transaction after executing all migrations.

### Pecking is Optional

`swellow` keeps track of migrations in its `records` table, which resides in a special schema `swellow` that it creates when the command `peck` is executed.

Behind the scenes, all migration commands start by running `peck`, making it, in most cases, superfluous. Pecking can be very useful for testing database connectivity, though. More importantly, perhaps, [it really makes you feel like a bird](https://knowyourmeme.com/memes/this-game-really-makes-you-feel-like-batman).

### Taking Snapshots

**⚠️ `swellow snapshot` does not save any data.** It is aimed at cleaning up directories filled with old migrations and combining them into a current definition of the database. The `snapshot` command/function scans the database and creates an `up.sql` script with everything needed to create all relations in the database.

In line with swellow's 'SQL-first' design philosophy, we believe that native options should be used when available (e.g. `pg_dump` for PostgreSQL). Unfortunately, this can make snapshotting behaviour *very inconsistent across databases*. On the upside, snapshots are completely harmless.

Here's a quick reference to what snapshotting does for each database:

- **Postgres**: runs `pg_dump` against the database.
- **Spark-Delta**: iterates through all databases and tables, generating `CREATE DATABASE` for databases, and parsing table properties obtained from `DESCRIBE TABLE` and `DESCRIBE DETAIL` into `CREATE` statements.
- **Spark-Iceberg**: iterates through all databases and tables, generating `CREATE DATABASE` for databases and executing Iceberg's native `SHOW CREATE TABLE` for tables.

Users are encouraged to take a look at the source code, look up the relevant documentation, and if they find any limitation with the snapshotting behaviour, [open an issue](https://github.com/franciscoabsampaio/swellow/issues/new/choose).

### Migrating to Swellow

`swellow` makes as few assumptions as possible about an existing database, and prioritizes native options when available. For this reason, given a directory of migration scripts, all that is required is a connection to the existing database - `swellow up` will take care of the rest.

If you wish to start tracking the database in CI, [take a snapshot](#taking-snapshots).

If a `swellow.records` table already exists in the target database, the latest migration version in its active records (a record is active if it has a status of `APPLIED` or `TESTED`) will be assumed as the current version. This can easily be overriden by specifying the `current_version` argument, or changing the versions in migrations directory to be larger.

## Examples

Refer to the [`examples` directory in this repository](https://github.com/franciscoabsampaio/swellow/tree/main/examples) for examples on how to use `swellow` with your data flock.

## CLI Reference

`swellow --help` will show you all commands and options available.

```sh
Swellow is the simple, SQL-first tool for managing table migrations, written in Rust.

Usage: swellow [OPTIONS] --db <DB_CONNECTION_STRING> --dir <MIGRATION_DIRECTORY> <COMMAND>

Commands:
  peck      Test connection to the database.
  up        Generate a migration plan and execute it.
  down      Generate a rollback plan and execute it.
  snapshot  ⚠️ Doesn't snapshot data, ONLY SCHEMA.
            Take a snapshot of the database schema into a set of CREATE statements.
            Automatically creates a new version migration subdirectory like '<VERSION>_snapshot'.
            ⚠️ Postgres: pg_dump must be installed with a version matching the server's.
  help      Print this message or the help of the given subcommand(s)

Options:
      --db <DB_CONNECTION_STRING>  Database connection string. Please follow your database's recommended format, e.g.:
                                       postgresql://<username>:<password>@<host>:<port>/<database>
                                    [env: DB_CONNECTION_STRING]
      --dir <MIGRATION_DIRECTORY>  Directory containing all migrations [env: MIGRATION_DIRECTORY=]
      --engine <ENGINE>            Database / catalog engine. [env: ENGINE=] [default: postgres] [possible values: postgres, spark-delta, spark-iceberg]
  -v, --verbose...                 Set level of verbosity. [default: INFO]
                                        -v: DEBUG
                                        -vv: TRACE
                                   --quiet takes precedence over --verbose.
  -q, --quiet                      Disable all information logs (only ERROR level logs are shown).
                                   --quiet takes precedence over --verbose.
      --json                       Enable JSON output format. Human readable output is disabled when this flag is set.
  -h, --help                       Print help
  -V, --version                    Print version
```

---
© 2025 Francisco A. B. Sampaio. Licensed under the MIT License.

This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.
