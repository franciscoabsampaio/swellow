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

### Creating New Migrations

Being SQL-first, swellow requires the user to define a `directory` where all migration scripts will be housed.

New migrations are defined by a subdirectory in the migrations directory, that must contain an `up.sql` and a `down.sql` script, and must follow the following naming convention:

```bash
# Assuming the migrations directory is "./migrations"
./migration/
├── 123_this_is_the_first_migration/
│   │   # 123 is the migration version
│   ├── up.sql      # This is the migration script
│   └── down.sql    # This is the rollback script
├── 242_this_is_the_second/  # Second, because 242 > 123 🥀
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

**Swellow** automatically gathers all migrations within the specified range (by default, all that haven't been applied), and executes them.

`up.sql` scripts specify the new migration to be applied, and `down.sql` scripts their respective rollback scripts. Missing `up.sql` scripts and missing `down.sql` scripts will result in errors when migrating and rolling back, respectively.

**If any migration or rollback fails, the transaction will be rolled back, and the database will keep its original state.** Users can also preemptively check the validity of transactions by passing the `--dry-run` flag, which automatically cancels (and rolls back) the transaction after executing all migrations.

### Taking Snapshots

The `snapshot` command/function scans the database and creates an `up.sql` script with everything needed to create all relations in the database. Database engines are used by default (e.g. `pg_dump` for PostgreSQL), so be sure to look up the relevant documentation if you find any issue with the snapshot behaviour.

### Migrating to Swellow

**Swellow** makes as few assumptions as possible about an existing database. For this reason, given a directory of migration scripts, all that is required is a connection to the existing database - `swellow up` will take care of the rest.

If you wish to start tracking the database in CI, [take a snapshot](#taking-snapshots).

If a `swellow_records` table already exists in the target database, the latest migration version in its active records (a record is active if it has a status of `APPLIED` or `TESTED`) will be assumed as the current version. This can easily be overriden by specifying the `current_version` argument, or changing the versions in migrations directory to be larger.

## CLI Reference

`swellow --help` will show you all commands and options available.

```sh
Swellow is the simple, SQL-first tool for managing table migrations, written in Rust.

Usage: swellow [OPTIONS] --db <DB_CONNECTION_STRING> --dir <MIGRATION_DIRECTORY> <COMMAND>

Commands:
  peck      Test connection to the database.
  up        Generate a migration plan and execute it.
  down      Generate a rollback plan and execute it.
  snapshot  Use pg_dump to take a snapshot of the database schema into a set of CREATE statements.

Options:
      --db <DB_CONNECTION_STRING>  Database connection string. Please follow your database's recommended format:
                                       postgresql://<username>:<password>@<host>:<port>/<database>
                                    [env: DB_CONNECTION_STRING]
      --dir <MIGRATION_DIRECTORY>  Directory containing all migrations [env: MIGRATION_DIRECTORY=]
```

---

## License

This action is licensed under the Apache 2.0 License.
