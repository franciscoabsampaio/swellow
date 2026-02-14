import pytest
import swellow
from swellow.app import SwellowEngineError, SwellowFileNotFoundError, SwellowVersionError


@pytest.mark.parametrize("db_backend", [("postgres", True)], indirect=True)
def test_invalid_version_number(db_backend):
    directory = f"./tests/common/invalid_version_number/"
    with pytest.raises(SwellowVersionError) as exc_info:
        swellow.up(
            db=db_backend['conn_url'],
            directory=directory,
            engine=db_backend['engine'],
            json=True
        )
    assert "Invalid version number" in str(exc_info.value)

@pytest.mark.parametrize("db_backend", [("postgres", True)], indirect=True)
def test_migration_version_conflict(db_backend):
    directory = f"./tests/common/migration_version_conflict/"
    with pytest.raises(SwellowVersionError) as exc_info:
        swellow.up(
            db=db_backend['conn_url'],
            directory=directory,
            engine=db_backend['engine'],
            json=True
        )
    assert "More than one migration found with version" in str(exc_info.value)

# Test no connection returns an EngineError
def test_no_connection():
    with pytest.raises(SwellowEngineError):
        swellow.peck(
            db="postgresql://invalid:invalid@localhost:5432/invalid",
            directory=f"./tests/postgres/missing_up",
            json=True
        )

# TODO: Test lock already exists

# Test missing up.sql failure
def test_missing_up(db_backend):
    with pytest.raises(SwellowFileNotFoundError):
        swellow.up(
            db=db_backend['conn_url'],
            directory=f"{db_backend['directory']}/missing_up",
            engine=db_backend['engine'],
            json=True
        )

# Test missing down.sql failure
def test_missing_down(db_backend):
    directory = f"{db_backend['directory']}/missing_down/"
    swellow.up(
        db=db_backend['conn_url'],
        directory=directory,
        engine=db_backend['engine'],
        json=True
    )
    with pytest.raises(SwellowFileNotFoundError):
        swellow.down(
            db=db_backend['conn_url'],
            directory=directory,
            engine=db_backend['engine'],
            json=True
        )

# Test migration with rollback
def test_migrate_and_rollback(db_backend, swellow_driver):
    # Migrate and rollback to/from progressively higher versions.
    directory = f"{db_backend['directory']}/migrate_and_rollback/"

    for i in range(3):
        swellow_driver.up(
            db=db_backend['conn_url'],
            directory=directory,
            engine=db_backend['engine'],
            json=True,
            target_version_id=i+1,
            no_transaction=db_backend['flag_no_transaction'],
        )
        swellow_driver.down(
            db=db_backend['conn_url'],
            directory=directory,
            engine=db_backend['engine'],
            json=True,
            no_transaction=db_backend['flag_no_transaction'],
        )

# Test snapshot creation and accuracy
def test_snapshot(db_backend):
    # Start by setting up some resources for the snapshot to capture.
    directory = f"{db_backend['directory']}/snapshot/"
    engine = db_backend['engine']
    swellow.up(
        db=db_backend['conn_url'],
        directory=directory,
        engine=db_backend['engine'],
        json=True,
        no_transaction=True,  # Required to CREATE DATABASE with Postgres
    )

    # Now create the snapshot.
    swellow.snapshot(
        db=db_backend['conn_url'],
        directory=directory,
        engine=db_backend['engine'],
        json=True,
    )

    # Finally, verify the snapshot contents.
    with open(f"{directory}000003_snapshot/up.sql", "r") as f:
        snapshot_sql = f.read()

    if engine == "postgres":
        assert "CREATE SCHEMA bird_watch" in snapshot_sql
    elif engine == "databricks-delta":
        assert "CREATE DATABASE IF NOT EXISTS bird_watch" in snapshot_sql
    else:
        assert "CREATE DATABASE bird_watch" in snapshot_sql

    if engine == "postgres":
        assert "CREATE TABLE bird_watch.flock" in snapshot_sql
    elif engine == "spark-delta":
        assert "CREATE TABLE spark_catalog.bird_watch.flock" in snapshot_sql
    elif engine == "spark-iceberg":
        assert "CREATE TABLE local.bird_watch.flock" in snapshot_sql
    elif engine == "databricks-delta":
        assert "CREATE TABLE bird_watch.flock" in snapshot_sql

    if engine == "postgres":
        assert "CREATE VIEW bird_watch.flock_summary" in snapshot_sql
    elif engine == "spark-delta":
        # Vanilla Spark with Delta doesn't support
        # 'SHOW CREATE TABLE',
        # and thus the view shouldn't exist in the snapshot.
        assert "CREATE VIEW spark_catalog.bird_watch.flock_summary" not in snapshot_sql
    elif engine == "spark-iceberg":
        assert "CREATE VIEW local.bird_watch.flock_summary" in snapshot_sql
    elif engine == "databricks-delta":
        assert "CREATE VIEW bird_watch.flock_summary" in snapshot_sql

    # Clean up by destroying the snapshot.
    import shutil
    shutil.rmtree(directory + "000004_snapshot")
