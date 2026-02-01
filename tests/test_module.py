import docker
import pytest
import time
import swellow
from swellow.app import SwellowEngineError, SwellowFileNotFoundError


def wait_for_log(container, message, timeout=30):
    start = time.time()
    while True:
        logs = container.logs().decode("utf-8")
        if message in logs:
            return
        if time.time() - start > timeout:
            raise TimeoutError(f"Message '{message}' not found in logs")
        time.sleep(0.5)


@pytest.fixture(
    scope="function",
    params=[ # (engine, flag_no_transaction)
        ("postgres", True),
        ("postgres", False),
        ("spark-delta", False),
        ("spark-iceberg", False),
    ],
    # Set custom ids for better readability in test outputs
    ids=lambda p: f"{p[0]}-no_tx={p[1]}",
)
def db_backend(request):
    backend = request.param[0]

    docker_client = docker.from_env()

    if backend == "postgres":
        image = docker_client.images.pull("postgres", tag="17.6")
        container = docker_client.containers.run(
            image,
            detach=True,
            environment={"POSTGRES_PASSWORD": "postgres"},
            ports={'5432/tcp': 5432},
        )
        time.sleep(5)
        conn_url = "postgresql://postgres:postgres@localhost:5432/postgres"

    elif backend in ["spark-delta", "spark-iceberg"]:
        if backend == "spark-delta":
            tag = 'delta'
        else:
            tag = 'iceberg'
        image = docker_client.images.pull("franciscoabsampaio/spark-connect-server", tag=tag)
        container = docker_client.containers.run(
            image,
            detach=True,
            ports={'15002/tcp': 15002}
        )
        wait_for_log(container, message="Spark Connect server started at:")
        conn_url = "sc://localhost:15002"

    else:
        raise ValueError(f"Unknown backend {backend}")

    yield {
        'conn_url': conn_url,
        'directory': f"./tests/{backend}",
        'engine': backend,
        'flag_no_transaction': request.param[1],
    }

    container.stop()

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
def test_migrate_and_rollback(db_backend):
    # Migrate and rollback to/from progressively higher versions.
    directory = f"{db_backend['directory']}/migrate_and_rollback/"

    for i in range(3):
        swellow.up(
            db=db_backend['conn_url'],
            directory=directory,
            engine=db_backend['engine'],
            json=True,
            target_version_id=i+1,
            no_transaction=db_backend['flag_no_transaction'],
        )
        swellow.down(
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

    match db_backend['engine']:
        case "postgres":
            assert "CREATE SCHEMA bird_watch" in snapshot_sql
        case _:
            assert "CREATE DATABASE bird_watch" in snapshot_sql

    match db_backend['engine']:
        case "postgres":
            assert "CREATE TABLE bird_watch.flock" in snapshot_sql
        case "spark-delta":
            assert "CREATE TABLE spark_catalog.bird_watch.flock" in snapshot_sql
        case "spark-iceberg":
            assert "CREATE TABLE local.bird_watch.flock" in snapshot_sql

    # Clean up by destroying the snapshot.
    import shutil
    shutil.rmtree(directory + "000003_snapshot")
