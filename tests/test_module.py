import docker
import pytest
import time
import swellow
from swellow.app import SwellowFileNotFoundError


def wait_for_log(container, message, timeout=30):
    start = time.time()
    while True:
        logs = container.logs().decode("utf-8")
        if message in logs:
            return
        if time.time() - start > timeout:
            raise TimeoutError(f"Message '{message}' not found in logs")
        time.sleep(0.5)


@pytest.fixture(scope="function", params=["postgres", "spark-delta", "spark-iceberg"])
def db_backend(request):
    backend = request.param

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

    yield conn_url, backend

    container.stop()

# TODO: Test lock already exists

# Test missing up
def test_missing_up(db_backend):
    with pytest.raises(SwellowFileNotFoundError):
        swellow.up(
            db=db_backend[0],
            directory=f"./tests/{db_backend[1]}/missing_up",
            engine=db_backend[1],
            json=True
        )

# Test missing down
def test_missing_down(db_backend):
    swellow.up(
        db=db_backend[0],
        directory=f"./tests/{db_backend[1]}/missing_down",
        engine=db_backend[1],
        json=True
    )
    with pytest.raises(SwellowFileNotFoundError):
        swellow.down(
            db=db_backend[0],
            directory=f"./tests/{db_backend[1]}/missing_down",
            engine=db_backend[1],
            json=True
        )

# Test migration+rollback:
def test_migrate_and_rollback(db_backend):
    # Migrate and rollback to/from progressively higher versions.
    for i in range(3):
        swellow.up(
            db=db_backend[0],
            directory=f"./tests/{db_backend[1]}/migrate_and_rollback",
            engine=db_backend[1],
            json=True,
            target_version_id=i+1
        )
        swellow.down(
            db=db_backend[0],
            directory=f"./tests/{db_backend[1]}/migrate_and_rollback",
            engine=db_backend[1],
            json=True
        )
