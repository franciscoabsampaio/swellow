import os
import pytest
import swellow
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="module", params=["postgres", "spark-delta", "spark-iceberg"])
def db_backend(request):
    backend = request.param

    if backend == "postgres":
        container = PostgresContainer("postgres:latest")
        container.start()
        conn_url = container.get_connection_url()

    elif backend == "spark-delta":
        container = (
            DockerContainer("franciscoabsampaio/spark-connect:latest")
            .with_exposed_ports(15002)  # Hive Thrift Server port
        )
        container.start()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(15002)
        conn_url = f"sc://{host}:{port}/;token=ABCDEFG;user_agent=some_agent;user_id=user123"

    elif backend == "spark-iceberg":
        container = (
            DockerContainer("franciscoabsampaio/spark-connect:latest")
            .with_exposed_ports(15002)
        )
        container.start()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(15002)
        conn_url = f"sc://{host}:{port}/;token=ABCDEFG;user_agent=some_agent;user_id=user123"

    else:
        raise ValueError(f"Unknown backend {backend}")

    os.environ["DB_CONN"] = conn_url
    print(conn_url)

    yield backend

    container.stop()

# TODO: Test lock already exists

# Test missing up
def test_missing_up(db_backend):
    with pytest.raises(FileNotFoundError):
        swellow.up(
            db=os.getenv("DB_CONN"),
            directory=f"./tests/{db_backend}/missing_up",
            engine=db_backend
        )

# Test missing down
def test_missing_down(db_backend):
    swellow.up(
        db=os.getenv("DB_CONN"),
        directory=f"./tests/{db_backend}/missing_down",
        engine=db_backend
    )
    with pytest.raises(FileNotFoundError):
        swellow.down(
            db=os.getenv("DB_CONN"),
            directory=f"./tests/{db_backend}/missing_down",
            engine=db_backend
        )

# Test migration+rollback:
def test_migrate_and_rollback(db_backend):
    # Migrate and rollback to/from progressively higher versions.
    for i in range(3):
        swellow.up(
            db=os.getenv("DB_CONN"),
            directory=f"./tests/{db_backend}/migrate_and_rollback",
            engine=db_backend,
            target_version_id=i+1
        )
        swellow.down(
            db=os.getenv("DB_CONN"),
            directory=f"./tests/{db_backend}/migrate_and_rollback",
            engine=db_backend
        )
