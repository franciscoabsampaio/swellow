import os
import pytest
import swellow
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="module", params=["postgres", "spark-delta", "spark-iceberg"])
def db_backend(request):
    backend = request.param

    if backend == "postgres":
        container = PostgresContainer("postgres:15")
        container.start()
        conn_url = container.get_connection_url()

    elif backend == "spark-delta":
        container = (
            DockerContainer("bitnami/spark:latest")
            .with_exposed_ports(10000)  # Hive Thrift Server port
            .with_env("SPARK_MODE", "thrift-server")
            .with_env("SPARK_PACKAGES", "io.delta:delta-core_2.12:2.4.0")
        )
        container.start()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(10000)
        conn_url = f"jdbc:hive2://{host}:{port}/default"

    elif backend == "spark-iceberg":
        container = (
            DockerContainer("bitnami/spark:latest")
            .with_exposed_ports(10000)
            .with_env("SPARK_MODE", "thrift-server")
            .with_env("SPARK_PACKAGES", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
        )
        container.start()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(10000)
        conn_url = f"jdbc:hive2://{host}:{port}/default"

    else:
        raise ValueError(f"Unknown backend {backend}")

    os.environ["DB_CONN"] = conn_url

    yield backend

    container.stop()

# TODO: Test lock already exists

# Test missing up
def test_missing_up():
    with pytest.raises(FileNotFoundError):
        swellow.up(
            db=os.getenv("DB_CONN"),
            directory="./tests/migrations/missing_up"
        )

# Test missing down
def test_missing_down():
    swellow.up(
        db=os.getenv("DB_CONN"),
        directory="./tests/migrations/missing_down"
    )
    with pytest.raises(FileNotFoundError):
        swellow.down(
            db=os.getenv("DB_CONN"),
            directory="./tests/migrations/missing_down"
        )

# Test migration+rollback:
def test_migrate_and_rollback():
    # Migrate and rollback to/from progressively higher versions.
    for i in range(3):
        swellow.up(
            db=os.getenv("DB_CONN"),
            directory="./tests/migrations/migrate_and_rollback",
            target_version_id=i+1
        )
        swellow.down(
            db=os.getenv("DB_CONN"),
            directory="./tests/migrations/migrate_and_rollback"
        )
