import docker
import pytest
import time


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