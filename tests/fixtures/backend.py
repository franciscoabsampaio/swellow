import docker
import os
from pathlib import Path
import pytest
import swellow
import time


def get_pg_version_from_file(path="PG_VERSION"):
    return Path(path).read_text().strip()


def wait_for_log(container, message, timeout=30):
    start = time.time()
    while True:
        logs = container.logs().decode("utf-8")
        if message in logs:
            return
        if time.time() - start > timeout:
            raise TimeoutError(f"Message '{message}' not found in logs")
        time.sleep(0.5)


def start_container(
    image_name: str,
    image_tag: str,
    dict_env_variables: dict[str, str] = None,
    dict_ports: dict[str, int] = None
):
    docker_client = docker.from_env()

    image = docker_client.images.pull(image_name, tag=image_tag)
    container = docker_client.containers.run(
        image,
        detach=True,
        environment=dict_env_variables,
        ports=dict_ports,
    )

    return Backend(container.stop)


class Backend():
    def __init__(self, callback: callable):
        self.clean_up = callback


@pytest.fixture(
    scope="function",
    params=[ # (engine, flag_no_transaction)
        ("postgres", True),
        ("postgres", False),
        ("spark-delta", False),
        ("spark-iceberg", False),
        # TODO: Enable databricks-delta tests in CI.
        # Requires setting up a cluster. 
        # ("databricks-delta", False),
    ],
    # Set custom ids for better readability in test outputs
    ids=lambda p: f"{p[0]}-no_tx={p[1]}",
)
def db_backend(request):
    backend_name = request.param[0]

    if backend_name == "postgres":
        backend = start_container(
            image_name="postgres",
            image_tag=get_pg_version_from_file(),
            dict_env_variables={
                "POSTGRES_USER": "pguser",
                "POSTGRES_PASSWORD": "pgpass",
                "POSTGRES_DB": "mydb"
            },
            dict_ports={'5432/tcp': 5432}
        )
        time.sleep(5)
        conn_url = "postgresql://pguser:pgpass@localhost:5432/mydb"

    elif backend_name in ["spark-delta", "spark-iceberg"]:
        backend = start_container(
            image_name="franciscoabsampaio/spark-connect-server",
            image_tag=backend_name.split('-')[-1],
            dict_ports={'15002/tcp': 15002}
        )
        wait_for_log(backend, message="Spark Connect server started at:")
        conn_url = "sc://localhost:15002"
    
    elif backend_name.startswith('databricks'):
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        # session_id = os.getenv("DATABRICKS_SESSION_ID")

        conn_url = (
            f"sc://{host}:443/;"
            "use_ssl=true;"
            f"token={token};"
            f"x-databricks-cluster-id={cluster_id};"
            # f"x-databricks-session-id={session_id};"
        )
        
        def clean_up():
            swellow.down(
                conn_url,
                './tests/fixtures/databricks',
                engine=backend_name,
                target_version_id=0
            )

        backend = Backend(clean_up)

    else:
        raise ValueError(f"Unknown backend {backend_name}")

    yield {
        'conn_url': conn_url,
        'directory': f"./tests/migrations/{backend_name.replace('databricks', 'spark')}",
        'engine': backend_name,
        'flag_no_transaction': request.param[1],
    }

    backend.clean_up()
