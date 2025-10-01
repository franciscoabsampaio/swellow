import os
import pytest
import swellow
from testcontainers.postgres import PostgresContainer


postgres = PostgresContainer("postgres:latest")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)
    os.environ["DB_CONN"] = postgres.get_connection_url()

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
