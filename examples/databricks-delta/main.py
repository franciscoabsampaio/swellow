import os
import sys
import swellow


def main():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    # session_id = os.getenv("DATABRICKS_SESSION_ID")

    db_connection_string = (
        f"sc://{host}:443/;"
        "use_ssl=true;"
        f"token={token};"
        f"x-databricks-cluster-id={cluster_id};"
        # f"x-databricks-session-id={session_id};"
    )

    # 2. Define common arguments to keep code DRY
    base_config = {
        "db": db_connection_string,
        "directory": "./migrations",
        "engine": "spark-delta",
        "json": False
    }

    print(f"--- Starting Swellow with DB: {db_connection_string} ---")

    # First time setup: is the DB wired correctly?
    print("\n[1/7] Checking DB connection (Peck)...")
    swellow.peck(**base_config)

    # Greenfield migration: apply all migrations up to version 1
    print("\n[2/7] Applying initial migration (v1)...")
    swellow.up(**base_config, target_version_id=1)

    # Run plan of migrations up to 3
    print("\n[3/7] Planning migrations up to v3...")
    swellow.up(**base_config, target_version_id=3, plan=True)

    # Now actually apply migrations up to 3
    print("\n[4/7] Applying migrations up to v3...")
    swellow.up(**base_config, target_version_id=3)

    # Dry runs are not supported on spark-delta, so this would run into an error.
    print("\n[5/7] Skipping dry-run of migration v4...")
    # swellow.up(**base_config, target_version_id=4, dry_run=True)

    # Run the actual migration up to 4
    print("\n[6/7] Applying migration v4...")
    swellow.up(**base_config, target_version_id=4)

    # Take a snapshot of the current database state
    print("\n[7/7] Taking Snapshot...")
    swellow.snapshot(**base_config)

    # Oops, the last migration caused issues: Plan the rollback
    print("\n[!] Planning Rollback to v3...")
    swellow.down(**base_config, target_version_id=3, plan=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError occurred: {e}", file=sys.stderr)
        sys.exit(1)