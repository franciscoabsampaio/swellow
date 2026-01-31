import os
import sys
import swellow


def main():
    # 1. Retrieve configuration from Environment Variables
    db_conn = os.getenv("DB_CONNECTION_STRING", "postgresql://pguser:pgpass@localhost:5432/mydb")
    mig_dir = os.getenv("MIGRATION_DIRECTORY", "./migrations")
    
    # 2. Define common arguments to keep code DRY
    base_config = {
        "db": db_conn,
        "directory": mig_dir,
        "engine": "postgres",
        "json": False
    }

    print(f"--- Starting Swellow with DB: {db_conn} ---")

    # First time setup: is the DB wired correctly?
    print("\n[1/7] Checking DB connection (Peck)...")
    swellow.peck(**base_config)

    # Greenfield migration: apply all migrations up to version 1
    print("\n[2/7] Applying initial migration (v1)...")
    swellow.up(**base_config, target_version_id=1)

    # Run plan of migrations up to 4
    print("\n[3/7] Planning migrations up to v4...")
    swellow.up(**base_config, target_version_id=4, plan=True)

    # Now actually apply migrations up to 4
    print("\n[4/7] Applying migrations up to v4...")
    swellow.up(**base_config, target_version_id=4)

    # We have a complex change coming up: Dry-run of migration up to 5
    print("\n[5/7] Dry-run testing migration v5...")
    swellow.up(**base_config, target_version_id=5, dry_run=True)

    # Run the actual migration up to 5
    print("\n[6/7] Applying migration v5...")
    swellow.up(**base_config, target_version_id=5)

    # Take a snapshot of the current database state
    print("\n[7/7] Taking Snapshot...")
    swellow.snapshot(**base_config)

    # Oops, the last migration caused issues: Plan the rollback
    print("\n[!] Planning Rollback to v4...")
    swellow.down(**base_config, target_version_id=4, plan=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError occurred: {e}", file=sys.stderr)
        sys.exit(1)