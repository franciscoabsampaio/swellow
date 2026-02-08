#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# ==========================================
# 1. Configuration & Environment Variables
# ==========================================

# We check if the env var exists; if not, use the default.
# We export them so swellow picks them up automatically.
export DB_CONNECTION_STRING="${DB_CONNECTION_STRING:-postgresql://pguser:pgpass@localhost:5432/mydb}"
export MIGRATION_DIRECTORY="${MIGRATION_DIRECTORY:-./migrations}"
export ENGINE="${ENGINE:-postgres}"

# Helper function for logging with timestamp-ish feel
log() {
    echo -e "\n\033[1;34m$1\033[0m" # Blue text
}

# ==========================================
# 2. Main Execution Flow
# ==========================================

echo "--- Starting Swellow with DB: $DB_CONNECTION_STRING ---"

# [1/7] First time setup: is the DB wired correctly?
log "[1/7] Checking DB connection (Peck)..."
swellow peck

# [2/7] Greenfield migration: apply all migrations up to version 1
log "[2/7] Applying initial migration (v1)..."
swellow up --target-version-id 1

# [3/7] Run plan of migrations up to 4
log "[3/7] Planning migrations up to v4..."
swellow up --target-version-id 4 --plan

# [4/7] Now actually apply migrations up to 4
log "[4/7] Applying migrations up to v4..."
swellow up --target-version-id 4

# [5/7] Complex change: Dry-run of migration up to 5
log "[5/7] Dry-run testing migration v5..."
swellow up --target-version-id 5 --dry-run

# [6/7] Run the actual migration up to 5
log "[6/7] Applying migration v5..."
swellow up --target-version-id 5

# [7/7] Take a snapshot of the current database state
log "[7/7] Taking Snapshot..."
swellow snapshot

# [!] Oops, issues found: Plan the rollback
log "[!] Planning Rollback to v4..."
swellow down --target-version-id 4 --plan

log "Done."