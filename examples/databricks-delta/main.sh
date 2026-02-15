#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# ==========================================
# 1. Configuration & Environment Variables
# ==========================================

# We check if the env var exists; if not, use the default.
# We export them so swellow picks them up automatically.
export DATABRICKS_HOST="${DATABRICKS_HOST}"
export DATABRICKS_TOKEN="${DATABRICKS_TOKEN}"
export DATABRICKS_CLUSTER_ID="${DATABRICKS_CLUSTER_ID}"
# export DATABRICKS_SESSION_ID="${DATABRICKS_SESSION_ID}"

DB_CONNECTION_STRING="sc://${DATABRICKS_HOST}:443/;use_ssl=true;token=${DATABRICKS_TOKEN};"
export DB_CONNECTION_STRING="${DB_CONNECTION_STRING}x-databricks-cluster-id=${DATABRICKS_CLUSTER_ID};"
# export DB_CONNECTION_STRING="${DB_CONNECTION_STRING}x-databricks-session-id=${DATABRICKS_SESSION_ID};"

export MIGRATION_DIRECTORY="${MIGRATION_DIRECTORY:-./migrations}"
export ENGINE="${ENGINE:-spark-delta}"

# Helper function for logging with timestamp-ish feel
log() {
    echo -e "\n\033[1;34m$1\033[0m" # Blue text
}

# ==========================================
# 2. Main Execution Flow
# ==========================================

echo "--- Starting Swellow with DB: $DB_CONNECTION_STRING ---"

# First time setup: is the DB wired correctly?
log "[1/8] Checking DB connection (Peck)..."
swellow peck

# Greenfield migration: apply all migrations up to version 1
log "[2/8] Applying initial migration (v1)..."
swellow up --target-version-id 1

# Run plan of migrations up to v3
log "[3/8] Planning migrations up to v3..."
swellow up --target-version-id 3 --plan

# Now actually apply migrations up to 3
log "[4/8] Applying migrations up to v3..."
swellow up --target-version-id 3

# Dry runs are not supported on spark-delta, so this would run into an error.
log "[5/8] Skipping dry-run of migration v4..."
# swellow up --target-version-id 4 --dry-run

# Run the actual migration up to 4
log "[6/8] Applying migration v4..."
swellow up --target-version-id 4

# Take a snapshot of the current database state
log "[7/8] Taking Snapshot..."
swellow snapshot

# Oops, issues found: Plan the rollback
log "[8/8] Planning Rollback to v3..."
swellow down --target-version-id 3 --plan

log "Done."