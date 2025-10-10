use crate::{
    db,
    directory,
    parser::ResourceCollection,
    ux
};
use std::fs;
use std::path::{Path, PathBuf};


#[derive(PartialEq)]
pub enum MigrationDirection {
    Up,
    Down,
}

impl MigrationDirection {
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Up => "Migrating",
            Self::Down => "Rolling back",
        }
    }
    pub fn noun(&self) -> &'static str {
        match self {
            Self::Up => "Migration",
            Self::Down => "Rollback",
        }
    }
    pub fn filename(&self) -> &'static str {
        match self {
            Self::Up => "up.sql",
            Self::Down => "down.sql",
        }
    }
}

/// Ensures the database is initialized and the migration table exists.
pub async fn peck(backend: &db::EngineBackend) -> anyhow::Result<()> {
    tracing::info!("Pecking database...");
    backend.ensure_table().await?;
    tracing::info!("Pecking successful 🐦");

    Ok(())
}

/// Builds the migration plan: determines versions and loads relevant scripts.
async fn plan(
    backend: &mut db::EngineBackend,
    migration_dir: &str,
    current_version_id: Option<i64>,
    target_version_id: Option<i64>,
    direction: &MigrationDirection,
) -> anyhow::Result<Vec<(i64, PathBuf, ResourceCollection)>> {
    peck(backend).await?;

    tracing::info!("Comencing transaction...");
    backend.begin().await?;

    // Acquire a lock on the swellow_records table
    // To ensure no other migration process is underway.
    tracing::info!("Acquiring lock on records table...");
    backend.acquire_lock().await?;

    // Determine current migration version
    let latest_version_from_records = backend
        .fetch_optional_i64(
            "SELECT MAX(version_id) AS version_id
             FROM swellow_records
             WHERE status IN ('APPLIED', 'TESTED')",
        )
        .await?
        .unwrap_or_else(|| match direction {
            MigrationDirection::Up => 0,
            MigrationDirection::Down => i64::MAX,
        });

    // Set the current migration version (default to user input)
    let current_version: i64 = current_version_id
        // If unavailable, get from table records
        .unwrap_or(latest_version_from_records);
    tracing::info!("Current version resolved: {current_version}");
    
    // Disable records with versions greater than the user-specified starting version
    backend.disable_records(current_version).await?;

    // Set direction_string, from_version, and to_version depending on direction
    let (from_version, to_version) = match direction {
        // Migrate from the last version (excluding) up to the user reference
        MigrationDirection::Up => (
            current_version,
            target_version_id.unwrap_or(i64::MAX)
        ),
        // Migrate from the last version (excluding) down to the user reference
        MigrationDirection::Down => (
            target_version_id.unwrap_or(0),
            current_version
        ),
    };

    tracing::info!("Loading migrations from '{migration_dir}'");
    // Get version names in migration_directory.
    let mut migrations = directory::load_in_interval(
        migration_dir,
        from_version,
        to_version,
        direction,
        backend,
    )
    .map_err(|e| {
        tracing::error!("Error loading migrations: {}", e);
        std::process::exit(1);
    })?;

    // Reverse execution direction if migration direction is down.
    if *direction == MigrationDirection::Down {
        migrations.reverse();
    }

    ux::show_migration_changes(&migrations, direction);
    Ok(migrations)
}

/// Executes migrations or rollbacks according to the provided plan and flags.
pub async fn migrate(
    backend: &mut db::EngineBackend,
    migration_dir: &str,
    current_version_id: Option<i64>,
    target_version_id: Option<i64>,
    direction: MigrationDirection,
    flag_plan: bool,
    flag_dry_run: bool,
) -> anyhow::Result<()> {
    let migrations = plan(
        backend,
        migration_dir,
        current_version_id,
        target_version_id,
        &direction,
    ).await?;

    if flag_plan {
        tracing::info!("Planning complete - no migrations executed.");
        return Ok(());
    }

    for (version_id, version_path, resources) in migrations {
        let file_path = version_path.join(direction.filename());
        tracing::info!("{} to version {}...", direction.verb(), version_id);

        if direction == MigrationDirection::Up {
            // Insert a new migration record for every resource
            tracing::info!("Inserting migration records for version {version_id}");
            for resource in resources.iter() {
                // Skip invalid placeholder records (double NULLs)
                if resource.name_before == "-1" && resource.name_after == "-1" {
                    continue;
                }
                backend.upsert_record(
                    &resource.object_type,
                    &resource.name_before,
                    &resource.name_after,
                    version_id,
                    &file_path,
                ).await?;
            }
        }

        // Execute migration
        backend.execute_sql_script(&file_path).await?;
        // Update records' status
        backend.update_record(&direction, version_id).await?;
    }

    if flag_dry_run {
        backend.rollback().await?;
        tracing::info!("Dry run completed - transaction successfully rolled back.");
    } else {
        backend.commit().await?;
        tracing::info!("Migration completed - transaction successfully committed.");
    }

    Ok(())
}

/// Takes a snapshot of the current database schema and stores it as a new migration.
pub fn snapshot(backend: &mut db::EngineBackend, migration_dir: &str) -> anyhow::Result<()> {
    tracing::info!("Taking database snapshot...");

    let output = backend.snapshot().map_err(|e| {
        tracing::error!("Snapshot failed: {}", e);
        std::process::exit(1);
    })?;

    // Store to SQL file with the latest possible version.
    // 1) Get latest version.
    let new_version = directory::collect_versions_from_directory(migration_dir)
        .map(|versions| versions.iter().fold(i64::MIN, |acc, (_, v)| acc.max(*v)) + 1)
        .unwrap_or_else(|e| {
            tracing::error!("Failed to collect versions: {}", e);
            std::process::exit(1);
        });

    // Output snapshot SQL script to directory with updated version
    let new_version_directory = Path::new(migration_dir).join(format!("{}_snapshot", new_version));
    fs::create_dir_all(&new_version_directory)?;
    fs::write(new_version_directory.join("up.sql"), output)?;

    tracing::info!("Snapshot created at version {} 🐦", new_version);
    Ok(())
}
