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
    Down
}

impl MigrationDirection {
    // Returns "Migrating" or "Rolling back"
    pub fn verb(&self) -> &'static str {
        match self {
            MigrationDirection::Up => "Migrating",
            MigrationDirection::Down => "Rolling back",
        }
    }
    // Returns "Migration" or "Rollback"
    pub fn noun(&self) -> &'static str {
        match self {
            MigrationDirection::Up => "Migration",
            MigrationDirection::Down => "Rollback",
        }
    }
    // Returns "up.sql" or "down.sql"
    pub fn filename(&self) -> &'static str {
        match self {
            MigrationDirection::Up => "up.sql",
            MigrationDirection::Down => "down.sql",
        }
    }
}


pub async fn peck(
    backend: &db::EngineBackend,
) -> anyhow::Result<()> {
    tracing::info!("Pecking database...");
    
    backend.ensure_table().await?;

    tracing::info!("Pecking successful 🐦");

    return Ok(())
}


async fn plan(
    backend: &mut db::EngineBackend,
    migration_directory: &String,
    current_version_id: Option<i64>,
    reference_version_id: Option<i64>,
    direction: &MigrationDirection
) -> anyhow::Result<Vec<(i64, PathBuf, ResourceCollection)>> {
    peck(backend).await?;

    tracing::info!("Beginning transaction...");
    backend.begin();
    
    // Acquire a lock on the swellow_records table
    // To ensure no other migration process is underway.
    tracing::info!("Acquiring lock on records table...");
    backend.acquire_lock();
    
    // Get latest version in records
    tracing::info!("Getting latest migration version from records...");
    let latest_version_from_records: i64 = backend.fetch_optional_i64("
        SELECT MAX(version_id) version_id
        FROM swellow_records
        WHERE status IN ('APPLIED', 'TESTED')
    ").await?
        .unwrap_or(match direction {
            // If unavailable, set to minimum/maximum
            MigrationDirection::Up => 0,
            MigrationDirection::Down => i64::MAX
        }
    );
    tracing::info!("Latest version resolved from records: {}", latest_version_from_records);

    // Set the current migration version (default to user input)
    let current_version_id: i64 = current_version_id
        // If unavailable, get from table records
        .unwrap_or(latest_version_from_records);

    // Disable records with versions greater than the user-specified starting version
    backend.disable_records(current_version_id);

    // Set direction_string, from_version, and to_version depending on direction
    let (
        from_version,
        to_version
    ) = match direction {
        // Migrate from the last version (excluding) up to the user reference
        MigrationDirection::Up => (
            current_version_id,
            reference_version_id.unwrap_or(i64::MAX),
        ),
        // Migrate from the last version (excluding) down to the user reference
        MigrationDirection::Down => (
            reference_version_id.unwrap_or(0),
            current_version_id
        )
    };
    
    tracing::info!("Loading migrations from directory '{}'...", migration_directory);
    // Get version names in migration_directory.
    let mut migrations = match directory::load_in_interval(
        migration_directory,
        from_version,
        to_version,
        &direction
    ) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // Reverse execution direction if migration direction is down.
    match direction {
        MigrationDirection::Down => migrations.reverse(),
        _ => ()
    }

    // Show user the plans.
    ux::show_migration_changes(&migrations, &direction);

    Ok(migrations)
}

pub async fn migrate(
    backend: &mut db::EngineBackend,
    migration_directory: &String,
    current_version_id: Option<i64>,
    target_version_id: Option<i64>,
    direction: MigrationDirection,
    flag_plan: bool,
    flag_dry_run: bool
) -> anyhow::Result<()> {
    let migrations = plan(
        backend,
        &migration_directory,
        current_version_id,
        target_version_id,
        &direction
    ).await?;

    if flag_plan {
        return Ok(())
    } else {
        for (version_id, version_path, resources) in migrations {
            let file_path: PathBuf = version_path.join(direction.filename());

            if direction == MigrationDirection::Up {
                // Insert a new migration record for every resource
                tracing::info!("Inserting new record for version {}", version_id);
                for resource in resources.iter() {
                    // Skip insertion of doubly NULL records.
                    if resource.name_before == "-1" && resource.name_after == "-1" {
                        continue
                    }
                    backend.upsert_record(
                        &resource.object_type,
                        &resource.name_before,
                        &resource.name_after,
                        version_id,
                        &file_path,
                    ).await?;
                };
            }

            // Execute migration
            tracing::info!(
                "{} to version {}...",
                direction.verb(),
                version_id
            );
            backend.execute_sql_script(&file_path).await?;

            // Update records' status
            backend.update_record(
                &direction,
                version_id,
            ).await?;
        }
    }

    if flag_dry_run {
        backend.rollback().await?;
        tracing::info!("Dry run completed.");
    } else {
        backend.commit().await?;
        tracing::info!("Migration completed.");
    }

    Ok(())
}

pub fn snapshot(
    backend: &mut db::EngineBackend,
    migration_directory: &String
) -> std::io::Result<()> {
    // Take snapshot
    let output = match backend.snapshot() {
        Ok(_out) => _out,
        Err(e) => {
            tracing::error!("Snapshot failed: \n\t{}", e);
            std::process::exit(1);
        }
    };

    // Store to SQL file with the latest possible version.
    // 1) Get latest version.
    let new_version: i64 = match directory::collect_versions_from_directory(
        migration_directory
    ) {
        Ok(v) => v.iter().fold(i64::MIN, |acc, (_, v)| acc.max(*v)) + 1,
        Err(e) => {
            tracing::error!(e);
            std::process::exit(1);
        },
    };
    // Output snapshot SQL script to directory with updated version
    let new_version_directory = Path::new(migration_directory).join(format!("{}_snapshot", new_version));
    fs::create_dir_all(&new_version_directory)?;
    fs::write(new_version_directory.join("up.sql"), output)?;
    
    tracing::info!("Snapshot complete! 🐦");
    Ok(())
}