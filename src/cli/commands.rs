use crate::{
    db,
    directory,
    parser::ResourceCollection,
    ux
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use sqlx::{PgPool, Pool, Postgres, Transaction};


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
    db_connection_string: &String
) -> sqlx::Result<Pool<Postgres>> {
    tracing::info!("Pecking database...");
    let pool: Pool<Postgres> = PgPool::connect(&db_connection_string).await?;

    db::ensure_table(&pool).await?;

    tracing::info!("Pecking successful 🐦");

    return Ok(pool)
}


async fn plan(
    db_connection_string: &String,
    migration_directory: &String,
    current_version_id: Option<i64>,
    reference_version_id: Option<i64>,
    direction: &MigrationDirection
) -> sqlx::Result<(
    Transaction<'static, Postgres>,
    Vec<(i64, PathBuf, ResourceCollection)>
)> {
    let pool: Pool<Postgres> = peck(&db_connection_string).await?;
    let mut tx = pool.begin().await?;
    
    // Get latest version in records
    let latest_version_from_records: i64 = db::begin(&mut tx).await?
        .unwrap_or(match direction {
            // If unavailable, set to minimum/maximum
            MigrationDirection::Up => 0,
            MigrationDirection::Down => i64::MAX
        }
    );

    // Set the current migration version (default to user input)
    let current_version_id: i64 = current_version_id
        // If unavailable, get from table records
        .unwrap_or(latest_version_from_records);

    db::disable_records(&mut tx, current_version_id).await?;

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

    Ok((tx, migrations))
}

pub async fn migrate(
    db_connection_string: &String,
    migration_directory: &String,
    current_version_id: Option<i64>,
    target_version_id: Option<i64>,
    direction: MigrationDirection,
    flag_plan: bool,
    flag_dry_run: bool
) -> sqlx::Result<()> {
    let (mut tx, migrations) = plan(
        &db_connection_string,
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
                    db::upsert_record(
                        &mut tx,
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
            db::execute_sql_script(&mut tx, &file_path).await?;

            // Update records' status
            db::update_record(
                &mut tx,
                &direction,
                version_id,
            ).await?;
        }
    }

    if flag_dry_run {
        tx.rollback().await?;
        tracing::info!("Dry run completed.");
    } else {
        tx.commit().await?;
        tracing::info!("Migration completed.");
    }

    Ok(())
}

pub fn snapshot(
    db_connection_string: &String,
    migration_directory: &String
) -> std::io::Result<()> {
    // Check if pg_dump is installed
    if process::Command::new("pg_dump").arg("--version").output()
        .is_err() {
        tracing::error!("pg_dump not installed or not in PATH.");
        std::process::exit(1);
    }

    // Take snapshot
    let output = process::Command::new("pg_dump")
        .arg("--schema-only") // only schema, no data
        .arg("--no-owner")    // drop ownership info
        .arg("--no-privileges")
        .arg(db_connection_string)
        .output()?;

    if !output.status.success() {
        eprintln!("pg_dump failed: {}", String::from_utf8_lossy(&output.stderr));
        std::process::exit(1);
    }

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
    fs::write(new_version_directory.join("up.sql"), &output.stdout)?;
    
    tracing::info!("Snapshot complete! 🐦");
    Ok(())
}