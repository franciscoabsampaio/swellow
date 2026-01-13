use crate::cli::error::SwellowErrorKind;
use crate::{db, parser, ux};
use crate::SwellowError;
use crate::migration::{MigrationDirection, MigrationCollection};
use crate::parser::ReferenceToStaticDialect;
use std::fs;
use std::path::Path;


/// Ensures the database is initialized and the migration table exists.
pub async fn peck(backend: &mut db::EngineBackend) -> Result<(), SwellowError> {
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
) -> Result<MigrationCollection, SwellowError> {
    // Determine current migration version
    let latest_version_from_records = backend
        .fetch_optional_i64(
            "SELECT MAX(version_id) AS version_id
             FROM swellow.records
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
    if from_version > to_version {
        return Err(SwellowError { kind: SwellowErrorKind::InvalidVersionInterval(from_version, to_version) })
    };

    tracing::info!("Loading migrations from '{migration_dir}'");
    let migrations = MigrationCollection::from_directory(
        ReferenceToStaticDialect::from(backend),
        migration_dir,
        direction,
        from_version,
        to_version,
    )?;

    ux::show_migration_changes(&migrations, direction)?;

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
    flag_no_transaction: bool,
    flag_ignore_locks: bool,
) -> Result<(), SwellowError> {
    // --dry-run cannot be set together with --no-transaction
    if flag_dry_run && flag_no_transaction {
        return Err(SwellowError { kind: SwellowErrorKind::DryRunRequiresTransaction });
    }
    // --dry-run is not supported for Spark engines
    if flag_dry_run && matches!(backend, db::EngineBackend::SparkDelta(_) | db::EngineBackend::SparkIceberg(_)) {
        return Err(SwellowError { kind: SwellowErrorKind::DryRunUnsupportedEngine(backend.engine()) });
    }

    peck(backend).await?;

    if flag_no_transaction {
        tracing::info!("Running outside transaction...");
    } else {
        tracing::info!("Beginning transaction...");
        backend.begin().await?;
    }

    // Acquire a lock on the swellow.records table
    // To ensure no other migration process is underway.
    if flag_ignore_locks {
        tracing::warn!("⚠️ Ignoring locks: sequential execution of migrations is not guaranteed.");
    } else {
        tracing::info!("Acquiring lock on records table...");
        backend.acquire_lock().await?;
    }

    let migrations = plan(
        backend,
        migration_dir,
        current_version_id,
        target_version_id,
        &direction,
    ).await?;

    if flag_plan {
        tracing::info!("Planning complete - no migrations executed 🐦");
        return Ok(());
    }

    for (version_id, migration) in migrations.iter() {
        tracing::info!("{} to version {}...", direction.verb(), version_id);

        if direction == MigrationDirection::Up {
            // Insert a new migration record for every resource
            tracing::info!("Inserting migration records for version {version_id}");
            for resource in migration.resources().iter() {
                // Skip invalid placeholder records (double NULLs)
                if resource.name_before == "-1" && resource.name_after == "-1" {
                    continue;
                }
                backend.upsert_record(
                    &resource.object_type,
                    &resource.name_before,
                    &resource.name_after,
                    *version_id,
                    &migration.statements.checksum().to_string(),
                ).await?;
            }
        }

        // Execute migration
        // There is a risk of execution of parseable statements without parseable resources.
        // This *should* be fine, since the user is the one who specified the migration code.
        // But, of course, it does create some drift between the UX output, the records,
        // and the SQL that was effectively executed.
        // Effectively, the priority is parseable statements - not resources.
        for stmt in &migration.statements {
            backend.execute(&stmt.to_string()).await?;
        }
        // Update records' status
        backend.update_record(&direction, *version_id).await?;
    }

    if flag_no_transaction {
        tracing::info!("Migration completed 🐦");
    } else {
        if flag_dry_run {
            backend.rollback().await?;
            tracing::info!("Dry run completed - transaction successfully rolled back 🐦");
        } else {
            backend.commit().await?;
            tracing::info!("Migration completed - transaction successfully committed 🐦");
        }
    }

    Ok(())
}

/// Takes a snapshot of the current database schema and stores it as a new migration.
pub async fn snapshot(
    backend: &mut db::EngineBackend,
    migration_dir: &str
) -> Result<(), SwellowError> {
    peck(backend).await?;

    tracing::info!("Taking database snapshot...");

    let output = backend.snapshot().await?;

    // Store to SQL file with the latest possible version.
    // Get latest version.
    let new_version = parser::collect_versions_from_directory(
        migration_dir,
        i64::MIN,
        i64::MAX,
        false
    )?
        .iter()
        .fold(0, |acc, (v, _)| acc.max(*v) + 1);

    // Output snapshot SQL script to directory with updated version
    let new_version_directory = Path::new(migration_dir).join(format!("{}_snapshot", new_version));
    fs::create_dir_all(&new_version_directory)
        .map_err(|source| {
            SwellowError { kind: SwellowErrorKind::IoDirectoryCreate { source, path: new_version_directory.clone() } }
        })?;
    fs::write(new_version_directory.join("up.sql"), output)
        .map_err(|source| {
            SwellowError { kind: SwellowErrorKind::IoFileWrite { source, path: new_version_directory.join("up.sql") } }
        })?;

    tracing::info!("Snapshot created at version {} 🐦", new_version);
    Ok(())
}
