use crate::{db, migrations::{self, Resource}, ux, MigrationDirection, SwellowArgs};

use std::path::PathBuf;
use sqlx::{PgPool, Pool, Postgres, Transaction};


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
    direction: MigrationDirection
) -> sqlx::Result<(Transaction<'static, Postgres>, Vec<(i64, PathBuf, Vec<Resource>)>)> {
    tracing::info!("Connecting to the database...");
    let pool: Pool<Postgres> = peck(&db_connection_string).await?;
    let mut tx = pool.begin().await?;
    let records = db::begin(&mut tx).await?;

    // Get latest version in records
    let latest_version_from_records = records
        .iter()
        .map(|m| m.migration_version_id)
        .max()
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
    let mut migrations = match migrations::load_in_interval(
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
    args: SwellowArgs,
    direction: MigrationDirection
) -> sqlx::Result<()> {
    let (tx, migrations) = plan(
        &db_connection_string,
        &migration_directory,
        args.current_version_id,
        args.target_version_id,
        direction
    ).await?;

    if args.plan {
        return Ok(())
    }

    if args.dry_run {
        tx.rollback().await?;
        tracing::info!("Dry run completed.");
    } else {
        tx.commit().await?;
        tracing::info!("Migration completed.");
    }

    Ok(())
}