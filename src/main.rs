mod cli;
mod migrations;

use clap::Parser;
use cli::{commands, ux};
use migrations::{db, directory, parser};
use sqlx;

/// Entry point for the Swellow CLI tool.
///
/// This program manages database migrations by delegating to subcommands:
/// - `peck`: Verify connectivity to the database.
/// - `up`: Apply migrations forward from the current to target version.
/// - `down`: Revert migrations backward from the current to target version.
/// - `snapshot`: Create a snapshot of the current migration state.
///
/// Arguments such as `--db` and `--dir` are parsed from the command line
/// and passed through to the relevant command handlers.
#[tokio::main]
async fn main() -> sqlx::Result<()> {
    let args: cli::Cli = cli::Cli::parse();

    let db_connection_string: String = args.db_connection_string;
    let migration_directory: String = args.migration_directory;

    ux::setup_logging(args.verbose, args.quiet);

    match args.command {
        cli::Commands::Peck { } => {
            commands::peck(&db_connection_string).await?;
        }
        cli::Commands::Up { args } => {
            commands::migrate(
                &db_connection_string,
                &migration_directory,
                args.current_version_id,
                args.target_version_id,
                commands::MigrationDirection::Up,
                args.plan,
                args.dry_run
            ).await?;
        }
        cli::Commands::Down { args } => {
            commands::migrate(
                &db_connection_string,
                &migration_directory,
                args.current_version_id,
                args.target_version_id,
                commands::MigrationDirection::Down,
                args.plan,
                args.dry_run
            ).await?;
        }
        cli::Commands::Snapshot { } => {
            commands::snapshot(&db_connection_string, &migration_directory)?;
        }
    }

    Ok(())
}
