mod cli;
mod db;
mod migrations;

use clap::Parser;
use cli::{commands, ux};
use migrations::{directory, parser};

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
async fn main() -> anyhow::Result<()> {
    let args: cli::Cli = cli::Cli::parse();

    let db_connection_string: String = args.db_connection_string;
    let migration_directory: String = args.migration_directory;
    let mut backend = args.engine.into_backend(db_connection_string).await?;

    ux::setup_logging(args.verbose, args.quiet);

    match args.command {
        cli::Commands::Peck { } => {
            commands::peck(&backend).await?;
        }
        cli::Commands::Up { args } => {
            commands::migrate(
                &mut backend,
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
                &mut backend,
                &migration_directory,
                args.current_version_id,
                args.target_version_id,
                commands::MigrationDirection::Down,
                args.plan,
                args.dry_run
            ).await?;
        }
        cli::Commands::Snapshot { } => {
            commands::snapshot(&mut backend, &migration_directory)?;
        }
    }

    Ok(())
}
