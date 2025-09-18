mod cli;
mod migrations;

use clap::Parser;
use cli::{commands, ux};
use migrations::{db, directory, parser};
use sqlx;


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
