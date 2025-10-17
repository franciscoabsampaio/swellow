mod cli;
mod db;
mod parser;

pub use cli::error::SwellowError;
pub use parser::migration;

use clap::Parser;
use cli::{commands, error, output, ux};


async fn run_command(args: cli::Cli) -> output::SwellowOutput<serde_json::Value> {
    let db_connection_string: String = args.db_connection_string;
    let migration_directory: String = args.migration_directory;
    let mut backend = match args.engine.into_backend(db_connection_string).await {
        Ok(b) => b,
        Err(e) => return 
    };

    let command_name = args.command.to_string();

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
                migration::MigrationDirection::Up,
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
                migration::MigrationDirection::Down,
                args.plan,
                args.dry_run
            ).await?;
        }
        cli::Commands::Snapshot { } => {
            commands::snapshot(&mut backend, &migration_directory)?;
        }
    }
}


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
async fn main() {
    let args: cli::Cli = cli::Cli::parse();

    ux::setup_logging(args.verbose, args.quiet);

    let result = run_command(command, migration_directory, backend).await;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result).unwrap());
    } else {
        ux::render_human_output(&result);
    }

    if let SwellowStatus::Error = result.status {
        std::process::exit(1);
    }
}
