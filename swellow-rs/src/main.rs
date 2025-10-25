mod cli;
mod db;
mod parser;

pub use cli::error::SwellowError;
pub use parser::migration;

use clap::Parser;
use cli::{commands, error, output, ux};
use output::{SwellowOutput, SwellowStatus};
use serde_json::Value;


async fn run_command(args: &cli::Cli) -> output::SwellowOutput<serde_json::Value> {
    let db_connection_string: String = args.db_connection_string.clone();
    let migration_directory: String = args.migration_directory.clone();

    let command_name = args.command.to_string();

    let mut backend = match args.engine.into_backend(db_connection_string).await {
        Ok(b) => b,
        Err(e) => return SwellowOutput {
            command: command_name,
            status: SwellowStatus::Error,
            data: None,
            error: Some((&e).into()),
        }
    };

    let output: SwellowOutput<Value> = match &args.command {
        cli::Commands::Peck { } => SwellowOutput::from_result(
            "peck",
            commands::peck(&backend).await
        ),
        cli::Commands::Up { args } => SwellowOutput::from_result(
            "up",
            commands::migrate(
                &mut backend,
                &migration_directory,
                args.current_version_id,
                args.target_version_id,
                migration::MigrationDirection::Up,
                args.plan,
                args.dry_run,
                args.ignore_locks,
            ).await
        ),
        cli::Commands::Down { args } => SwellowOutput::from_result(
            "down",
            commands::migrate(
                &mut backend,
                &migration_directory,
                args.current_version_id,
                args.target_version_id,
                migration::MigrationDirection::Down,
                args.plan,
                args.dry_run,
                args.ignore_locks,
            ).await
        ),
        cli::Commands::Snapshot { } => SwellowOutput::from_result(
            "snapshot",
            commands::snapshot(
                &mut backend,
                &migration_directory
            )
        ),
    };

    match backend.release_lock().await {
        Ok(_) => output,
        Err(e) => SwellowOutput {
            command: command_name,
            status: SwellowStatus::Error,
            data: None,
            error: Some((&e).into()),
        },
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

    ux::setup_logging(args.verbose, args.quiet, args.json);

    let result = run_command(&args).await;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result).unwrap());
    }

    if let SwellowStatus::Error = result.status {
        tracing::error!("'swellow {}' failed: {:?}", result.command, result.error.unwrap());
        std::process::exit(1);
    }
}
