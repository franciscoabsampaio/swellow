// Project
mod commands;
mod db;
mod migration_directory;
mod postgres;
mod ux;
// Dependencies
use clap::{Parser, Subcommand};
use sqlx;
use std::fmt;


#[derive(Parser)]
#[command(name = "swellow", version, about = "Database migration tool in Rust.")]
struct Cli {
    #[arg(
        long = "db",
        help = "Database connection string. Please follow the following format:
    postgresql://<username>:<password>@<host>:<port>/<database>\n",
        env = "DB_CONNECTION_STRING",
        hide_env_values = true
    )]
    db_connection_string: String,

    #[arg(
        long = "dir",
        help = "Directory containing all migrations",
        env = "MIGRATION_DIRECTORY",
    )]
    migration_directory: String,

    #[arg(
        short,
        long,
        action = clap::ArgAction::Count,
        help = "Set level of verbosity. [default: INFO]\n\t-v: DEBUG\n\t-vv: TRACE\n--quiet takes precedence over --verbose."
    )]
    verbose: u8,

    #[arg(
        short,
        long,
        action = clap::ArgAction::SetTrue,
        help = "Disable all information logs (only ERROR level logs are shown).\n--quiet takes precedence over --verbose."
    )]
    quiet: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
struct SwellowArgs {
    #[arg(
        long,
        help = "Specify the database's latest migration version ID.
Any existing records with a larger version ID will be set to disabled.
If not set, swellow will assume the current version to be the last enabled record.
If no record is enabled, swellow will assume the current version to be 0.",
    )]
    current_version_id: Option<i64>,

    #[arg(
        long,
        help = "Migrate up/down to the specified version ID.\nOnly numbers up to 64 bits.",
    )]
    target_version_id: Option<i64>,

    #[arg(
        long,
        help = "Generate the migration and skip execution.",
    )]
    plan: bool,
    
    #[arg(
        long,
        help = "Generate the migration, execute it, then rollback the transaction.",
    )]
    dry_run: bool,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Test connection to the database.")]
    Peck {},

    #[command(about = "Generate a migration plan and execute it.")]
    Up {
        #[command(flatten)]
        args: SwellowArgs,
    },
    #[command(about = "Generate a rollback plan and execute it.")]
    Down {
        #[command(flatten)]
        args: SwellowArgs,
    },

    #[command(about = "Use pg_dump to take a snapshot of the database schema into a set of CREATE statements.
Automatically creates a new version migration subdirectory like '<VERSION>_snapshot'.
⚠️  pg_dump must be installed with a version matching the server's.")]
    Snapshot {}
}

enum MigrationDirection {
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

#[tokio::main]
async fn main() -> sqlx::Result<()> {
    let args: Cli = Cli::parse();

    let db_connection_string: &String = &args.db_connection_string;
    let migration_directory: &String = &args.migration_directory;

    ux::setup_logging(args.verbose, args.quiet);

    match args.command {
        Commands::Peck { } => {
            commands::peck(db_connection_string).await?;
        }
        Commands::Up { args } => {
            commands::migrate(
                db_connection_string,
                migration_directory,
                args,
                MigrationDirection::Up
            ).await?;
        }
        Commands::Down { args } => {
            commands::migrate(
                db_connection_string,
                migration_directory,
                args,
                MigrationDirection::Down
            ).await?;
        }
        Commands::Snapshot { } => {
            commands::snapshot(db_connection_string, migration_directory)?;
        }
    }

    Ok(())
}
