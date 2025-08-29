// Project
mod commands;
mod db;
mod migrations;
mod ux;
// Dependencies
use clap::{Parser, Subcommand};
use sqlx;


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

#[derive(Subcommand)]
enum Commands {
    /// Test connection to DB.
    Peck {},
    Plan {
        #[arg(
            long,
            help = "Migrate up to the specified version ID.\nLargest value possible: 64 bits.",
        )]
        version_id: Option<i64>
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
        Commands::Plan { version_id } => {
            // If no version is specified, set the largest possible number.
            let version_id = version_id.unwrap_or(i64::MAX);

            commands::plan(
                db_connection_string,
                migration_directory,
                version_id
            ).await?;
        }
    }

    Ok(())
}
