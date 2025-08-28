// Project
mod commands;
mod db;
// Dependencies
use anyhow::{Result};
use clap::{Parser, Subcommand};
use sqlx::{Pool, Postgres, PgPool};


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

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Test connection to DB.
    Peck {},
    Plan {}
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Cli = Cli::parse();

    let db_connection_string: &String = &args.db_connection_string;
    let migration_directory: &String = &args.migration_directory;

    match args.command {
        Commands::Peck { } => {
            commands::peck(db_connection_string).await?;
        }
        Commands::Plan { } => {
            let records = commands::plan(db_connection_string).await?;
            println!("{:#?}", records)
        }
    }

    Ok(())
}
