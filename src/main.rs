// src/main.rs
use anyhow::{Result};
use clap::{Parser, Subcommand};
use sqlx::{Pool, Postgres, PgPool};

#[derive(Parser)]
#[command(name = "swellow", version, about = "Database migration tool in Rust.")]
struct Cli {
    /// Database connection URL (e.g., postgres://user:pass@localhost/db)
    #[arg(
        short,
        long,
        help = "Database connection string",
        env = "DB_CONNECTION_STRING",
        hide_env_values = true
    )]
    db_connection_string: String,

    /// Directory containing all migrations.
    #[arg(
        short,
        long,
        help = "Database connection string",
        env = "DB_CONNECTION_STRING",
        hide_env_values = true
    )]
    db_connection_string: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Test connection to DB.
    Peck {}
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Cli = Cli::parse();

    let db_connection_string: &String = &args.db_connection_string;

    match args.command {
        Commands::Peck { } => {
            println!("Pecking database...");
            let _: Pool<Postgres> = PgPool::connect(&db_connection_string).await?;
        }
    }

    Ok(())
}
