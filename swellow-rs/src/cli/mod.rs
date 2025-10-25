pub mod commands;
pub mod error;
pub mod output;
pub mod ux;

use crate::db;
pub use clap::{Parser, Subcommand, ValueEnum};


/// User-facing enum to select engine
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum Engine {
    Postgres,
    SparkDelta,
    SparkIceberg,
}

impl Engine {
    pub async fn into_backend(self, conn_str: String) -> Result<db::EngineBackend, db::EngineError> {
        match self {
            Engine::Postgres => Ok(db::EngineBackend::Postgres(db::PostgresEngine::new(&conn_str))),
            Engine::SparkDelta => Ok(db::EngineBackend::SparkDelta(db::SparkEngine::new(&conn_str, db::SparkCatalog::Delta).await?)),
            Engine::SparkIceberg => Ok(db::EngineBackend::SparkIceberg(db::SparkEngine::new(&conn_str, db::SparkCatalog::Iceberg).await?)),
        }
    }
}


#[derive(Parser)]
#[command(name = "swellow", version, about = "Swellow is the simple, SQL-first tool for managing table migrations, written in Rust.")]
pub struct Cli {
    #[arg(
        long = "db",
        help = "Database connection string. Please follow your database's recommended format, e.g.:
    postgresql://<username>:<password>@<host>:<port>/<database>\n",
        env = "DB_CONNECTION_STRING",
        hide_env_values = true
    )]
    pub db_connection_string: String,

    #[arg(
        long = "dir",
        help = "Directory containing all migrations",
        env = "MIGRATION_DIRECTORY",
    )]
    pub migration_directory: String,

    #[arg(
        long = "engine",
        value_enum,
        help = "Database / catalog engine.",
        default_value_t = Engine::Postgres,
        env = "ENGINE",
    )]
    pub engine: Engine,

    #[arg(
        short,
        long,
        action = clap::ArgAction::Count,
        help = "Set level of verbosity. [default: INFO]\n\t-v: DEBUG\n\t-vv: TRACE\n--quiet takes precedence over --verbose."
    )]
    pub verbose: u8,

    #[arg(
        short,
        long,
        action = clap::ArgAction::SetTrue,
        help = "Disable all information logs (only ERROR level logs are shown).\n--quiet takes precedence over --verbose."
    )]
    pub quiet: bool,

    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable JSON output format. Human readable output is disabled when this flag is set."
    )]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Parser)]
pub struct SwellowArgs {
    #[arg(
        long,
        help = "Specify the database's latest migration version ID.
Any existing records with a larger version ID will be set to disabled.
If not set, swellow will assume the current version to be the last enabled record.
If no record is enabled, swellow will assume the current version to be 0.",
    )]
    pub current_version_id: Option<i64>,

    #[arg(
        long,
        help = "Migrate up/down to the specified version ID.\nOnly numbers up to 64 bits.",
    )]
    pub target_version_id: Option<i64>,

    #[arg(
        long,
        help = "Generate the migration and skip execution.",
    )]
    pub plan: bool,
    
    #[arg(
        long,
        help = "Generate the migration, execute it, then rollback the transaction.",
    )]
    pub dry_run: bool,

    #[arg(
        long,
        help = "Ignore acquiring locks. ⚠️ Warning: sequential execution of migrations is not guaranteed when this flag is set.",
    )]
    pub ignore_locks: bool,
}

#[derive(Subcommand)]
pub enum Commands {
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

    #[command(about = "Take a snapshot of the database schema into a set of CREATE statements.
Automatically creates a new version migration subdirectory like '<VERSION>_snapshot'.
⚠️ Postgres: pg_dump must be installed with a version matching the server's.")]
    Snapshot {}
}

impl std::fmt::Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Commands::Peck { .. } => "peck",
            Commands::Up { .. } => "up",
            Commands::Down { .. } => "down",
            Commands::Snapshot { .. } => "snapshot",
        };
        write!(f, "{name}")
    }
}