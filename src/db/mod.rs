mod odbc;
mod postgres;
pub use odbc::{OdbcEngine, OdbcCatalog};
pub use postgres::PostgresEngine;

pub enum EngineBackend {
    Postgres(PostgresEngine),
    SparkDelta(OdbcEngine),
    SparkIceberg(OdbcEngine),
}

impl EngineBackend {
    pub async fn ensure_table(&self) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.ensure_table().await,
            EngineBackend::SparkDelta(engine) => engine.ensure_table().await,
            EngineBackend::SparkIceberg(engine) => engine.ensure_table().await,
        }
    }
}

// #[async_trait::async_trait]
pub trait DbEngine {
    async fn ensure_table(&self) -> anyhow::Result<()>;
    async fn begin(&mut self) -> anyhow::Result<Option<i64>>;
}
