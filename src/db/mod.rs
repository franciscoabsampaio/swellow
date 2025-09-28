pub mod backend;
pub use backend::{DbEngine, PostgresEngine, OdbcEngine, OdbcCatalog};

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