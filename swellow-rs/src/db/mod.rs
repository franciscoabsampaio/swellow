mod spark;
mod postgres;
pub use postgres::PostgresEngine;
pub use spark::{SparkEngine, SparkCatalog};

use crate::{commands::MigrationDirection, parser::StatementCollection};

use sqlparser;


pub enum EngineBackend {
    Postgres(PostgresEngine),
    SparkDelta(SparkEngine),
    SparkIceberg(SparkEngine),
}

impl EngineBackend {
    pub async fn ensure_table(&self) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.ensure_table().await,
            EngineBackend::SparkDelta(engine) => engine.ensure_table().await,
            EngineBackend::SparkIceberg(engine) => engine.ensure_table().await,
        }
    }

    pub async fn begin(&mut self) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.begin().await,
            EngineBackend::SparkDelta(engine) => engine.begin().await,
            EngineBackend::SparkIceberg(engine) => engine.begin().await,
        }
    }

    pub async fn fetch_optional_i64(&mut self, sql: &str) -> anyhow::Result<Option<i64>> {
        match self {
            EngineBackend::Postgres(engine) => engine.fetch_optional_i64(sql).await,
            EngineBackend::SparkDelta(engine) => engine.fetch_optional_i64(sql).await,
            EngineBackend::SparkIceberg(engine) => engine.fetch_optional_i64(sql).await,
        }
    }

    pub async fn acquire_lock(&mut self) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.acquire_lock().await,
            EngineBackend::SparkDelta(engine) => engine.acquire_lock().await,
            EngineBackend::SparkIceberg(engine) => engine.acquire_lock().await,
        }
    }

    pub async fn disable_records(&mut self, current_version_id: i64) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.disable_records(current_version_id).await,
            EngineBackend::SparkDelta(engine) => engine.disable_records(current_version_id).await,
            EngineBackend::SparkIceberg(engine) => engine.disable_records(current_version_id).await,
        }
    }

    pub async fn upsert_record(
        &mut self,
        object_type: &sqlparser::ast::ObjectType,
        object_name_before: &str,
        object_name_after: &str,
        version_id: i64,
        checksum: &str
    ) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.upsert_record(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                checksum,
            ).await,
            EngineBackend::SparkDelta(engine) => engine.upsert_record(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                checksum,
            ).await,
            EngineBackend::SparkIceberg(engine) => engine.upsert_record(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                checksum,
            ).await,
        }
    }

    pub async fn execute_statements(&mut self, statements: StatementCollection) -> anyhow::Result<()> {
        for stmt in statements.to_strings() {
            match self {
                EngineBackend::Postgres(engine) => engine.execute(&stmt).await?,
                EngineBackend::SparkDelta(engine) => engine.execute(&stmt).await?,
                EngineBackend::SparkIceberg(engine) => engine.execute(&stmt).await?,
            }
        }

        Ok(())
    }

    pub async fn update_record(
        &mut self, 
        direction: &MigrationDirection,
        version_id: i64
    ) -> anyhow::Result<()> {
        let status = match direction {
            MigrationDirection::Up => "APPLIED",
            MigrationDirection::Down => "ROLLED_BACK"
        };

        match self {
            EngineBackend::Postgres(engine) => engine.update_record(status, version_id).await,
            EngineBackend::SparkDelta(engine) => engine.update_record(status, version_id).await,
            EngineBackend::SparkIceberg(engine) => engine.update_record(status, version_id).await,
        }
    }

    pub async fn rollback(&mut self) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.rollback().await,
            EngineBackend::SparkDelta(engine) => engine.rollback().await,
            EngineBackend::SparkIceberg(engine) => engine.rollback().await,
        }
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.commit().await,
            EngineBackend::SparkDelta(engine) => engine.commit().await,
            EngineBackend::SparkIceberg(engine) => engine.commit().await,
        }
    }

    pub fn snapshot(&mut self) -> anyhow::Result<Vec<u8>> {
        match self {
            EngineBackend::Postgres(engine) => engine.snapshot(),
            EngineBackend::SparkDelta(engine) => engine.snapshot(),
            EngineBackend::SparkIceberg(engine) => engine.snapshot(),
        }
    }
}


pub trait DbEngine {
    async fn ensure_table(&self) -> anyhow::Result<()>;
    async fn begin(&mut self) -> anyhow::Result<()>;
    async fn execute(&mut self, sql: &str) -> anyhow::Result<()>;
    async fn fetch_optional_i64(&mut self, sql: &str) -> anyhow::Result<Option<i64>>;
    async fn acquire_lock(&mut self) -> anyhow::Result<()>;
    async fn disable_records(&mut self, current_version_id: i64) -> anyhow::Result<()>;
    async fn upsert_record(
        &mut self,
        object_type: &sqlparser::ast::ObjectType,
        object_name_before: &str,
        object_name_after: &str,
        version_id: i64,
        checksum: &str
    ) -> anyhow::Result<()>;
    async fn update_record(&mut self, status: &str, version_id: i64) -> anyhow::Result<()>;
    async fn rollback(&mut self) -> anyhow::Result<()>;
    async fn commit(&mut self) -> anyhow::Result<()>;
    fn snapshot(&mut self) -> anyhow::Result<Vec<u8>>;
}
