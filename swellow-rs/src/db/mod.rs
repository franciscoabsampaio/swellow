mod error;
mod spark;
mod postgres;
pub use error::{EngineError, EngineErrorKind};
pub use postgres::PostgresEngine;
pub use spark::{SparkEngine, SparkCatalog};

use crate::migration::MigrationDirection;

use sqlparser;


pub enum EngineBackend {
    Postgres(PostgresEngine),
    SparkDelta(SparkEngine),
    SparkIceberg(SparkEngine),
}

impl EngineBackend {
    pub async fn ensure_table(&self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.ensure_table().await,
            EngineBackend::SparkDelta(engine) => engine.ensure_table().await,
            EngineBackend::SparkIceberg(engine) => engine.ensure_table().await,
        }
    }

    pub async fn begin(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.begin().await,
            EngineBackend::SparkDelta(engine) => engine.begin().await,
            EngineBackend::SparkIceberg(engine) => engine.begin().await,
        }
    }

    pub async fn fetch_optional_i64(&mut self, sql: &str) -> Result<Option<i64>, EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.fetch_optional_i64(sql).await,
            EngineBackend::SparkDelta(engine) => engine.fetch_optional_i64(sql).await,
            EngineBackend::SparkIceberg(engine) => engine.fetch_optional_i64(sql).await,
        }
    }

    pub async fn acquire_lock(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.acquire_lock().await,
            EngineBackend::SparkDelta(engine) => engine.acquire_lock().await,
            EngineBackend::SparkIceberg(engine) => engine.acquire_lock().await,
        }
    }

    pub async fn disable_records(&mut self, current_version_id: i64) -> Result<(), EngineError> {
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
    ) -> Result<(), EngineError> {
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

    pub async fn execute(&mut self, sql: &str) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.execute(sql).await?,
            EngineBackend::SparkDelta(engine) => engine.execute(sql).await?,
            EngineBackend::SparkIceberg(engine) => engine.execute(sql).await?,
        }

        Ok(())
    }

    pub async fn update_record(
        &mut self, 
        direction: &MigrationDirection,
        version_id: i64
    ) -> Result<(), EngineError> {
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

    pub async fn rollback(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.rollback().await,
            EngineBackend::SparkDelta(engine) => engine.rollback().await,
            EngineBackend::SparkIceberg(engine) => engine.rollback().await,
        }
    }

    pub async fn commit(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.commit().await,
            EngineBackend::SparkDelta(engine) => engine.commit().await,
            EngineBackend::SparkIceberg(engine) => engine.commit().await,
        }
    }

    pub fn snapshot(&mut self) -> Result<Vec<u8>, EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.snapshot(),
            EngineBackend::SparkDelta(engine) => engine.snapshot(),
            EngineBackend::SparkIceberg(engine) => engine.snapshot(),
        }
    }
}


pub trait DbEngine {
    async fn ensure_table(&self) -> Result<(), EngineError>;
    async fn begin(&mut self) -> Result<(), EngineError>;
    async fn execute(&mut self, sql: &str) -> Result<(), EngineError>;
    async fn fetch_optional_i64(&mut self, sql: &str) -> Result<Option<i64>, EngineError>;
    async fn acquire_lock(&mut self) -> Result<(), EngineError>;
    async fn disable_records(&mut self, current_version_id: i64) -> Result<(), EngineError>;
    async fn upsert_record(
        &mut self,
        object_type: &sqlparser::ast::ObjectType,
        object_name_before: &str,
        object_name_after: &str,
        version_id: i64,
        checksum: &str
    ) -> Result<(), EngineError>;
    async fn update_record(&mut self, status: &str, version_id: i64) -> Result<(), EngineError>;
    async fn rollback(&mut self) -> Result<(), EngineError>;
    async fn commit(&mut self) -> Result<(), EngineError>;
    fn snapshot(&mut self) -> Result<Vec<u8>, EngineError>;
}
