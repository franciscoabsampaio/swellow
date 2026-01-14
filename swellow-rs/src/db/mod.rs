mod arrow_utils;
mod error;
mod spark;
mod postgres;
pub use error::{EngineError, EngineErrorKind};
pub use postgres::PostgresEngine;
pub use spark::{SparkEngine, SparkCatalog};

use crate::{cli, migration::MigrationDirection};

use sqlparser;


pub enum EngineBackend {
    Postgres(PostgresEngine),
    SparkDelta(SparkEngine),
    SparkIceberg(SparkEngine),
}

impl EngineBackend {
    pub fn engine(&self) -> cli::Engine {
        match self {
            EngineBackend::Postgres(_) => cli::Engine::Postgres,
            EngineBackend::SparkDelta(_) => cli::Engine::SparkDelta,
            EngineBackend::SparkIceberg(_) => cli::Engine::SparkIceberg,
        }
    }

    pub async fn ensure_table(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.ensure_table().await,
            EngineBackend::SparkDelta(engine) => engine.ensure_table().await,
            EngineBackend::SparkIceberg(engine) => engine.ensure_table().await,
        }
    }

    pub async fn begin(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.begin().await,
            _ => Ok(()),
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

    pub async fn release_lock(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.release_lock().await,
            EngineBackend::SparkDelta(engine) => engine.release_lock().await,
            EngineBackend::SparkIceberg(engine) => engine.release_lock().await,
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

    pub async fn execute(&mut self, sql: &str, flag_no_transaction: bool) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => if flag_no_transaction {
                engine.execute_outside_transaction(sql).await?
            } else {
                engine.execute(sql).await?
            },
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
            _ => Ok(()),
        }
    }

    pub async fn commit(&mut self) -> Result<(), EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.commit().await,
            _ => Ok(()),
        }
    }

    pub async fn snapshot(&mut self) -> Result<String, EngineError> {
        match self {
            EngineBackend::Postgres(engine) => engine.snapshot().await,
            EngineBackend::SparkDelta(engine) => engine.snapshot().await,
            EngineBackend::SparkIceberg(engine) => engine.snapshot().await,
        }
    }
}


pub trait DbEngine {
    async fn ensure_table(&mut self) -> Result<(), EngineError>;
    async fn execute(&mut self, sql: &str) -> Result<(), EngineError>;
    async fn fetch_optional_i64(&mut self, sql: &str) -> Result<Option<i64>, EngineError>;
    async fn acquire_lock(&mut self) -> Result<(), EngineError>;
    async fn release_lock(&mut self) -> Result<(), EngineError>;
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
    async fn snapshot(&mut self) -> Result<String, EngineError>;
}
