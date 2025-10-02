mod spark;
mod postgres;
pub use spark::SparkEngine;
pub use postgres::PostgresEngine;

use crate::commands::MigrationDirection;

use sha2::{Sha256, Digest};
use sqlparser;
use std::{fs, io::{BufReader, Read}, path};


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
        object_name_before: &String,
        object_name_after: &String,
        version_id: i64,
        file_path: &path::PathBuf
    ) -> anyhow::Result<()> {
        match self {
            EngineBackend::Postgres(engine) => engine.upsert_record(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                file_path,
            ).await,
            EngineBackend::SparkDelta(engine) => engine.upsert_record(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                file_path,
            ).await,
            EngineBackend::SparkIceberg(engine) => engine.upsert_record(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                file_path,
            ).await,
        }
    }

    pub async fn execute_sql_script(&mut self, file_path: &path::PathBuf) -> anyhow::Result<()> {
        let sql = match fs::read_to_string(file_path) {
            Ok(sql) => sql,
            Err(e) => {
                tracing::error!("Error processing {:?}: {}", file_path, e);
                std::process::exit(1);
            }
        };

        match self {
            EngineBackend::Postgres(engine) => engine.execute(&sql).await,
            EngineBackend::SparkDelta(engine) => engine.execute(&sql).await,
            EngineBackend::SparkIceberg(engine) => engine.execute(&sql).await,
        }
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
        object_name_before: &String,
        object_name_after: &String,
        version_id: i64,
        file_path: &path::PathBuf
    ) -> anyhow::Result<()>;
    async fn update_record(&mut self, status: &str, version_id: i64) -> anyhow::Result<()>;
    async fn rollback(&mut self) -> anyhow::Result<()>;
    async fn commit(&mut self) -> anyhow::Result<()>;
    fn snapshot(&mut self) -> anyhow::Result<Vec<u8>>;
}


fn file_checksum(path: &path::Path) -> Result<String, std::io::Error> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 4096];

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    // Convert result to hex string
    Ok(format!("{:x}", hasher.finalize()))
}