use crate::db::{EngineError, error::EngineErrorKind};

use super::DbEngine;
use sqlparser;
use sqlx::{PgPool, Postgres, Transaction};
use std::ops::DerefMut;
use std::process;

pub struct PostgresEngine {
    conn_str: String,
    tx: Option<Transaction<'static, Postgres>>,
}


impl PostgresEngine {
    pub fn new(conn_str: &str) -> Self {
        return PostgresEngine { conn_str: conn_str.to_string(), tx: None }
    }

    async fn transaction(&mut self) -> Result<&mut Transaction<'static, Postgres>, EngineError> {
        if self.tx.is_none() {
            let txn = PgPool::connect(&self.conn_str)
                .await?
                .begin()
                .await?;
            self.tx = Some(txn);
        }
        
        self.tx.as_mut().ok_or_else(|| EngineError {
            kind: EngineErrorKind::TransactionNotStarted,
        })
    }
}


// #[async_trait::async_trait]
impl DbEngine for PostgresEngine {
    async fn ensure_table(&mut self) -> Result<(), EngineError> {
        let pool = PgPool::connect(&self.conn_str).await?;
        
        sqlx::query("CREATE SCHEMA swellow;")
            .execute(&pool)
            .await?;
        
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            .execute(&pool)
            .await?;
        
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS swellow.records (
                oid OID,
                version_id BIGINT NOT NULL,
                object_type TEXT NOT NULL,
                object_name_before TEXT NOT NULL,
                object_name_after TEXT NOT NULL,
                status TEXT NOT NULL,
                checksum TEXT NOT NULL,
                dtm_created_at TIMESTAMP DEFAULT now(),
                dtm_updated_at TIMESTAMP DEFAULT now(),
                PRIMARY KEY (version_id, object_type, object_name_before, object_name_after)
            );
        "#)
        .execute(&pool)
        .await?;
        
        Ok(())
    }

    async fn begin(&mut self) -> Result<(), EngineError> {
        self.transaction().await?;

        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<(), EngineError> {
        let tx = self.transaction().await?;

        sqlx::raw_sql(&sql)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> Result<Option<i64>, EngineError> {
        let tx = self.transaction().await?;
        
        Ok(sqlx::query_scalar(sql)
            .fetch_one(&mut **tx)
            .await?)
    }

    async fn acquire_lock(&mut self) -> Result<(), EngineError> {
        let tx = self.transaction().await?;

        sqlx::query("LOCK TABLE swellow.records IN ACCESS EXCLUSIVE MODE;")
            .execute(tx.deref_mut())
            .await?;

        Ok(())
    }

    async fn release_lock(&mut self) -> Result<(), EngineError> {
        Ok(())
    }

    async fn disable_records(&mut self, current_version_id: i64) -> Result<(), EngineError> {
        let tx = self.transaction().await?;

        sqlx::query(
            r#"
            UPDATE swellow.records
            SET status='DISABLED'
            WHERE version_id > $1
            "#,
        )
            .bind(current_version_id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn upsert_record(
        &mut self,
        object_type: &sqlparser::ast::ObjectType,
        object_name_before: &str,
        object_name_after: &str,
        version_id: i64,
        checksum: &str
    ) -> Result<(), EngineError> {
        let tx = self.transaction().await?;

        sqlx::query(
            r#"
            INSERT INTO swellow.records(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                status,
                checksum
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                'READY',
                md5($5)
            )
            ON CONFLICT (version_id, object_type, object_name_before, object_name_after)
            DO UPDATE SET
                status = EXCLUDED.status,
                checksum = EXCLUDED.checksum
            "#,
        )
            .bind(object_type.to_string())
            .bind(object_name_before)
            .bind(object_name_after)
            .bind(version_id)
            .bind(checksum.to_string())
            .execute(&mut **tx)
            .await?;

        Ok(())
    }
    async fn update_record(&mut self, status: &str, version_id: i64) -> Result<(), EngineError> {
        let tx = self.transaction().await?;

        sqlx::query(
            r#"
            UPDATE swellow.records
            SET
                status=$1
            WHERE
                version_id=$2
            "#,
        )
            .bind(status)
            .bind(version_id)
            .execute(&mut **tx)
            .await?;
        
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), EngineError> {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await?;
        }
        Ok(())
    }
    
    async fn commit(&mut self) -> Result<(), EngineError> {
        if let Some(tx) = self.tx.take() {
            tx.commit().await?;
        }
        Ok(())
    }

    async fn snapshot(&mut self) -> Result<String, EngineError> {
        // Check if pg_dump is installed
        if process::Command::new("pg_dump").arg("--version").output()
            .is_err() {
            tracing::error!("pg_dump not installed or not in PATH.");
            std::process::exit(1);
        }
        let output = process::Command::new("pg_dump")
            .arg("--schema-only") // only schema, no data
            .arg("--no-owner")    // drop ownership info
            .arg("--no-privileges")
            .arg(&self.conn_str)
            .output()
            .map_err(|source| {
                EngineError { kind: EngineErrorKind::Process { source, cmd: "pg_dump --schema-only --no-owner --no-privileges".to_string() }}
            })?;

        if output.status.success() {
            let stdout = String::from_utf8(output.stdout)
                .map_err(|e| EngineError { kind: EngineErrorKind::Utf8(e) })?;

            Ok(stdout)
        } else {
            let stderr = String::from_utf8(output.stderr)
                .map_err(|e| EngineError { kind: EngineErrorKind::Utf8(e) })?;

            Err(EngineError {
                kind: EngineErrorKind::PGDump(stderr),
            })
        }
    }
}
