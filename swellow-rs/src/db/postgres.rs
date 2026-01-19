use crate::db::{EngineError, error::EngineErrorKind, sql_common};
use super::DbEngine;
use sqlparser;
use sqlx::{PgPool, Pool, Postgres, Transaction};
use sqlx::postgres::PgPoolOptions;
use std::process;


pub struct PostgresEngine {
    conn_str: String,
    pool: Option<Pool<Postgres>>,
    tx: Option<Transaction<'static, Postgres>>,
    flag_no_transaction: bool,  // Flag to indicate if transactions should be used
}


impl PostgresEngine {
    pub fn new(conn_str: &str) -> Self {
        return PostgresEngine {
            conn_str: conn_str.to_string(),
            pool: None,
            tx: None,
            flag_no_transaction: false,
        };
    }

    pub fn disable_transactions(&mut self) -> () {
        self.flag_no_transaction = true;
    }

    async fn pool(&mut self) -> Result<PgPool, EngineError> {
        if self.pool.is_none() {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&self.conn_str)
                .await?;
            self.pool = Some(pool);
        }

        self.pool.clone().ok_or_else(|| EngineError {
            kind: EngineErrorKind::TransactionNotStarted,
        })
    }

    async fn transaction(&mut self) -> Result<&mut Transaction<'static, Postgres>, EngineError> {
        if self.tx.is_none() {
            let txn = self.pool().await?.begin().await?;
            self.tx = Some(txn);
        }
        
        self.tx.as_mut().ok_or_else(|| EngineError {
            kind: EngineErrorKind::TransactionNotStarted,
        })
    }

    async fn _execute(&mut self, sql: &str) -> Result<(), EngineError> {
        if self.flag_no_transaction {
            let pool = self.pool().await?;
            sqlx::raw_sql(&sql)
                .execute(&pool)
                .await?;
        } else {
            let tx = self.transaction().await?;
            sqlx::raw_sql(&sql)
                .execute( &mut **tx)
                .await?;
        }

        Ok(())
    }

    pub async fn begin(&mut self) -> Result<(), EngineError> {
        self.pool().await?;
        self.transaction().await?;

        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), EngineError> {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await?;
        }
        Ok(())
    }
    
    pub async fn commit(&mut self) -> Result<(), EngineError> {
        if let Some(tx) = self.tx.take() {
            tx.commit().await?;
        }
        Ok(())
    }

    /// Returns true if the query returns at least one row
    async fn exists(&mut self, sql: &str) -> Result<bool, EngineError> {
        if self.flag_no_transaction {
            let pool = self.pool().await?;
            Ok(sqlx::query_scalar::<_, i32>(sql)
                .fetch_optional(&pool)
                .await?
                .is_some())
        } else {
            let tx = self.transaction().await?;
            Ok(sqlx::query_scalar::<_, i32>(sql)
                .fetch_optional(&mut **tx)
                .await?
                .is_some())
        }
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> Result<Option<i64>, EngineError> {
        if self.flag_no_transaction {
            let pool = self.pool().await?;
            sqlx::query_scalar(sql)
                .fetch_one(&pool)
                .await
                .map_err(Into::into)
        } else {
            let tx = self.transaction().await?;
            sqlx::query_scalar(sql)
                .fetch_one(&mut **tx)
                .await
                .map_err(Into::into)
        }
    }
}


impl DbEngine for PostgresEngine {
    async fn ensure_table(&mut self) -> Result<(), EngineError> {
        let pool = PgPool::connect(&self.conn_str).await?;
        
        sqlx::query("CREATE SCHEMA IF NOT EXISTS swellow;")
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

    async fn execute(&mut self, sql: &str) -> Result<(), EngineError> {
        self._execute(sql).await
    }

    async fn fetch_latest_applied_version(&mut self) -> Result<Option<i64>, EngineError> {
        self.fetch_optional_i64(sql_common::QUERY_LATEST_VERSION).await
    }

    async fn acquire_lock(&mut self) -> Result<(), EngineError> {
        if self.flag_no_transaction {
            // If transactions are disabled, we simulate a lock
            // by inserting a LOCK record and checking for its existence.
            if self.exists(sql_common::QUERY_LOCK_EXISTS).await? {
                return Err(EngineError { kind: EngineErrorKind::LockConflict })
            }

            self.execute(r#"
                INSERT INTO swellow.records (
                    version_id,
                    object_type,
                    object_name_before,
                    object_name_after,
                    status,
                    checksum,
                    dtm_created_at,
                    dtm_updated_at
                )
                VALUES (
                    0,
                    'LOCK',
                    'LOCK',
                    'LOCK',
                    'LOCKED',
                    'LOCK',
                    now(),
                    now()
                )
            "#).await?;
        } else {
            self.execute(&"LOCK TABLE swellow.records IN ACCESS EXCLUSIVE MODE;").await?;
        }

        Ok(())
    }

    async fn release_lock(&mut self) -> Result<(), EngineError> {
        if self.flag_no_transaction {
            self.execute(sql_common::QUERY_DELETE_LOCK).await?;
        } else {
            // In a transaction, the lock is released automatically on commit/rollback.
            // No action needed.
        }

        Ok(())
    }

    async fn disable_records(&mut self, current_version_id: i64) -> Result<(), EngineError> {
        let query = r#"
            UPDATE swellow.records
            SET status='DISABLED'
            WHERE version_id > $1
        "#;

        if self.flag_no_transaction {
            let pool = self.pool().await?;
            sqlx::query(query)
                .bind(current_version_id)
                .execute(&pool)
                .await?;
        } else {
            let tx = self.transaction().await?;
            sqlx::query(query)
                .bind(current_version_id)
                .execute(&mut **tx)
                .await?;
        }

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
        let query = r#"
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
        "#;

        if self.flag_no_transaction {
            let pool = self.pool().await?;
            sqlx::query(query)
                .bind(object_type.to_string())
                .bind(object_name_before)
                .bind(object_name_after)
                .bind(version_id)
                .bind(checksum.to_string())
                .execute(&pool)
                .await?;
        } else {
            let tx = self.transaction().await?;
            sqlx::query(query)
                .bind(object_type.to_string())
                .bind(object_name_before)
                .bind(object_name_after)
                .bind(version_id)
                .bind(checksum.to_string())
                .execute(&mut **tx)
                .await?;
        }

        Ok(())
    }


    async fn update_record(&mut self, status: &str, version_id: i64) -> Result<(), EngineError> {
        let query = r#"
            UPDATE swellow.records
            SET
                status=$1
            WHERE
                version_id=$2
        "#;

        if self.flag_no_transaction {
            let pool = self.pool().await?;
            sqlx::query(query)
                .bind(status)
                .bind(version_id)
                .execute(&pool)
                .await?;
        } else {
            let tx = self.transaction().await?;
            sqlx::query(query)
                .bind(status)
                .bind(version_id)
                .execute(&mut **tx)
                .await?;
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
