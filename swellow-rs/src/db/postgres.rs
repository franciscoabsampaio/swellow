use std::ops::DerefMut;

use super::{DbEngine, file_checksum};
use sqlparser;
use sqlx::{PgPool, Postgres, Transaction};
use std::{path, process};

pub struct PostgresEngine {
    conn_str: String,
    tx: Option<Transaction<'static, Postgres>>,
}


impl PostgresEngine {
    pub fn new(conn_str: String) -> Self {
        return PostgresEngine { conn_str: conn_str, tx: None }
    }

    async fn transaction(&mut self) -> anyhow::Result<&mut Transaction<'static, Postgres>> {
        if self.tx.is_none() {
            let txn = PgPool::connect(&self.conn_str).await?.begin().await?;
            self.tx = Some(txn);
        }
        
        Ok(self.tx.as_mut().unwrap())
    }
}


// #[async_trait::async_trait]
impl DbEngine for PostgresEngine {
    async fn ensure_table(&self) -> anyhow::Result<()> {
        let pool = PgPool::connect(&self.conn_str).await?;
        
        sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            .execute(&pool)
            .await?;
        
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS swellow_records (
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

    async fn begin(&mut self) -> anyhow::Result<()> {
        self.transaction().await?;

        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        let tx = self.transaction().await?;

        sqlx::raw_sql(&sql)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> anyhow::Result<Option<i64>> {
        let tx = self.transaction().await?;
        
        Ok(sqlx::query_scalar(sql)
            .fetch_one(&mut **tx)
            .await?)
    }

    async fn acquire_lock(&mut self) -> anyhow::Result<()> {
        let tx = self.transaction().await?;

        sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE;")
            .execute(tx.deref_mut())
            .await?;

        return Ok(())
    }

    async fn disable_records(&mut self, current_version_id: i64) -> anyhow::Result<()> {
        let tx = self.transaction().await?;

        sqlx::query(
            r#"
            UPDATE swellow_records
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
        object_name_before: &String,
        object_name_after: &String,
        version_id: i64,
        file_path: &path::PathBuf
    ) -> anyhow::Result<()> {
        let tx = self.transaction().await?;

        sqlx::query(
            r#"
            INSERT INTO swellow_records(
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
            .bind(file_checksum(&file_path)?)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }
    async fn update_record(&mut self, status: &str, version_id: i64) -> anyhow::Result<()> {
        let tx = self.transaction().await?;

        sqlx::query(
            r#"
            UPDATE swellow_records
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

    async fn rollback(&mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.rollback().await?;
        }
        Ok(())
    }
    
    async fn commit(&mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.commit().await?;
        }
        Ok(())
    }

    fn snapshot(&mut self) -> anyhow::Result<Vec<u8>> {
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
            .output()?;
        
        if output.status.success() {
            return Ok(output.stdout)
        } else {
            anyhow::bail!("pgdump error: {}", String::from_utf8_lossy(&output.stderr))
        }
    }
}
