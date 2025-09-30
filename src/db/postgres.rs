use super::DbEngine;
use sqlx::{PgPool, Postgres, Transaction};


pub struct PostgresEngine {
    conn_str: String,
    tx: Option<Transaction<'static, Postgres>>,
}


impl PostgresEngine {
    pub fn new(conn_str: String) -> Self {
        return PostgresEngine { conn_str: conn_str, tx: None }
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


    async fn begin(&mut self) -> anyhow::Result<Option<i64>> {
        let mut tx = PgPool::connect(&self.conn_str).await?.begin().await?;

        tracing::info!("Acquiring lock on records table...");
        // Acquire a lock on the swellow_records table
        // To ensure no other migration process is underway.
        sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE;")
            .execute(&mut *tx)
            .await?;

        tracing::info!("Getting latest migration version from records...");
        let version: Option<i64> = sqlx::query_scalar("
        SELECT
            MAX(version_id) version_id
        FROM swellow_records
        WHERE status IN ('APPLIED', 'TESTED')
        ")
            .fetch_one(&mut *tx)
            .await?;

        self.tx = Some(tx);

        return Ok(version)
    }
}
