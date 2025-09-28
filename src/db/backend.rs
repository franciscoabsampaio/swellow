use sqlx::PgPool;

#[async_trait::async_trait]
pub trait DbEngine {
    async fn ensure_table(&self) -> anyhow::Result<()>;
}

/// Postgres engine
pub struct PostgresEngine {
    pub conn_str: String,
}

#[async_trait::async_trait]
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
}

/// Catalog type for ODBC engines
#[derive(Clone, Copy)]
pub enum OdbcCatalog {
    Delta,
    Iceberg,
}

/// ODBC-based engines (Spark Delta / Iceberg)
pub struct OdbcEngine {
    pub conn_str: String,
    pub catalog: OdbcCatalog,
}

#[async_trait::async_trait]
impl DbEngine for OdbcEngine {
    async fn ensure_table(&self) -> anyhow::Result<()> {
        let conn_str = self.conn_str.clone();
        let catalog = self.catalog;

        tokio::task::spawn_blocking(move || {
            let env = odbc::create_environment_v3().map_err(|e| e.unwrap())?;
            let conn = env.connect_with_connection_string(&conn_str)
                .map_err(|e| anyhow::anyhow!("ODBC connect failed: {:?}", e))?;

            let using_clause = match catalog {
                OdbcCatalog::Delta => "USING DELTA",
                OdbcCatalog::Iceberg => "USING ICEBERG",
            };

            let create_table_sql = format!(
                r#"
                CREATE TABLE IF NOT EXISTS swellow_records (
                    version_id BIGINT,
                    object_type STRING,
                    object_name_before STRING,
                    object_name_after STRING,
                    status STRING,
                    checksum STRING,
                    dtm_created_at TIMESTAMP,
                    dtm_updated_at TIMESTAMP
                )
                {}
                "#,
                using_clause
            );

            conn.execute(&create_table_sql, ())?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        Ok(())
    }
}
