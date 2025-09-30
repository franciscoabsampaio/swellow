use super::DbEngine;
use odbc_api as odbc;


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
    env: odbc::Environment
}


impl OdbcEngine {
    pub fn new(conn_str: String, catalog: OdbcCatalog) -> anyhow::Result<Self, odbc_api::Error> {
        return Ok(OdbcEngine {
            conn_str: conn_str,
            catalog: catalog,
            env: odbc::Environment::new()?
        })
    }

    fn connect(&self) -> anyhow::Result<odbc::Connection> {
        let conn = self.env.connect_with_connection_string(
            &self.conn_str,
            odbc::ConnectionOptions::default()
        )?;
        
        Ok(conn)
    }
}


// #[async_trait::async_trait]
impl DbEngine for OdbcEngine {
    async fn ensure_table(&self) -> anyhow::Result<()> {
        let conn_str = self.conn_str.clone();
        let catalog = self.catalog;

        // tokio::task::spawn_blocking(move || {
        
        let using_clause = match catalog {
            OdbcCatalog::Delta => "DELTA",
            OdbcCatalog::Iceberg => "ICEBERG",
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
            USING {}
            "#,
            using_clause
        );

        conn.execute(&create_table_sql, (), Some(30))?;

        Ok(())
    }

    async fn begin(&mut self) -> anyhow::Result<Option<i64>> {

        let mut tx = ::connect(&self.conn_str).await?.begin().await?;

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
