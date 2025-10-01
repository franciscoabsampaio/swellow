use super::{DbEngine, file_checksum};
use odbc_api::{self as odbc, Cursor, Nullable, ParameterCollectionRef};
// use sqlparser;
use std::path;


/// Catalog type for ODBC engines
#[derive(Clone, Copy)]
pub enum OdbcCatalog {
    Delta,
    Iceberg,
}


/// ODBC-based engines (Spark Delta / Iceberg)
pub struct OdbcEngine {
    conn_str: String,
    catalog: OdbcCatalog,
    env: odbc::Environment,
    // snapshot: 
}


impl OdbcEngine {
    pub fn new(conn_str: String, catalog: OdbcCatalog) -> anyhow::Result<Self, odbc_api::Error> {
        return Ok(OdbcEngine {
            conn_str: conn_str,
            catalog: catalog,
            env: odbc::Environment::new()?
        })
    }

    fn connect(&self) -> anyhow::Result<odbc::Connection<'_>> {
        let conn = self.env.connect_with_connection_string(
            &self.conn_str,
            odbc::ConnectionOptions::default()
        )?;
        
        Ok(conn)
    }

    /// Executes a statement, returning nothing
    fn _execute(&self, sql: &str, params: impl ParameterCollectionRef) -> anyhow::Result<()> {
        let conn = self.connect()?;
        
        conn.execute(sql, params, Some(30))?;

        Ok(())
    }

    /// Fetch all rows for a single i64 column
    fn fetch_all_i64(&mut self, sql: &str) -> anyhow::Result<Vec<i64>> {
        let conn = self.connect()?;
        
        let mut results = Vec::new();
        
        let cursor_opt = conn.execute(sql, (), Some(30))?;
        
        if let Some(mut cursor) = cursor_opt {
            while let Some(mut row) = cursor.next_row()? {
                let mut buf = Nullable::<i64>::null();
                row.get_data(1, &mut buf)?;
                if let Some(value) = buf.into_opt() {
                    results.push(value);
                }
            }
        }
        Ok(results)
    }
}


impl DbEngine for OdbcEngine {
    async fn ensure_table(&self) -> anyhow::Result<()> {
        let catalog = self.catalog;
        
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

        self._execute(&create_table_sql, ())?;

        Ok(())
    }

    async fn begin(&mut self) -> anyhow::Result<()> {
        self.connect()?;
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        self._execute(sql, ())?;
        Ok(())
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> anyhow::Result<Option<i64>> {
        let conn = self.connect()?;

        let cursor_opt = conn.execute(sql, (), Some(30))?;
        
        if let Some(mut cursor) = cursor_opt {
            if let Some(mut row) = cursor.next_row()? {
                let mut buf = Nullable::<i64>::null();
                row.get_data(1, &mut buf)?;
        
                return Ok(buf.into_opt());
            }
        }

        Ok(None)
    }

    async fn acquire_lock(&mut self) -> anyhow::Result<()> {
        let query = r#"
            MERGE INTO swellow_records t
            USING (
                SELECT 0 AS version_id,
                    'LOCK' AS object_type,
                    'LOCK' AS object_name_before,
                    'LOCK' AS object_name_after,
                    'LOCKED' AS status,
                    'LOCK' AS checksum
            ) s
            ON t.version_id = s.version_id
            AND t.object_type = s.object_type
            AND t.object_name_before = s.object_name_before
            AND t.object_name_after = s.object_name_after
            WHEN NOT MATCHED THEN
            INSERT *
        "#;
        
        if self.fetch_optional_i64(query).await?.is_none() {
            anyhow::bail!("Lock already exists!")
        }

        return Ok(())
    }

    async fn disable_records(&mut self, current_version_id: i64) -> anyhow::Result<()> {
        self._execute(
            r#"
            UPDATE swellow_records
            SET status='DISABLED'
            WHERE version_id > ?
            "#,
            &current_version_id
        )?;

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
        self._execute(&format!(r#"
            INSERT INTO swellow_records(
                object_type,
                object_name_before,
                object_name_after,
                version_id,
                status,
                checksum
            )
            VALUES (
                {},
                {},
                {},
                {},
                'READY',
                md5({})
            )
            ON CONFLICT (version_id, object_type, object_name_before, object_name_after)
            DO UPDATE SET
                status = EXCLUDED.status,
                checksum = EXCLUDED.checksum
        "#,
            object_type.to_string(),
            object_name_before.to_string(),
            object_name_after.to_string(),
            version_id,
            file_checksum(&file_path)?,
        ), ())?;

        Ok(())
    }

    async fn update_record(&mut self, status: &str, version_id: i64) -> anyhow::Result<()> {
        self._execute(&format!(
            r#"
            UPDATE swellow_records
            SET
                status={}
            WHERE
                version_id={}
            "#, status, version_id
        ), ())?;
        
        Ok(())
    }

    async fn rollback(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    
    async fn commit(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn snapshot(&mut self) -> anyhow::Result<Vec<u8>> {
        anyhow::bail!("This feature isn't ready.")
    }
}
