use super::DbEngine;
use arrow::{self, array::Array, array::Int64Array, array::RecordBatch};
use spark_connect as spark;


/// Catalog type for ODBC engines
#[derive(Clone, Copy)]
pub enum SparkCatalog {
    Delta,
    Iceberg,
}


/// The Spark Engine uses a Spark Connect client
/// to run queries against a data catalog.
pub struct SparkEngine {
    catalog: SparkCatalog,
    session: spark::SparkSession,
    // snapshot: 
}

impl SparkEngine {
    pub async fn new(conn_str: &str, catalog: SparkCatalog) -> anyhow::Result<Self, spark::SparkError> {
        return Ok(SparkEngine {
            catalog: catalog,
            session: spark::SparkSessionBuilder::new(conn_str).build().await?
        })
    }

    async fn sql(&mut self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        Ok(self.session.query(sql).execute().await?)
    }

    /// Fetch all rows for a single i64 column
    async fn fetch_all_i64(&mut self, sql: &str, column_name: &str) -> anyhow::Result<Vec<i64>> {        
        let mut results = Vec::new();

        let batches = self.sql(sql).await?;
        for batch in batches {
            let column_index = batch.schema().index_of(column_name).expect(
                &format!("Column not found: {column_name}")
            );
            let array_ref = batch.column(column_index);
            let int64_array = array_ref
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Column is not Int64Array!");

            for i in 0..int64_array.len() {
                if int64_array.is_valid(i) {
                    results.push(int64_array.value(i));
                }
            }
        }

        Ok(results)
    }
}


impl DbEngine for SparkEngine {
    async fn ensure_table(&self) -> anyhow::Result<()> {
        let catalog = self.catalog;
        
        let using_clause = match catalog {
            SparkCatalog::Delta => "DELTA",
            SparkCatalog::Iceberg => "ICEBERG",
        };

        let sql = format!(r#"
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
            USING {using_clause};
        "#);

        self.session.query(&sql).execute().await?;

        Ok(())
    }

    async fn begin(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        self.sql(sql).await?;
        Ok(())
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> anyhow::Result<Option<i64>> {
        let batches: Vec<RecordBatch> = self.sql(sql).await?;

        // If no batches returned, return None
        let first_batch = match batches.first() {
            Some(batch) => batch,
            None => return Ok(None),
        };

        // If the batch has no columns, return None
        let first_column = match first_batch.column(0).as_any().downcast_ref::<Int64Array>() {
            Some(col) => col,
            None => anyhow::bail!("Expected first column to be Int64Array"),
        };

        // If column is empty, return None
        if first_column.is_empty() {
            return Ok(None);
        }

        // Return the first value
        Ok(Some(first_column.value(0)))
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
                    'LOCK' AS checksum,
                    current_timestamp() AS dtm_updated_at
            ) s
            ON t.version_id = s.version_id
            AND t.object_type = s.object_type
            AND t.object_name_before = s.object_name_before
            AND t.object_name_after = s.object_name_after
            WHEN NOT MATCHED THEN
            INSERT (
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
                s.version_id,
                s.object_type,
                s.object_name_before,
                s.object_name_after,
                s.status,
                s.checksum,
                current_timestamp(),
                current_timestamp()
            )
        "#;
        
        if self.fetch_optional_i64(query).await?.is_none() {
            anyhow::bail!("Lock already exists!")
        }

        return Ok(())
    }

    async fn disable_records(&mut self, current_version_id: i64) -> anyhow::Result<()> {
        self.session.query(r#"
            UPDATE swellow_records
            SET status='DISABLED'
            WHERE version_id > ?
        "#)
            .bind(current_version_id)
            .execute()
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
    ) -> anyhow::Result<()> {
        self.session.query(r#"
            MERGE INTO swellow_records AS target
            USING (
                SELECT
                    ? AS object_type,
                    ? AS object_name_before,
                    ? AS object_name_after,
                    ? AS version_id,
                    'READY' AS status,
                    md5(?) AS checksum
            ) AS source
            ON target.version_id = source.version_id
                AND target.object_type = source.object_type
                AND target.object_name_before = source.object_name_before
                AND target.object_name_after = source.object_name_after
            WHEN MATCHED THEN
                UPDATE SET
                    target.status = source.status,
                    target.checksum = source.checksum
            WHEN NOT MATCHED THEN
                INSERT (
                    object_type,
                    object_name_before,
                    object_name_after,
                    version_id,
                    status,
                    checksum
                )
                VALUES (
                    source.object_type,
                    source.object_name_before,
                    source.object_name_after,
                    source.version_id,
                    source.status,
                    source.checksum
                )"#)
            .bind(object_type.to_string())
            .bind(object_name_before.to_string())
            .bind(object_name_after.to_string())
            .bind(version_id)
            .bind(checksum.to_string())
            .execute()
            .await?;

        Ok(())
    }

    async fn update_record(&mut self, status: &str, version_id: i64) -> anyhow::Result<()> {
        self.session.query(r#"
            UPDATE swellow_records
            SET
                status=?
            WHERE
                version_id=?
        "#)
            .bind(status)
            .bind(version_id)
            .execute()
            .await?;
        
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
