use super::{DbEngine, file_checksum};
use arrow::{self, array::Array, array::RecordBatch};
use spark_connect_sql as spark;
use std::path;


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
    pub async fn new(conn_str: String, catalog: SparkCatalog) -> anyhow::Result<Self, spark::error::SparkError> {
        return Ok(SparkEngine {
            catalog: catalog,
            session: spark::SparkSessionBuilder::new(&conn_str).build().await?
        })
    }

    async fn sql(&mut self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let plan = self.session.sql(sql).await?;
        Ok(self.session.collect(plan).await?)
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

        self.session.sql(&create_table_sql).await?;

        Ok(())
    }

    async fn begin(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> anyhow::Result<()> {
        self.session.sql(sql).await?;
        Ok(())
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> anyhow::Result<Option<i64>> {
        let batches = self.session.sql(sql).await?;

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
        self.session.sql(
            r#"
            UPDATE swellow_records
            SET status='DISABLED'
            WHERE version_id > ?
            "#,
            &current_version_id
        ).await?;

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
        self.session.sql(&format!(r#"
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
        )).await?;

        Ok(())
    }

    async fn update_record(&mut self, status: &str, version_id: i64) -> anyhow::Result<()> {
        self.session.sql(&format!(
            r#"
            UPDATE swellow_records
            SET
                status={}
            WHERE
                version_id={}
            "#, status, version_id
        )).await?;
        
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
