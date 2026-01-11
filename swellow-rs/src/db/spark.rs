use crate::db::{DbEngine, EngineError, error::EngineErrorKind};
use crate::db::arrow_utils::get_column;
use arrow;
use arrow::array::{Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray};
use arrow::datatypes::DataType;
use spark_connect as spark;
use std::fmt::Write;
use std::vec;


/// Helper: Parses the "DESCRIBE TABLE" output to build column definitions
/// Input batch columns: [col_name, data_type, comment]
fn build_schema_string(batch: &RecordBatch) -> Result<String, EngineError> {
    let col_names = get_column::<StringArray>(
        batch, 0, DataType::Utf8
    )?;
    let data_types = get_column::<StringArray>(
        batch, 0, DataType::Utf8
    )?;
    let comments = get_column::<StringArray>(
        batch, 0, DataType::Utf8
    )?;

    let mut schema_str = String::from("(");

    // Iterate through each row to build each column's definition
    for i in 0..batch.num_rows() {
        if i > 0 {
            schema_str.push_str(", ");
        }

        let name = col_names.value(i);
        let dtype = data_types.value(i);
        
        // Basic format: "col_name data_type"
        write!(&mut schema_str, "{} {}", name, dtype)?;

        // Add comment if it exists and is not null
        if comments.is_valid(i) {
            let comment = comments.value(i);
            if !comment.is_empty() {
                write!(&mut schema_str, " COMMENT '{}'", comment)?;
            }
        }
    }

    schema_str.push(')');
    Ok(schema_str)
}


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
    pub async fn new(conn_str: &str, catalog: SparkCatalog) -> Result<Self, EngineError> {
        return Ok(SparkEngine {
            catalog: catalog,
            session: spark::SparkSessionBuilder::new(conn_str).build().await?
        })
    }

    fn using_clause(&self) -> &str {
        match self.catalog {
            SparkCatalog::Delta => "delta",
            SparkCatalog::Iceberg => "iceberg",
        }
    }

    async fn sql(&mut self, sql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        Ok(self.session.query(sql).execute().await?)
    }

    async fn fetch_vec_of_strings(&mut self, sql: &str, column_index: usize) -> Result<Vec<String>, EngineError> {
        let batches: Vec<RecordBatch> = self.sql(sql).await?;

        let mut vec_of_strings: Vec<String> = vec![];

        for batch in &batches {
            let column = get_column::<StringArray>(
                batch, column_index, DataType::Utf8
            )?
                .iter()
                .flatten()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            
            vec_of_strings.extend(column);
        }

        Ok(vec_of_strings)
    }

    pub async fn generate_create_table_statement(
        &mut self,
        db: &str,
        table: &str,
    ) -> Result<String, EngineError> {
        // 0. Fetch DESCRIBE TABLE and DESCRIBE DETAIL info from Spark
        let describe_table_batch = &self.sql(
            &format!("DESCRIBE TABLE {db}.{table}")
        ).await?[0];
        let describe_detail_batch = &self.sql(
            &format!("DESCRIBE DETAIL {db}.{table}")
        ).await?[0];

        // 1. If we have a schema batch, format it (e.g., "(id int, name string)")
        // Otherwise, we leave it empty (Spark will infer schema from Location)
        let schema_def = build_schema_string(&describe_table_batch)?;

        // 2. Parse DESCRIBE DETAIL output for table properties
        let table_name = get_column::<StringArray>(
            describe_detail_batch, 2, DataType::Utf8,
        )?.value(0);
        let location = get_column::<StringArray>(
            describe_detail_batch, 4, DataType::Utf8,
        )?.value(0);
        let partition_columns_array = get_column::<ListArray>(
            describe_detail_batch, 7, DataType::Utf8,
        )?.value(0);
        let properties_array = get_column::<MapArray>(
            describe_detail_batch, 11, DataType::Utf8,
        )?.value(0);

        // Parsing Partitions
        let partitions: Vec<String> = match partition_columns_array
            .as_any()
            .downcast_ref::<StringArray>() {
                Some(arr) => arr
                    .iter()
                    .filter_map(|opt| opt.map(|s| s.to_string()))
                    .collect(),
                None => vec![],
            };

        // Parsing Properties
        let mut tbl_properties = Vec::new();
        let map = properties_array.as_any()
            .downcast_ref::<StructArray>();
        let keys = get_column::<StringArray>(
            describe_detail_batch, 0, DataType::Utf8,
        )?;
        let values = get_column::<StringArray>(
            describe_detail_batch, 1, DataType::Utf8,
        )?;
        
        if let Some(map) = map {
            for i in 0..map.len() {
                if keys.is_valid(i) && values.is_valid(i) {
                    tbl_properties.push(format!("'{}'='{}'", keys.value(i), values.value(i)));
                }
            }
        }

        // --- Updated Statement Construction ---
        // Injects `schema_def` between table name and USING DELTA
        let mut stmt = format!(
            "CREATE TABLE {} {} USING {} LOCATION '{}'", 
            table_name, schema_def, self.using_clause(), location
        );

        if !partitions.is_empty() {
            stmt.push_str(&format!(" PARTITIONED BY ({})", partitions.join(", ")));
        }
        if !tbl_properties.is_empty() {
            stmt.push_str(&format!(" TBLPROPERTIES ({})", tbl_properties.join(", ")));
        }
        
        Ok(stmt)
    }
}


impl DbEngine for SparkEngine {
    async fn ensure_table(&mut self) -> Result<(), EngineError> {
        self.sql(&format!(r#"
            CREATE TABLE IF NOT EXISTS swellow.records (
                version_id BIGINT,
                object_type STRING,
                object_name_before STRING,
                object_name_after STRING,
                status STRING,
                checksum STRING,
                dtm_created_at TIMESTAMP,
                dtm_updated_at TIMESTAMP
            )
            USING {};
        "#, self.using_clause())).await?;

        Ok(())
    }

    async fn begin(&mut self) -> Result<(), EngineError> {
        Ok(())
    }

    async fn execute(&mut self, sql: &str) -> Result<(), EngineError> {
        self.sql(sql).await?;
        Ok(())
    }

    /// Fetch an optional single column value
    async fn fetch_optional_i64(&mut self, sql: &str) -> Result<Option<i64>, EngineError> {
        let batches: Vec<RecordBatch> = self.sql(sql).await?;

        // If no batches returned, return None
        let first_batch = match batches.first() {
            Some(batch) => batch,
            None => return Ok(None),
        };

        // Get the first column as Int64Array
        // Raise error if type mismatch or column not found
        let col = get_column::<Int64Array>(
            first_batch,
            0,
            DataType::Int64
        )?;

        // If the column is empty, return None
        if col.is_empty() {
            return Ok(None);
        }

        // Return the first value (only if not null)
        if col.is_null(0) {
            return Ok(None);
        }

        Ok(Some(col.value(0)))
    }

    async fn acquire_lock(&mut self) -> Result<(), EngineError> {
        let query_select_lock = r#"
            SELECT *
            FROM swellow.records
            WHERE version_id = 0
                AND object_type = 'LOCK'
                AND object_name_before = 'LOCK'
                AND object_name_after = 'LOCK'
                AND status = 'LOCKED'
        "#;

        if self.fetch_optional_i64(query_select_lock).await?.is_some() {
            return Err(EngineError { kind: EngineErrorKind::LockConflict })
        }

        self.session.query(r#"
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
                current_timestamp(),
                current_timestamp()
            )
        "#)
            .execute()
            .await?;

        Ok(())
    }

    async fn release_lock(&mut self) -> Result<(), EngineError> {
        self.session.query(r#"
            DELETE FROM swellow.records
            WHERE version_id = 0
                AND object_type = 'LOCK'
                AND object_name_before = 'LOCK'
                AND object_name_after = 'LOCK'
        "#)
            .execute()
            .await?;

        Ok(())
    }

    async fn disable_records(&mut self, current_version_id: i64) -> Result<(), EngineError> {
        self.session.query(r#"
            UPDATE swellow.records
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
    ) -> Result<(), EngineError> {
        self.session.query(r#"
            MERGE INTO swellow.records AS target
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

    async fn update_record(&mut self, status: &str, version_id: i64) -> Result<(), EngineError> {
        self.session.query(r#"
            UPDATE swellow.records
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

    async fn rollback(&mut self) -> Result<(), EngineError> {
        Ok(())
    }
    
    async fn commit(&mut self) -> Result<(), EngineError> {
        Ok(())
    }

    async fn snapshot(&mut self) -> Result<String, EngineError> {
        let mut snapshot_string: String = String::new();

        // 1: Get all databases
        let db_names: Vec<String> = self.fetch_vec_of_strings("SHOW DATABASES", 0).await?;

        // 2: Iterate over databases
        for db in db_names {
            // Add CREATE DATABASE statement
            snapshot_string = format!("{snapshot_string}CREATE DATABASE {db};\n\n");

            // 3. Get tables in this database
            let table_names: Vec<String> = self.fetch_vec_of_strings(
                // Get the second column which contains table names
                &format!("SHOW TABLES IN {db}"), 1
            ).await?;

            // 4: For each table, get CREATE statement
            for table in table_names {
                println!("{:?}", self.sql(
                    &format!("SHOW CREATE TABLE {db}.{table}")
                ).await?);
                let stmt = match self.catalog {
                    SparkCatalog::Delta => self.generate_create_table_statement(&db, &table).await?,
                    SparkCatalog::Iceberg => "temp".to_string(),
                };
                snapshot_string = format!("{snapshot_string}{stmt};\n\n");
            }
        }

        Ok(snapshot_string)
    }
}