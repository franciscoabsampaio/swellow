use crate::db::{Catalog, DbEngine, EngineError, error::EngineErrorKind, sql_common};
use crate::db::arrow_utils::{ get_column, get_column_by_name, get_first_string };
use arrow;
use arrow::array::{
    Array,
    ArrowPrimitiveType,
    ListArray,
    MapArray,
    PrimitiveArray,
    RecordBatch,
    StringArray,
    StructArray
};
use arrow::datatypes::{Int32Type, Int64Type};
use spark_connect as spark;
use std::fmt::Write;
use std::vec;


/// Helper: Parses the "DESCRIBE TABLE" output to build column definitions
/// Input batch columns: [col_name, data_type, comment]
fn build_schema_string(batch: &RecordBatch) -> Result<String, EngineError> {
    let col_name = get_first_string(batch, "col_name")?;
    let data_type = get_first_string(batch, "data_type")?;
    let comments = get_column_by_name::<StringArray>(
        batch, "comment"
    )?;

    let mut schema_str = String::from("(");
    let num_rows = batch.num_rows();

    // Iterate through each row to build each column's definition
    for i in 0..num_rows {
        if i > 0 {
            schema_str.push_str(", ");
        }

        // Basic format: "col_name data_type"
        write!(&mut schema_str, "{} {}", col_name, data_type)?;

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


/// The Spark Engine uses a Spark Connect client
/// to run queries against a data catalog.
pub struct SparkEngine {
    catalog: Catalog,
    session: spark::SparkSession,
}

impl SparkEngine {
    pub async fn new(conn_str: &str, catalog: Catalog) -> Result<Self, EngineError> {
        return Ok(SparkEngine {
            catalog: catalog,
            session: spark::SparkSessionBuilder::new(conn_str).build().await?
        })
    }

    fn using_clause(&self) -> &str {
        match self.catalog {
            Catalog::Delta | Catalog::DatabricksDelta => "delta",
            Catalog::Iceberg => "iceberg",
        }
    }

    async fn sql(&mut self, sql: &str) -> Result<Vec<RecordBatch>, EngineError> {
        Ok(self.session.query(sql).execute().await?)
    }

    async fn fetch_column_of_strings(&mut self, sql: &str, column_index: usize) -> Result<Vec<String>, EngineError> {
        let batches: Vec<RecordBatch> = self.sql(sql).await?;

        let mut vec_of_strings: Vec<String> = vec![];

        for batch in &batches {
            let column = get_column::<StringArray>(
                batch, column_index
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
        // ---------------------------------------
        // 0. Fetch DESCRIBE TABLE and DESCRIBE DETAIL info from Spark
        let describe_table_batch = &self.sql(
            &format!("DESCRIBE EXTENDED {db}.{table}")
        ).await?[0];
        let describe_detail_batch = &self.sql(
            &format!("DESCRIBE DETAIL {db}.{table}")
        ).await?[0];

        // ---------------------------------------
        // 1. If we have a schema batch, format it (e.g., "(id int, name string)")
        // Otherwise, we leave it empty (Spark will infer schema from Location)
        let schema_def = build_schema_string(&describe_table_batch)?;

        // ---------------------------------------
        // 2. Parse DESCRIBE DETAIL output for table properties
        // Note: Column 2 is Name, Column 4 is Location (in standard Spark Delta describe)
        let table_name = get_first_string(describe_detail_batch, "name")?;
        let location = get_first_string(describe_detail_batch, "location")?;

        // 2.1. Partitions
        let partitions: Vec<String> = {
            let column_partition_columns = get_column_by_name::<ListArray>(
                describe_detail_batch, "partitionColumns"
            )?;

            if column_partition_columns.is_valid(0) {
                let value_slice = column_partition_columns.value(0);

                match value_slice.as_any().downcast_ref::<StringArray>() {
                    Some(string_arr) => string_arr
                        .iter()
                        .filter_map(|s| s.map(|inner| inner.to_string()))
                        .collect(),
                    None => vec![], // Could not cast inner list to StringArray
                }
            } else {
                vec![] // No partitions
            }
        };

        // 2.2. Table Properties
        let mut tbl_properties = Vec::new();

        let column_properties = get_column_by_name::<MapArray>(
            describe_detail_batch, "properties"
        )?;

        if column_properties.is_valid(0) {
            let entry = column_properties.value(0);

            if let Some(struct_array) = entry.as_any().downcast_ref::<StructArray>() {
                // The StructArray inside a Map always has 2 columns: 0 (keys) and 1 (values)
                let keys_col = struct_array.column(0);
                let values_col = struct_array.column(1);
            
                // Downcast the keys and values to StringArrays
                if let (Some(keys), Some(values)) = (
                    keys_col.as_any().downcast_ref::<StringArray>(),
                    values_col.as_any().downcast_ref::<StringArray>()
                ) {
                    // Iterate over the PROPERTIES
                    for i in 0..keys.len() {
                        if keys.is_valid(i) && values.is_valid(i) {
                            tbl_properties.push(format!("'{}'='{}'", keys.value(i), values.value(i)));
                        }
                    }
                }
            }
        }
        
        // ---------------------------------------
        // 3. Construct the CREATE TABLE statement
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

    /// Fetch an optional single column integer
    async fn fetch_optional_int<T>(
        &mut self,
        sql: &str,
    ) -> Result<Option<T::Native>, EngineError>
    where
        T: ArrowPrimitiveType,
    {
        let batches = self.sql(sql).await?;

        let first_batch = match batches.first() {
            Some(batch) => batch,
            None => return Ok(None),
        };

        let col: &PrimitiveArray<T> =
            get_column(first_batch, 0)?;

        if col.is_empty() || col.is_null(0) {
            return Ok(None);
        }

        Ok(Some(col.value(0)))
    }
}


impl DbEngine for SparkEngine {
    async fn ensure_table(&mut self) -> Result<(), EngineError> {
        self.sql(&"CREATE DATABASE IF NOT EXISTS swellow").await?;
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

    async fn execute(&mut self, sql: &str) -> Result<(), EngineError> {
        self.sql(sql).await?;
        Ok(())
    }

    async fn fetch_latest_applied_version(&mut self) -> Result<Option<i64>, EngineError> {
        self.fetch_optional_int::<Int64Type>(
            sql_common::QUERY_LATEST_VERSION,
        ).await
    }

    async fn acquire_lock(&mut self) -> Result<(), EngineError> {
        if self.fetch_optional_int::<Int32Type>(
            sql_common::QUERY_LOCK_EXISTS,
        ).await?.is_some() {
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
        self.session.query(sql_common::QUERY_DELETE_LOCK)
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

    async fn snapshot(&mut self) -> Result<String, EngineError> {
        let mut snapshot_string: String = String::new();

        // 1: Fetch all relevant tables and their types in one go
        let batches = self.sql(self.catalog.show_tables_query()).await?;
        // Keep track of current schema.
        // Since we're ORDERing BY the schema,
        // we go over each schema and 
        let mut current_db = String::new();

        // 2: Iterate over every batch
        for batch in batches {
            // 3: Get list of tables from batch
            let tables = self.catalog.map_table_batch(&batch)?;

            for table_info in tables {
                let db = &table_info.schema;
                let table = &table_info.name;
                let table_type = &table_info.table_type;

                // CREATE DATABASE if it's the first time we find it in the list
                if db != &current_db {
                    snapshot_string.push_str(&format!("CREATE DATABASE IF NOT EXISTS {db};\n\n"));
                    current_db = db.clone();
                }

                // 5. Generate Table/View Creation Statement
                // a) Try SHOW CREATE TABLE first
                let stmt = match self.fetch_column_of_strings(
                    &format!("SHOW CREATE TABLE {db}.{table}"),
                    0
                ).await {
                    Ok(cols) if !cols.is_empty() => cols[0].clone(),
                    Ok(_) => return Err(EngineError {
                        kind: EngineErrorKind::InvalidSchema {
                            stderr: format!("'SHOW CREATE TABLE {db}.{table}' returned empty"),
                        },
                    }),
                    // b) DatabricksDelta and SparkIceberg support SHOW CREATE TABLE for views,
                    // so it shouldn't have failed.
                    Err(err) if matches!(
                        self.catalog, Catalog::DatabricksDelta | Catalog::Iceberg
                    ) => return Err(err),
                    // c) If it is a VIEW, skip,
                    // because the CREATE statement cannot be faithfully generated.
                    Err(_) if table_type.contains("VIEW") => continue,
                    // d) If it is NOT a VIEW and the catalog isn't Databricks,
                    // manually generate the table statement.
                    Err(_) => self.generate_create_table_statement(db, table).await?,
                };

                snapshot_string.push_str(&format!("{stmt};\n\n"));
            }
        }

        Ok(snapshot_string)
    }
}