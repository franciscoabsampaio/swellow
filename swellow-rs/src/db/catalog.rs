use crate::db::arrow_utils::get_column_by_name;
use crate::db::EngineError;
use arrow::array::{Array, BooleanArray, RecordBatch, StringArray};


/// Catalog type for OLAP databases
#[derive(Clone, Copy)]
pub enum Catalog {
    DatabricksDelta,
    Delta,
    Iceberg,
}


#[derive(Debug)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub table_type: String, // "TABLE" or "VIEW"
}


impl Catalog {
    /// SQL to list tables for this catalog
    pub fn show_tables_query(&self) -> &'static str {
        match self {
            Catalog::DatabricksDelta => "
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'sys')
            ",
            Catalog::Delta => "SHOW TABLES IN bird_watch",
            Catalog::Iceberg => "SHOW TABLES IN bird_watch"
        }
    }

    pub fn map_table_batch(&self, batch: &RecordBatch) -> Result<Vec<TableInfo>, EngineError> {
        match self {
            Catalog::DatabricksDelta => {
                let column_db = get_column_by_name::<StringArray>(batch, "table_schema")?;
                let column_table = get_column_by_name::<StringArray>(batch, "table_name")?;
                let column_type = get_column_by_name::<StringArray>(batch, "table_type")?;
                
                Ok((0..batch.num_rows())
                    .filter_map(|row| {
                        if column_db.is_null(row) || column_table.is_null(row) || column_type.is_null(row) {
                            return None;
                        }
                        Some(TableInfo {
                            schema: column_db.value(row).to_string(),
                            name: column_table.value(row).to_string(),
                            table_type: column_type.value(row).to_string(),
                        })
                    })
                    .collect())
            }
            Catalog::Delta | Catalog::Iceberg => {
                // SHOW TABLES returns different column names
                let column_db = get_column_by_name::<StringArray>(batch, "namespace")?;
                let column_table = get_column_by_name::<StringArray>(batch, "tableName")?;
                let column_type = get_column_by_name::<BooleanArray>(batch, "isTemporary")?;
                
                Ok((0..batch.num_rows())
                    .filter_map(|row| {
                        if column_db.is_null(row) || column_table.is_null(row) || column_type.is_null(row) {
                            return None;
                        }
                        Some(TableInfo {
                            schema: column_db.value(row).to_string(),
                            name: column_table.value(row).to_string(),
                            table_type: if column_type.value(row) { "TEMPORARY" } else { "TABLE" }.to_string(),
                        })
                    })
                    .collect())
            }
        }
    }
}