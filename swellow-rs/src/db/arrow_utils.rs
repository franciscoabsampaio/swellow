use crate::db::error::{EngineError, EngineErrorKind};
use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;


/// Retrieves and downcasts a column from an Arrow RecordBatch.
pub fn get_column<'a, T: Array + 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_index: usize,
) -> Result<&'a T, EngineError> {
    let array = batch
        .columns()
        .get(column_index)
        .ok_or_else(|| EngineError {
            kind: EngineErrorKind::ColumnIndexOutOfBounds {
                column_index,
                num_columns: batch.num_columns(),
            },
        })?;

    let schema = batch.schema();
    let field = schema
        .fields()
        .get(column_index)
        .ok_or_else(|| EngineError {
            kind: EngineErrorKind::ColumnIndexOutOfBounds {
                column_index,
                num_columns: batch.schema().fields().len(),
            },
        })?;

    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| EngineError {
            kind: EngineErrorKind::ColumnTypeMismatch {
                column_index,
                expected: std::any::type_name::<T>(),
                found: field.data_type().clone(),
            },
        })
}


/// Retrieves and downcasts a column from an Arrow RecordBatch by column name.
pub fn get_column_by_name<'a, T: Array + 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_name: &str,
) -> Result<&'a T, EngineError> {
    // Find the column index by name
    let column_index = batch
        .schema()
        .index_of(column_name)
        .map_err(|_| EngineError {
            kind: EngineErrorKind::ColumnNotFound {
                column_name: column_name.to_string(),
            },
        })?;

    // Get column by index
    get_column::<T>(batch, column_index)
}


/// Get the first string value from a batch's column
pub fn get_first_string(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<String, EngineError> {
    let array = get_column_by_name::<StringArray>(
        batch, column_name
    )?;

    if array.is_valid(0) {
        Ok(array.value(0).to_string())
    } else {
        Err(EngineError {
            kind: EngineErrorKind::InvalidSchema {
                stderr: format!("Column {} is empty", column_name),
            }
        })
    }
}
