use crate::db::error::{EngineError, EngineErrorKind};
use arrow::array::Array;
use arrow::datatypes::DataType;


/// Downcasts an Arrow array to the expected type, returning an error if the types do not match.
pub fn downcast_column<'a, T: Array + 'static>(
    array: &'a dyn Array,
    column_index: usize,
    expected: DataType,
    found: DataType,
) -> Result<&'a T, EngineError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| EngineError {
            kind: EngineErrorKind::ColumnTypeMismatch {
                column_index,
                expected,
                found,
            },
        })
}


/// Retrieves and downcasts a column from an Arrow RecordBatch.
pub fn get_column<'a, T: Array + 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    column_index: usize,
    expected: DataType,
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

    downcast_column::<T>(
        array,
        column_index,
        expected,
        field.data_type().clone(),
    )
}