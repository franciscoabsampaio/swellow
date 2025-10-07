use crate::error::SparkError;
use arrow::array::RecordBatch;
use arrow_ipc::reader::StreamReader;


pub fn deserialize(stream: &[u8], row_count: i64) -> Result<(Vec<RecordBatch>, isize), SparkError> {
    let reader = StreamReader::try_new(stream, None)?;
    
    let mut batches: Vec<RecordBatch> = vec![];
    let mut total_count: isize = 0;

    for batch in reader {
        let record = batch?;
        if record.num_rows() != row_count as usize {
            return Err(SparkError::ArrowError(format!(
                "Expected {} rows in arrow batch but got {}",
                row_count,
                record.num_rows()
            )));
        };
        batches.push(record);
        total_count += row_count as isize;
    }

    Ok((batches, total_count))
}