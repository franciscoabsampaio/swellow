use crate::spark;
use arrow::record_batch::RecordBatch;


#[derive(Default, Debug, Clone)]
pub struct AnalyzeHandler {
    pub schema: Option<spark::DataType>,
    pub spark_version: Option<String>,
}

#[derive(Default, Debug, Clone)]
pub struct ExecuteHandler {
    pub batches: Vec<RecordBatch>,
    pub relation: Option<spark::Relation>,
    pub result_complete: bool,
    pub total_count: isize,
}

#[derive(Default, Debug, Clone)]
pub struct InterruptHandler {
    pub interrupted_ids: Vec<String>
}
