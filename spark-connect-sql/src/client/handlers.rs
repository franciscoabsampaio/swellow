use crate::spark;
use arrow::record_batch::RecordBatch;


#[derive(Default, Debug, Clone)]
pub(crate) struct AnalyzeHandler {
    pub(crate) schema: Option<spark::DataType>,
    pub(crate) spark_version: Option<String>,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct ExecuteHandler {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) relation: Option<spark::Relation>,
    pub(crate) result_complete: bool,
    pub(crate) total_count: isize,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct InterruptHandler {
    pub(crate) interrupted_ids: Vec<String>
}
