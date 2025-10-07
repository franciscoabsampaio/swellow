use crate::{SparkSession, error::SparkError};
use crate::spark::expression::Literal;
use crate::sql::ToLiteral;

use arrow::array::RecordBatch;


pub struct SqlQueryBuilder<'a> {
    session: &'a SparkSession,
    query: String,
    params: Vec<Literal>,
}

impl<'a> SqlQueryBuilder<'a> {
    pub fn new(session: &'a SparkSession, query: &str) -> Self {
        Self {
            session,
            query: query.to_string(),
            params: Vec::new(),
        }
    }

    pub fn bind<T: ToLiteral>(mut self, value: T) -> Self {
        self.params.push(value.to_literal());
        self
    }

    pub async fn execute(self) -> Result<Vec<RecordBatch>, SparkError> {
        let plan = self.session.sql(&self.query, self.params).await?;
        self.session.collect(plan).await
    }
}
