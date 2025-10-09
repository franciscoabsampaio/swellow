//! Provides support for building and executing parameterized SQL queries through [`SparkSession::query`].
//!
//! # Overview
//!
//! This module defines the internal [`SqlQueryBuilder`] type used by [`SparkSession::query`] to
//! support a fluent, type-safe API for parameterized SQL queries.  
//!
//! Users are not expected to instantiate [`SqlQueryBuilder`] directly; instead, call
//! [`SparkSession::query`], and then chain `.bind()` calls to attach
//! parameters before executing the query.
//!
//! # Example
//!
//! ```
//! use spark_connect::SparkSessionBuilder;
//! use arrow::array::RecordBatch;
//!
//! # tokio_test::block_on(async {
//! let session = SparkSessionBuilder::new("sc://localhost:15002").build().await.unwrap();
//!
//! // Build and execute a parameterized query fluently
//! let results: Vec<RecordBatch> = session
//!     .query("SELECT ? AS id, ? AS name")
//!     .bind(42)
//!     .bind("Alice")
//!     .execute()
//!     .await
//!     .unwrap();
//!
//! assert!(!results.is_empty());
//! # });
//! ```
//!
//! # How it works
//!
//! - [`SparkSession::query`] creates an internal [`SqlQueryBuilder`] instance tied to the session
//!   and initializes it with a SQL query string containing `?` placeholders.
//! - `.bind()` attaches parameter values, converting each Rust type into a Spark [`Literal`] via
//!   the [`ToLiteral`] trait.
//! - `.execute()` runs the query asynchronously and collects the resulting Arrow
//!   [`RecordBatch`]es into memory.
//!
//! # See also
//! - [`ToLiteral`] — converts native Rust types into Spark literals.
//! - [`SparkSession::sql`] — executes parameterized SQL queries directly.
//!
//! # Errors
//!
//! Returns a [`SparkError`] if query preparation or execution fails.

use crate::{SparkSession, error::SparkError};
use crate::spark::expression::Literal;
use crate::ToLiteral;

use arrow::array::RecordBatch;


pub struct SqlQueryBuilder<'a> {
    session: &'a SparkSession,
    query: String,
    params: Vec<Literal>,
}

impl<'a> SqlQueryBuilder<'a> {
    pub(crate) fn new(session: &'a SparkSession, query: &str) -> Self {
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
