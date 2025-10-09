//! High-level user-facing interface for Spark Connect.
//!
//! This module provides [`SparkSession`] — the main entry point for interacting
//! with a Spark Connect server. It exposes a familiar API surface inspired by
//! PySpark and Scala's `SparkSession`, while delegating low-level gRPC work to
//! [`SparkClient`](crate::SparkClient).
//!
//! # Typical usage
//!
//! ```
//! use spark_connect_rs::SparkSessionBuilder;
//!
//! # tokio_test::block_on(async {
//! let session = SparkSessionBuilder::new("sc://localhost:15002")
//!     .build()
//!     .await
//!     .expect("failed to connect");
//!
//! println!("Connected to Spark session: {}", session.session_id());
//! # });
//! ```
//!
//! The `SparkSession` provides an ergonomic API for executing SQL, analyzing
//! plans, and inspecting results — without exposing internal client plumbing.
use crate::client::ChannelBuilder;
use crate::client::HeaderInterceptor;
use crate::client::SparkClient;
use crate::spark;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;
use crate::spark::expression::Literal;
use crate::query::SqlQueryBuilder;
use crate::SparkError;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tower::ServiceBuilder;

/// Builder for creating [`SparkSession`] instances.
///
/// Configures a connection to a Spark Connect endpoint
/// following the URL format defined by
/// [Apache Spark's client connection spec](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md).
///
/// # Example
///
/// ```
/// use spark_connect_rs::SparkSessionBuilder;
///
/// # tokio_test::block_on(async {
/// let session = SparkSessionBuilder::new("sc://localhost:15002")
///     .build()
///     .await
///     .unwrap();
///
/// println!("Session ID: {}", session.session_id());
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct SparkSessionBuilder {
    channel_builder: ChannelBuilder,
}

impl SparkSessionBuilder {
    /// Creates a new builder from a Spark Connect connection string.
    ///
    /// The connection string must follow the format:
    /// `sc://<host>:<port>/;key1=value1;key2=value2;...`
    pub fn new(connection: &str) -> Self {
        let channel_builder =
            ChannelBuilder::new(connection).expect("Invalid Spark connection string");
        Self { channel_builder }
    }

    /// Establishes a connection and returns a ready-to-use [`SparkSession`].
    ///
    /// This method performs:
    /// - gRPC channel setup;
    /// - Metadata interceptor attachment;
    /// - [`SparkClient`](crate::SparkClient) initialization.
    pub async fn build(&self) -> Result<SparkSession, SparkError> {
        let channel = Channel::from_shared(self.channel_builder.endpoint())?
            .connect()
            .await?;

        let channel = ServiceBuilder::new().service(channel);

        let grpc_client = SparkConnectServiceClient::with_interceptor(
            channel, HeaderInterceptor::new(
                self.channel_builder.headers().unwrap_or_default()
            )
        );
        let spark_client = SparkClient::new(
            Arc::new(RwLock::new(grpc_client)),
            self.channel_builder.clone(),
        );

        Ok(SparkSession::new(spark_client))
    }
}

/// Represents a logical connection to a Spark Connect backend.
///
/// `SparkSession` is the main entry point for executing commands, analyzing
/// queries, and retrieving results from Spark Connect.
///
/// It wraps an internal [`SparkClient`](crate::SparkClient) and tracks session
/// state (such as the `session_id`).
///
/// # Examples
///
/// ```
/// use spark_connect_rs::SparkSessionBuilder;
///
/// # tokio_test::block_on(async {
/// let session = SparkSessionBuilder::new("sc://localhost:15002")
///     .build()
///     .await
///     .unwrap();
///
/// println!("Session ID: {}", session.session_id());
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct SparkSession {
    client: SparkClient,
    session_id: String,
}

impl SparkSession {
    /// Creates a new session from a [`SparkClient`].
    ///
    /// Usually invoked internally by [`SparkSessionBuilder::build`].
    pub fn new(client: SparkClient) -> Self {
        let session_id = client.session_id().to_string();
        Self { client, session_id }
    }

     /// Returns the unique session identifier for this connection.
    pub fn session_id(&self) -> String {
        self.session_id.to_string()
    }

    /// Returns a mutable reference to the underlying [`SparkClient`].
    ///
    /// While exposed for advanced use cases, typical consumers are advised to rely on
    /// higher-level abstractions in `SparkSession` instead of manipulating the
    /// client directly.
    pub(crate) fn client(&self) -> SparkClient {
        self.client.clone()
    }

    /// Execute a SQL query and return a lazy [`plan`](crate::spark::Plan).
    pub async fn sql(
        &self,
        query: &str,
        params: Vec<Literal>
    ) -> Result<spark::Plan, SparkError> {
        let sql_cmd = spark::command::CommandType::SqlCommand(
            spark::SqlCommand {
                sql: query.to_string(),
                args: Default::default(),
                pos_args: params,
            },
        );

        // Execute plan
        let plan = spark::Plan {
            op_type: Some(spark::plan::OpType::Command(spark::Command {
                command_type: Some(sql_cmd),
            })),
        };
        let mut client = self.client();
        let result = client.execute_plan(plan).await?;

        Ok(spark::Plan {
            op_type: Some(spark::plan::OpType::Root(result.relation()?)),
        })
    }

    /// Alternative ["sqlx-like"](https://docs.rs/sqlx/latest/sqlx/) query interface.
    /// Returns a [`SqlQueryBuilder`] to `bind()` parameters and `execute()`.
    pub fn query(
        &self,
        query: &str,
    ) -> SqlQueryBuilder<'_> {
        SqlQueryBuilder::new(&self, query)
    }

    /// Collect the results from a lazy [`plan`](crate::spark::Plan).
    pub async fn collect(&self, plan: spark::Plan) -> Result<Vec<RecordBatch>, SparkError> {
        let mut client = self.client();

        Ok(client.execute_plan(plan).await?.batches())
    }

    /// Interrupt all running operations.
    pub async fn interrupt_all(&self) -> Result<Vec<String>, SparkError> {
        Ok(
            self.client().interrupt(
                spark::interrupt_request::InterruptType::All,
                None
            ).await?.interrupted_ids()
        )
    }

    /// Interrupt a specific operation by ID.
    pub async fn interrupt_operation(&self, op_id: &str) -> Result<Vec<String>, SparkError> {
        Ok(
            self.client().interrupt(
                spark::interrupt_request::InterruptType::OperationId,
                Some(op_id.to_string()),
            ).await?.interrupted_ids()
        )
    }

    /// Request the version of the Spark Connect server.
    pub async fn version(&self) -> Result<String, SparkError> {
        let version = spark::analyze_plan_request::Analyze::SparkVersion(
            spark::analyze_plan_request::SparkVersion {},
        );

        let mut client = self.client.clone();
        
        Ok(client.analyze(version).await?.spark_version()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::test_utils::setup_session;
    use crate::SparkError;
    
    use arrow::array::{Int32Array, StringArray};
    use regex::Regex;

    #[tokio::test]
    async fn test_session_create() {
        let spark = setup_session().await;
        assert!(spark.is_ok());
    }

    /// Verifies that the client can connect, establish a session, and perform
    /// a basic analysis operation (fetching the Spark version).
    /// This tests `SparkClient::new` and `SparkClient::analyze`.
    #[tokio::test]
    async fn test_session_version() -> Result<(), SparkError> {
        // Arrange: Start server and create a session
        let spark = setup_session().await?;
        
        // Act: The version() method on SparkSession will trigger the
        // underlying SparkClient::analyze call.
        let version = spark.version().await?;

        // Assert: Check for a valid version string
        let re = Regex::new(r"^\d+\.\d+\.\d+$").unwrap();
        assert!(re.is_match(&version), "Version {} invalid", version);
        Ok(())
    }

    /// Verifies that the client can execute a SQL query
    /// and correctly retrieve the resulting Arrow RecordBatches.
    /// This tests `SparkClient::execute_command_and_fetch`.
    #[tokio::test]
    async fn test_sql() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");

        // Act: Execute a simple SQL query.
        let lazy_plan = session
            .sql("SELECT 1 AS id, 'hello' AS text", vec![])
            .await
            .expect("SQL query failed");
        let batches = session
            .collect(lazy_plan)
            .await
            .expect("Failed to collect batches");

        // Assert: Validate the structure and content of the returned data
        assert_eq!(batches.len(), 1, "Expected exactly one RecordBatch");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1, "Expected one row");
        assert_eq!(batch.num_columns(), 2, "Expected two columns");

        // Verify the data in the first column (id)
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Column 0 should be an Int32Array");
        assert_eq!(id_col.value(0), 1);
    }
    
    #[tokio::test]
    async fn test_sql_query_builder_bind() -> Result<(), SparkError> {
        let session = setup_session().await?;

        // Use SqlQueryBuilder and bind parameters
        let batches = session
            .query("SELECT ? AS id, ? AS text")
            .bind(42_i32)
            .bind("world")
            .execute()
            .await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let id_col = batch.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 42);

        let text_col = batch.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text_col.value(0), "world");

        Ok(())
    }
}
