use crate::builder::ChannelBuilder;
use crate::client::SparkClient;
use crate::error::SparkError;
use crate::middleware::HeaderInterceptor;
use crate::spark;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tower::ServiceBuilder;

/// Builder for creating remote Spark sessions.
#[derive(Clone, Debug)]
pub struct SparkSessionBuilder {
    channel_builder: ChannelBuilder,
}

impl SparkSessionBuilder {
    pub fn new(connection: &str) -> Self {

        let channel_builder =
            ChannelBuilder::create(connection).expect("Invalid Spark connection string");
        Self { channel_builder }
    }

    /// Connects to Spark and returns a `SparkSession`.
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

/// Represents an active Spark session.
/// All queries return Arrow batches directly.
#[derive(Clone, Debug)]
pub struct SparkSession {
    client: SparkClient,
    session_id: String,
}

impl SparkSession {
    pub fn new(client: SparkClient) -> Self {
        let session_id = client.session_id().to_string();
        Self { client, session_id }
    }

    /// Return the session ID
    pub fn session_id(&self) -> String {
        self.session_id.to_string()
    }

    /// Return a clone of the client
    pub fn client(&self) -> SparkClient {
        self.client.clone()
    }

    /// Execute a SQL query and return a plan (lazy).
    pub async fn sql(&self, query: &str) -> Result<spark::Plan, SparkError> {
        let sql_cmd = spark::command::CommandType::SqlCommand(
            spark::SqlCommand {
                sql: query.to_string(),
                args: Default::default(),
                pos_args: vec![],
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

    /// Collect the results from a lazy plan
    pub async fn collect(&self, plan: spark::Plan) -> Result<Vec<RecordBatch>, SparkError> {
        let mut client = self.client();

        Ok(client.execute_plan(plan).await?.batches())
    }

    /// Interrupt all running operations
    pub async fn interrupt_all(&self) -> Result<Vec<String>, SparkError> {
        Ok(
            self.client().interrupt(
                spark::interrupt_request::InterruptType::All,
                None
            ).await?.interrupted_ids()
        )
    }

    /// Interrupt a specific operation by ID
    pub async fn interrupt_operation(&self, op_id: &str) -> Result<Vec<String>, SparkError> {
        Ok(
            self.client().interrupt(
                spark::interrupt_request::InterruptType::OperationId,
                Some(op_id.to_string()),
            ).await?.interrupted_ids()
        )
    }

    /// The version of Spark on which this application is running.
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
    
    use super::*;
    
    use arrow::array::Int32Array;
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
            .sql("SELECT 1 AS id, 'hello' AS text")
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
}
