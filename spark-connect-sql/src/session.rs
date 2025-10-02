use crate::builder::ChannelBuilder;
use crate::client::{SparkClient, deserialize_arrow};
use crate::error::SparkError;
use crate::middleware::HeadersLayer;
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

        let channel = ServiceBuilder::new()
            .layer(HeadersLayer::new(
                self.channel_builder.headers().unwrap_or_default(),
            ))
            .service(channel);

        let grpc_client = SparkConnectServiceClient::new(channel);
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
        let session_id = client.session_id();
        Self { client, session_id }
    }

    /// Return the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Return a clone of the client
    pub fn client(&self) -> SparkClient {
        self.client.clone()
    }

    /// Execute a SQL query and return Arrow record batches directly.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>, SparkError> {
        let sql_cmd = crate::spark::command::CommandType::SqlCommand(
            crate::spark::SqlCommand {
                sql: query.to_string(),
                args: Default::default(),
                pos_args: vec![],
            },
        );

        // Execute command and fetch response
        let resp = self.client.clone()
            .execute_command_and_fetch(sql_cmd.into())
            .await?;

        // Deserialize Arrow batches
        let mut batches = Vec::new();
        for batch_bytes in resp.batches {
            batches.push(deserialize_arrow(&batch_bytes)?);
        }

        Ok(batches)
    }

    /// Return the Spark version for this session
    pub async fn version(&self) -> Result<String, SparkError> {
        let analyze = crate::spark::analyze_plan_request::Analyze::SparkVersion(
            crate::spark::analyze_plan_request::SparkVersion {},
        );
        self.client.clone().analyze(analyze).await?.spark_version()
    }

    /// Interrupt all running operations
    pub async fn interrupt_all(&self) -> Result<Vec<String>, SparkError> {
        let resp = self.client
            .interrupt_request(crate::spark::interrupt_request::InterruptType::All, None)
            .await?;
        Ok(resp.interrupted_ids)
    }

    /// Interrupt a specific operation by ID
    pub async fn interrupt_operation(&self, op_id: &str) -> Result<Vec<String>, SparkError> {
        let resp = self.client
            .interrupt_request(
                crate::spark::interrupt_request::InterruptType::OperationId,
                Some(op_id.to_string()),
            )
            .await?;
        Ok(resp.interrupted_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;

    async fn setup_session() -> SparkSession {
        let connection = "sc://127.0.0.1:15002/;user_id=rust_test;session_id=0d2af2a9-cc3c-4d4b-bf27-e2fefeaca233";
        SparkSessionBuilder::new(connection).build().await.unwrap()
    }

    #[tokio::test]
    async fn test_session_create() {
        let connection = "sc://localhost:15002/;token=ABCDEFG;user_agent=agent;user_id=user123";
        let spark = SparkSessionBuilder::new(connection).build().await;
        assert!(spark.is_ok());
    }

    #[tokio::test]
    async fn test_session_version() -> Result<(), SparkError> {
        let spark = setup_session().await;
        let version = spark.version().await?;
        let re = Regex::new(r"^\d+\.\d+\.\d+$").unwrap();
        assert!(re.is_match(&version), "Version {} invalid", version);
        Ok(())
    }
}
