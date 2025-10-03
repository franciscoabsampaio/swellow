use crate::builder::ChannelBuilder;
use crate::error::SparkError;
use crate::middleware::HeadersMiddleware;
use crate::spark::execute_plan_response::ResponseType;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;
use crate::spark;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;

/// The minimal Spark client, used internally by `SparkSession`.
#[derive(Clone, Debug)]
pub struct SparkClient {
    stub: Arc<RwLock<SparkConnectServiceClient<HeadersMiddleware<Channel>>>>,
    builder: ChannelBuilder,
    session_id: String,
}

impl SparkClient {
    pub fn new(
        stub: Arc<RwLock<SparkConnectServiceClient<HeadersMiddleware<Channel>>>>,
        builder: ChannelBuilder,
    ) -> Self {
        Self {
            session_id: builder.session_id.to_string(),
            stub,
            builder,
        }
    }

    /// Returns the session ID
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    pub async fn analyze(
        &self,
        analyze: spark::analyze_plan_request::Analyze,
    ) -> Result<spark::AnalyzePlanResponse, SparkError> {
        let req = spark::AnalyzePlanRequest {
            session_id: self.session_id.clone(),
            user_context: None,
            analyze: Some(analyze),
        };

        let mut client = self.stub.write().await;
        let resp = client.analyze_plan(req).await?.into_inner();
        Ok(resp)
    }

    /// Execute a SQL command and fetch Arrow batches
    pub async fn execute_command_and_fetch(
        &self,
        sql_cmd: spark::command::CommandType,
    ) -> Result<Vec<RecordBatch>, SparkError> {
        let req = spark::ExecutePlanRequest {
            session_id: self.session_id.clone(),
            user_context: None,
            operation_id: None,
            plan: Some(sql_cmd.into()), // directly into plan
            client_type: self.builder.user_agent.clone(),
            request_options: vec![],
            tags: vec![],
        };

        let mut client = self.stub.write().await;
        let mut stream = client.execute_plan(req).await?.into_inner();
        drop(client);

        let mut batches = Vec::new();

        while let Some(resp) = stream.message().await? {
            if let Some(schema) = &resp.schema {
                // optional: validate schema if needed
            }
            if let Some(data) = resp.response_type {
                match data {
                    ResponseType::ArrowBatch(batch) => {
                        let reader = StreamReader::try_new(batch.data.as_slice(), None)?;
                        for batch in reader {
                            batches.push(batch?);
                        }
                    }
                    ResponseType::ResultComplete(_) => break, // finished
                    _ => {} // ignore anything else
                }
            }
        }

        Ok(batches)
    }

    pub async fn interrupt_request(
        &self,
        interrupt_type: spark::interrupt_request::InterruptType,
        op_id: Option<String>,
    ) -> Result<spark::InterruptResponse, SparkError> {
        let req = spark::InterruptRequest {
            session_id: self.session_id.clone(),
            interrupt_type: Some(interrupt_type.into()),
            operation_id: op_id,
        };

        let mut client = self.stub.write().await;
        let resp = client.interrupt(req).await?.into_inner();
        Ok(resp)
    }
}
