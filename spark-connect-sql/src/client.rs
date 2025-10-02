//! Minimal SparkConnect gRPC client for executing SQL queries and fetching Arrow batches

use std::sync::Arc;
use tokio::sync::RwLock;

use tonic::transport::Channel;
use tonic::codegen::StdError;
use tonic::body::BoxBody;
use tonic::client::GrpcService;
use tonic::codegen::Bytes;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;

use crate::builder::ChannelBuilder;
use crate::error::SparkError;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;
use crate::spark::execute_plan_response::ResponseType;
use crate::spark;

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
            session_id: builder.session_id.clone(),
            stub,
            builder,
        }
    }

    /// Returns the session ID
    pub fn session_id(&self) -> String {
        self.session_id.clone()
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
}

/// A simple wrapper to add headers middleware (used internally)
#[derive(Clone, Debug)]
pub struct HeadersMiddleware<T>(pub T);

impl<T> GrpcService<BoxBody> for HeadersMiddleware<T>
where
    T: GrpcService<BoxBody>,
{
    type ResponseBody = T::ResponseBody;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: tonic::Request<BoxBody>) -> Self::Future {
        // Optionally inject headers here if needed
        self.0.call(req)
    }
}
