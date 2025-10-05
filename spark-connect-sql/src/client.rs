use crate::builder::ChannelBuilder;
use crate::error::SparkError;
use crate::middleware::HeadersMiddleware;
use crate::spark::execute_plan_response::ResponseType;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;
use crate::spark;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use uuid;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;


#[derive(Default, Debug, Clone)]
pub(crate) struct AnalyzeHandler {
    pub(crate) schema: Option<spark::DataType>,
    pub(crate) explain: Option<String>,
    pub(crate) tree_string: Option<String>,
    pub(crate) is_local: Option<bool>,
    pub(crate) is_streaming: Option<bool>,
    pub(crate) input_files: Option<Vec<String>>,
    pub(crate) spark_version: Option<String>,
    pub(crate) ddl_parse: Option<spark::DataType>,
    pub(crate) same_semantics: Option<bool>,
    pub(crate) semantic_hash: Option<i32>,
    pub(crate) get_storage_level: Option<spark::StorageLevel>,
}


/// The minimal Spark client, used internally by `SparkSession`.
#[derive(Clone, Debug)]
pub struct SparkClient {
    stub: Arc<RwLock<SparkConnectServiceClient<HeadersMiddleware<Channel>>>>,
    pub builder: ChannelBuilder,
    user_context: Option<spark::UserContext>,
    session_id: String,
    operation_id: Option<String>,
    analyzer: AnalyzeHandler,
}

impl SparkClient {
    pub fn new(
        stub: Arc<RwLock<SparkConnectServiceClient<HeadersMiddleware<Channel>>>>,
        builder: ChannelBuilder,
    ) -> Self {
        let user_ref = builder.user_id.clone().unwrap_or("".to_string());
        let session_id = builder.session_id.to_string();

        Self {
            stub,
            builder,
            user_context: Some(spark::UserContext {
                user_id: user_ref.clone(),
                user_name: user_ref,
                extensions: vec![],
            }),
            session_id,
            operation_id: None,
            analyzer: AnalyzeHandler::default()
        }
    }

    /// Returns the session ID
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    pub async fn analyze(
        &mut self,
        analyze: spark::analyze_plan_request::Analyze,
    ) -> Result<&mut Self, SparkError> {
        let req = spark::AnalyzePlanRequest {
            session_id: self.session_id.clone(),
            user_context: self.user_context.clone(),
            client_type: self.builder.user_agent.clone(),
            analyze: Some(analyze),
        };

        // clear out any prior responses
        self.analyzer = AnalyzeHandler::default();

        let mut client = self.stub.write().await;
        let resp = client.analyze_plan(req).await?.into_inner();
        drop(client);

        self.handle_analyze(resp)
    }

    fn handle_analyze(
        &mut self,
        resp: spark::AnalyzePlanResponse,
    ) -> Result<&mut Self, SparkError> {
        self.validate_session(&resp.session_id)?;
        if let Some(result) = resp.result {
            match result {
                spark::analyze_plan_response::Result::Schema(schema) => {
                    self.analyzer.schema = schema.schema
                }
                spark::analyze_plan_response::Result::Explain(explain) => {
                    self.analyzer.explain = Some(explain.explain_string)
                }
                spark::analyze_plan_response::Result::TreeString(tree_string) => {
                    self.analyzer.tree_string = Some(tree_string.tree_string)
                }
                spark::analyze_plan_response::Result::IsLocal(is_local) => {
                    self.analyzer.is_local = Some(is_local.is_local)
                }
                spark::analyze_plan_response::Result::IsStreaming(is_streaming) => {
                    self.analyzer.is_streaming = Some(is_streaming.is_streaming)
                }
                spark::analyze_plan_response::Result::InputFiles(input_files) => {
                    self.analyzer.input_files = Some(input_files.files)
                }
                spark::analyze_plan_response::Result::SparkVersion(spark_version) => {
                    self.analyzer.spark_version = Some(spark_version.version)
                }
                spark::analyze_plan_response::Result::DdlParse(ddl_parse) => {
                    self.analyzer.ddl_parse = ddl_parse.parsed
                }
                spark::analyze_plan_response::Result::SameSemantics(same_semantics) => {
                    self.analyzer.same_semantics = Some(same_semantics.result)
                }
                spark::analyze_plan_response::Result::SemanticHash(semantic_hash) => {
                    self.analyzer.semantic_hash = Some(semantic_hash.result)
                }
                spark::analyze_plan_response::Result::Persist(_) => {}
                spark::analyze_plan_response::Result::Unpersist(_) => {}
                spark::analyze_plan_response::Result::GetStorageLevel(level) => {
                    self.analyzer.get_storage_level = level.storage_level
                }
            }
        }

        Ok(self)
    }

    fn validate_session(&self, session_id: &str) -> Result<(), SparkError> {
        if self.builder.session_id.to_string() != session_id {
            return Err(SparkError::AnalysisException(format!(
                "Received incorrect session identifier for request: {0} != {1}",
                self.builder.session_id, session_id
            )));
        }
        Ok(())
    }

    /// Execute a SQL command and fetch Arrow batches
    pub async fn execute_command_and_fetch(
        &mut self,
        sql_cmd: spark::command::CommandType,
    ) -> Result<Vec<RecordBatch>, SparkError> {
        let operation_id = uuid::Uuid::new_v4().to_string();

        self.operation_id = Some(operation_id.clone());

        let req = spark::ExecutePlanRequest {
            session_id: self.session_id.clone(),
            user_context: self.user_context.clone(),
            operation_id: Some(operation_id),
            plan: Some(spark::Plan {
                op_type: Some(spark::plan::OpType::Command(spark::Command {
                    command_type: Some(sql_cmd),
                })),
            }),
            client_type: self.builder.user_agent.clone(),
            request_options: vec![],
            tags: vec![],
        };

        let mut client = self.stub.write().await;
        let mut stream = client.execute_plan(req).await?.into_inner();
        drop(client);

        let mut batches = Vec::new();

        while let Some(resp) = stream.message().await? {
            self.validate_session(&resp.session_id)?;
            if let Some(data) = resp.response_type {
                match data {
                    ResponseType::ArrowBatch(batch) => {
                        let reader = StreamReader::try_new(batch.data.as_slice(), None)?;
                        for batch in reader {
                            batches.push(batch?);
                        }
                    },
                    ResponseType::SqlCommandResult(sql_cmd) => {
                        panic!("{sql_cmd:?}");
                    },
                    ResponseType::ResultComplete(_) => break, // finished
                    _ => {
                        return Err(SparkError::Unimplemented(
                            format!("Handling {data:?} not implemented!")
                        ))
                    }
                }
            }
        }

        Ok(batches)
    }

    pub async fn interrupt_request(
        &self,
        interrupt_type: spark::interrupt_request::InterruptType,
        id_or_tag: Option<String>,
    ) -> Result<spark::InterruptResponse, SparkError> {
        let mut req = spark::InterruptRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.builder.user_agent.clone(),
            interrupt_type: 0,
            interrupt: None,
        };

        match interrupt_type {
            spark::interrupt_request::InterruptType::All => {
                req.interrupt_type = interrupt_type.into();
            }
            spark::interrupt_request::InterruptType::Tag => {
                return Err(SparkError::Unimplemented(
                    "Tag interrupts are not implemented!".to_string()
                ))
            }
            spark::interrupt_request::InterruptType::OperationId => {
                let op_id = id_or_tag.expect("Operation ID can not be empty");
                let interrupt = spark::interrupt_request::Interrupt::OperationId(op_id);
                req.interrupt_type = interrupt_type.into();
                req.interrupt = Some(interrupt);
            }
            spark::interrupt_request::InterruptType::Unspecified => {
                return Err(SparkError::AnalysisException(
                    "Interrupt Type was not specified".to_string(),
                ))
            }
        };

        let mut client = self.stub.write().await;
        let resp = client.interrupt(req).await?.into_inner();
        Ok(resp)
    }

    pub fn spark_version(&mut self) -> Result<String, SparkError> {
        self.analyzer.spark_version.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Spark Version response is empty".to_string())
        })
    }
}


#[cfg(test)]
mod tests {
    use crate::test_utils::test_utils::setup_session;
    use crate::spark;
    
    use arrow::array::Int32Array;
    
    /// **Essential Test 1: Connection and Analysis**
    /// Verifies that the client can connect, establish a session, and perform
    /// a basic analysis operation (fetching the Spark version).
    /// This tests `SparkClient::new` and `SparkClient::analyze`.
    #[tokio::test]
    async fn test_analyze_fetches_spark_version() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");

        // Act: The version() method on SparkSession will trigger the
        // underlying SparkClient::analyze call.
        let version = session
            .version()
            .await
            .expect("Failed to get Spark version");

        // Assert: Check for a valid version string
        assert!(!version.is_empty(), "Version string should not be empty");
        assert!(
            version.starts_with("3.5"),
            "Expected a Spark 3.5.x version"
        );
    }

    /// **Essential Test 2: SQL Execution and Data Fetching**
    /// The most critical test. Verifies that the client can execute a SQL query
    /// and correctly retrieve the resulting Arrow RecordBatches.
    /// This tests `SparkClient::execute_command_and_fetch`.
    #[tokio::test]
    async fn test_execute_and_fetch_simple_sql() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");

        // Act: Execute a simple SQL query. This uses the client's
        // execute_command_and_fetch method internally.
        let batches = session
            .sql("SELECT 1 AS id, 'hello' AS text")
            .await
            .expect("SQL query failed");

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

    /// **Essential Test 3: Session Validation Error**
    /// Verifies that the client correctly handles and reports errors, such as
    /// a session validation failure.
    #[tokio::test]
    async fn test_validate_session_error() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");

        // Create a clone of the client and manually corrupt its session ID
        let mut client_with_bad_session = session.client().clone();
        client_with_bad_session.session_id = "invalid-session-id".to_string();

        // Act: Attempt to use the corrupted client. This will cause the real server
        // to return an error that Spark Connect may not map directly to a session
        // ID mismatch, but it will be an error nonetheless.
        let result = client_with_bad_session
            .analyze(spark::analyze_plan_request::Analyze::SparkVersion(
                spark::analyze_plan_request::SparkVersion {},
            ))
            .await;

        // Assert: The operation should fail.
        // NOTE: The real server might return a more generic "INVALID_HANDLE" or
        // "SESSION_NOT_FOUND" error rather than a mismatched ID error.
        // We just check that an error of some kind occurred.
        assert!(
            result.is_err(),
            "Expected an error due to invalid session ID"
        );
    }
    
    /// **Essential Test 4: Interrupt Request**
    /// Verifies that the client can send an interrupt request without errors.
    /// This tests the `SparkClient::interrupt_request` method.
    #[tokio::test]
    async fn test_interrupt_all_request() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");
        
        // Act: Send an "interrupt all" request. The server should accept this
        // command gracefully even if nothing is running.
        let result = session
            .client()
            .interrupt_request(spark::interrupt_request::InterruptType::All, None)
            .await;
            
        // Assert: The request should succeed. The response may be empty.
        assert!(result.is_ok(), "Interrupt request should not fail");
        let response = result.unwrap();
        assert_eq!(response.session_id, session.session_id());
    }
}