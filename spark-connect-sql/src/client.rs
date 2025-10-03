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
    builder: ChannelBuilder,
    session_id: String,
    user_context: Option<spark::UserContext>,
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
            session_id,
            stub,
            builder,
            user_context: Some(spark::UserContext {
                user_id: user_ref.clone(),
                user_name: user_ref,
                extensions: vec![],
            }),
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
            user_context: None,
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
        &self,
        sql_cmd: spark::command::CommandType,
    ) -> Result<Vec<RecordBatch>, SparkError> {
        let req = spark::ExecutePlanRequest {
            session_id: self.session_id.clone(),
            user_context: None,
            operation_id: None,
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
                    ResponseType::ResultComplete(_) => break, // finished
                    _ => {
                        return Err(SparkError::Unimplemented(
                            format!("Handling {:?} not implemented!", data)
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
