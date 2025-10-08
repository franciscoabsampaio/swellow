mod builder;
mod handlers;
mod middleware;

pub use self::builder::ChannelBuilder;
pub use self::middleware::HeaderInterceptor;
use self::handlers::{AnalyzeHandler, ExecuteHandler, InterruptHandler};
use crate::spark;
use crate::spark::spark_connect_service_client::SparkConnectServiceClient;
use crate::spark::execute_plan_response::ResponseType;
use crate::SparkError;

use arrow::array::RecordBatch;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::codec::Streaming;
use tonic::transport::Channel;
use uuid;


/// Utility type to reduce boilerplate.
type InterceptedChannel = tonic::service::interceptor::InterceptedService<Channel, HeaderInterceptor>;


/// The Spark client used internally by [`SparkSession`](crate::SparkSession).
#[derive(Clone, Debug)]
pub struct SparkClient {
    stub: Arc<RwLock<SparkConnectServiceClient<InterceptedChannel>>>,
    pub(crate) builder: ChannelBuilder,
    user_context: Option<spark::UserContext>,
    use_reattachable_execute: bool,
    session_id: String,
    operation_id: Option<String>,
    response_id: Option<String>,
    handler_analyze: AnalyzeHandler,
    handler_execute: ExecuteHandler,
    handler_interrupt: InterruptHandler,
}

impl SparkClient {
    pub fn new(
        stub: Arc<RwLock<SparkConnectServiceClient<InterceptedChannel>>>,
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
            response_id: None,
            handler_analyze: AnalyzeHandler::default(),
            handler_execute: ExecuteHandler::default(),
            handler_interrupt: InterruptHandler::default(),
            use_reattachable_execute: true,
        }
    }

    /// Return session id
    pub fn session_id(&self) -> String {
        self.session_id.to_string()
    }

    /// Return the spark version.
    pub fn spark_version(&self) -> Result<String, SparkError> {
        self.handler_analyze.spark_version.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Spark Version response is empty".to_string())
        })
    }

    /// Return interrupt ids.
    pub fn interrupted_ids(&self) -> Vec<String> {
        self.handler_interrupt.interrupted_ids.to_owned()
    }

    /// Return relation response.
    pub fn relation(&self) -> Result<spark::Relation, SparkError> {
        self.handler_execute.relation.to_owned().ok_or_else(|| {
            SparkError::AnalysisException("Relation response is empty".to_string())
        })
    }

    /// Return batches in response handler.
    pub fn batches(&self) -> Vec<RecordBatch> {
        self.handler_execute.batches.to_owned()
    }

    fn validate_session(&self, session_id: &str) -> Result<(), SparkError> {
        if self.session_id() != session_id {
            return Err(SparkError::AnalysisException(format!(
                "Received incorrect session identifier for request: {0} != {1}",
                self.builder.session_id, session_id
            )));
        }
        Ok(())
    }

    /// Execute an [analyze request](crate::spark::analyze_plan_request::Analyze).
    pub async fn analyze(
        &mut self,
        analyze: spark::analyze_plan_request::Analyze,
    ) -> Result<&mut Self, SparkError> {
        let req = spark::AnalyzePlanRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            client_type: self.builder.user_agent.clone(),
            analyze: Some(analyze),
        };
        
        let mut client = self.stub.write().await;
        let resp = client.analyze_plan(req).await?.into_inner();
        drop(client);
        
        self.handle_analyze_response(resp)?;
        
        Ok(self)
    }

    fn handle_analyze_response(
        &mut self,
        resp: spark::AnalyzePlanResponse,
    ) -> Result<(), SparkError> {
        self.validate_session(&resp.session_id)?;

        // clear out any prior responses
        self.handler_analyze = AnalyzeHandler::default();
        
        if let Some(result) = resp.result {
            match result {
                spark::analyze_plan_response::Result::Schema(schema) => {
                    self.handler_analyze.schema = schema.schema
                }
                // spark::analyze_plan_response::Result::Explain(explain) => {
                //     self.handler_analyze.explain = Some(explain.explain_string)
                // }
                // spark::analyze_plan_response::Result::TreeString(tree_string) => {
                //     self.handler_analyze.tree_string = Some(tree_string.tree_string)
                // }
                // spark::analyze_plan_response::Result::IsLocal(is_local) => {
                //     self.handler_analyze.is_local = Some(is_local.is_local)
                // }
                // spark::analyze_plan_response::Result::IsStreaming(is_streaming) => {
                //     self.handler_analyze.is_streaming = Some(is_streaming.is_streaming)
                // }
                // spark::analyze_plan_response::Result::InputFiles(input_files) => {
                //     self.handler_analyze.input_files = Some(input_files.files)
                // }
                spark::analyze_plan_response::Result::SparkVersion(spark_version) => {
                    self.handler_analyze.spark_version = Some(spark_version.version)
                }
                // spark::analyze_plan_response::Result::DdlParse(ddl_parse) => {
                //     self.handler_analyze.ddl_parse = ddl_parse.parsed
                // }
                // spark::analyze_plan_response::Result::SameSemantics(same_semantics) => {
                //     self.handler_analyze.same_semantics = Some(same_semantics.result)
                // }
                // spark::analyze_plan_response::Result::SemanticHash(semantic_hash) => {
                //     self.handler_analyze.semantic_hash = Some(semantic_hash.result)
                // }
                // spark::analyze_plan_response::Result::Persist(_) => {}
                // spark::analyze_plan_response::Result::Unpersist(_) => {}
                // spark::analyze_plan_response::Result::GetStorageLevel(level) => {
                //     self.handler_analyze.get_storage_level = level.storage_level
                // }
                _ => return Err(SparkError::Unimplemented(format!(
                    "Handling of analyze response {result:?} not implemented!"
                )))
            }
        }

        Ok(())
    }

    /// Execute an [interrupt request](crate::spark::InterruptRequest).
    pub async fn interrupt(
        &mut self,
        interrupt_type: spark::interrupt_request::InterruptType,
        id_or_tag: Option<String>,
    ) -> Result<&mut Self, SparkError> {
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
        drop(client);
        
        self.handler_interrupt = InterruptHandler::default();
        self.handler_interrupt.interrupted_ids = resp.interrupted_ids;
        
        Ok(self)
    }
    

    /// Execute a [plan execution request](crate::spark::ExecutePlanRequest).
    pub async fn execute_plan(
        &mut self,
        plan: spark::Plan
    ) -> Result<&mut Self, SparkError> {
        let mut request = self.new_execute_plan_request();
        request.plan = Some(plan);

        let mut client = self.stub.write().await;
        let mut stream = client
            .execute_plan(request)
            .await?
            .into_inner();
        drop(client);

        self.handler_execute = ExecuteHandler::default();
        self.process_stream(&mut stream).await?;
        
        if self.use_reattachable_execute && self.handler_execute.result_complete {
            self.release_all().await?;
        }
        
        Ok(self)
    }

    fn new_execute_plan_request(&mut self) -> spark::ExecutePlanRequest {
        let operation_id = uuid::Uuid::new_v4().to_string();

        self.operation_id = Some(operation_id.clone());

        spark::ExecutePlanRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: Some(operation_id),
            plan: None,
            client_type: self.builder.user_agent.clone(),
            request_options: vec![spark::execute_plan_request::RequestOption {
                request_option: Some(
                    spark::execute_plan_request::request_option::RequestOption::ReattachOptions(
                        spark::ReattachOptions { reattachable: self.use_reattachable_execute },
                    ),
                ),
            }],
            tags: vec![],
        }
    }
    
    fn handle_execute_response(
        &mut self,
        resp: spark::ExecutePlanResponse
    ) -> Result<(), SparkError> {
        self.validate_session(&resp.session_id)?;

        self.operation_id = Some(resp.operation_id);
        self.response_id = Some(resp.response_id);

        if let Some(data) = resp.response_type {
            match data {
                ResponseType::ArrowBatch(res) => {
                    let (batches, total_count) = crate::io::deserialize(res.data.as_slice(), res.row_count)?;

                    self.handler_execute.batches.extend(batches);
                    self.handler_execute.total_count += total_count;
                }
                ResponseType::SqlCommandResult(sql_cmd) => {
                    self.handler_execute.relation = sql_cmd.clone().relation
                }
                // ResponseType::WriteStreamOperationStartResult(write_stream_op) => {
                //     self.handler.write_stream_operation_start_result = Some(write_stream_op)
                // }
                // ResponseType::StreamingQueryCommandResult(stream_qry_cmd) => {
                //     self.handler.streaming_query_command_result = Some(stream_qry_cmd)
                // }
                // ResponseType::GetResourcesCommandResult(resource_cmd) => {
                //     self.handler.get_resources_command_result = Some(resource_cmd)
                // }
                // ResponseType::StreamingQueryManagerCommandResult(stream_qry_mngr_cmd) => {
                //     self.handler.streaming_query_manager_command_result = Some(stream_qry_mngr_cmd)
                // }
                ResponseType::ResultComplete(_) => self.handler_execute.result_complete = true,
                _ => return Err(SparkError::Unimplemented(
                    format!("Handling of plan response {data:?} not implemented!")
                ))
            }
        }
        Ok(())
    }

    /// Execute an [execution reattachment request](crate::spark::ReattachExecuteRequest).
    async fn reattach(&mut self) -> Result<(), SparkError> {
        let request = spark::ReattachExecuteRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: self.operation_id.clone().unwrap(),
            client_type: self.builder.user_agent.clone(),
            last_response_id: self.response_id.clone(),
        };

        let mut client = self.stub.write().await;
        let mut stream = client
            .reattach_execute(request)
            .await?
            .into_inner();
        drop(client);

        self.process_stream(&mut stream).await?;
        
        if self.use_reattachable_execute && self.handler_execute.result_complete {
            self.release_all().await?;
        }

        Ok(())
    }
    
    async fn process_stream(
        &mut self, stream: &mut Streaming<spark::ExecutePlanResponse>
    ) -> Result<(), SparkError> {
        while let Some(_resp) = match stream.message().await {
            Ok(Some(msg)) => {
                self.handle_execute_response(msg.clone())?;
                Some(msg)
            }
            Ok(None) => {
                if self.use_reattachable_execute && !self.handler_execute.result_complete {
                    Box::pin(self.reattach()).await?;
                }
                None
            }
            Err(err) => {
                if self.use_reattachable_execute && self.response_id.is_some() {
                    self.release_until().await?;
                }
                return Err(err.into());
            }
        } {}

        Ok(())
    }

    async fn release_until(&mut self) -> Result<(), SparkError> {
        let release_until = spark::release_execute_request::ReleaseUntil {
            response_id: self.response_id.clone().unwrap(),
        };

        self.release_execute(spark::release_execute_request::Release::ReleaseUntil(
            release_until,
        )).await
    }

    async fn release_all(&mut self) -> Result<(), SparkError> {
        let release_all = spark::release_execute_request::ReleaseAll {};

        self.release_execute(spark::release_execute_request::Release::ReleaseAll(
            release_all,
        )).await
    }

    async fn release_execute(
        &mut self,
        release: spark::release_execute_request::Release,
    ) -> Result<(), SparkError> {
        let mut client = self.stub.write().await;

        let req = spark::ReleaseExecuteRequest {
            session_id: self.session_id(),
            user_context: self.user_context.clone(),
            operation_id: self.operation_id.clone().unwrap(),
            client_type: self.builder.user_agent.clone(),
            release: Some(release),
        };

        let _resp = client.release_execute(req).await?.into_inner();

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use crate::test_utils::test_utils::setup_session;
    use crate::spark;
    
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
        assert!(
            result.is_err(),
            "Expected an error due to invalid session ID"
        );
    }
    
    /// Verifies that the client can send an interrupt request without errors.
    /// This tests the `SparkClient::interrupt_request` method.
    #[tokio::test]
    async fn test_interrupt_all_request() {
        // Arrange: Start server and create a session
        let session = setup_session().await.expect("Failed to create Spark session");
        
        // Act: Send an "interrupt all" request. The server should accept this
        // command gracefully even if nothing is running.
        let mut client = session.client();
        let result = client
            .interrupt(spark::interrupt_request::InterruptType::All, None)
            .await
            .unwrap();
            
        // Assert: The request should succeed. The response may be empty.
        assert_eq!(result.session_id(), session.session_id());
    }
}