use core::fmt;
use tonic::Code;
use uuid;


/// Different `Spark` Error types
#[derive(Debug)]
pub enum SparkError {
    Aborted(String),
    AnalysisException(String),
    Cancelled(String),
    DataLoss(String),
    DeadlineExceeded(String),
    FailedPrecondition(String),
    InvalidArgument(String),
    InvalidConnectionUrl(String),
    InvalidUUID(uuid::Error),
    NotFound(String),
    OutOfRange(String),
    PermissionDenied(String),
    ResourceExhausted(String),
    Unknown(String),
    Unavailable(String),
    Unimplemented(String),
    Unauthenticated(String),
}

impl fmt::Display for SparkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SparkError::Aborted(msg) => write!(f, "Aborted: {}", msg),
            SparkError::AnalysisException(msg) => write!(f, "Analysis Exception: {}", msg),
            SparkError::Cancelled(msg) => write!(f, "Cancelled: {}", msg),
            SparkError::DataLoss(msg) => write!(f, "Data Loss: {}", msg),
            SparkError::DeadlineExceeded(msg) => write!(f, "Deadline Exceeded: {}", msg),
            SparkError::FailedPrecondition(msg) => write!(f, "Failed Precondition: {}", msg),
            SparkError::InvalidArgument(msg) => write!(f, "Invalid Argument: {}", msg),
            SparkError::InvalidConnectionUrl(msg) => write!(f, "Invalid Connection URL: {}", msg),
            SparkError::InvalidUUID(e) => write!(f, "Invalid UUID: {}", e),
            SparkError::NotFound(msg) => write!(f, "Not Found: {}", msg),
            SparkError::OutOfRange(msg) => write!(f, "Out Of Range: {}", msg),
            SparkError::PermissionDenied(msg) => write!(f, "Permission Denied: {}", msg),
            SparkError::ResourceExhausted(msg) => write!(f, "Resource Exhausted: {}", msg),
            SparkError::Unknown(msg) => write!(f, "Unknown: {}", msg),
            SparkError::Unavailable(msg) => write!(f, "Unavailable: {}", msg),
            SparkError::Unimplemented(msg) => write!(f, "Unimplemented: {}", msg),
            SparkError::Unauthenticated(msg) => write!(f, "Unauthenticated: {}", msg),
        }
    }
}

impl From<arrow::error::ArrowError> for SparkError {
    fn from(error: arrow::error::ArrowError) -> Self {
        SparkError::InvalidConnectionUrl(error.to_string())
    }
}

impl From<uuid::Error> for SparkError {
    fn from(error: uuid::Error) -> Self {
        SparkError::InvalidUUID(error)
    }
}

impl From<tonic::codegen::http::uri::InvalidUri> for SparkError {
    fn from(error: tonic::codegen::http::uri::InvalidUri) -> Self {
        SparkError::InvalidConnectionUrl(error.to_string())
    }
}

impl From<tonic::transport::Error> for SparkError {
    fn from(error: tonic::transport::Error) -> Self {
        SparkError::InvalidConnectionUrl(error.to_string())
    }
}

impl From<tonic::Status> for SparkError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            Code::Ok => SparkError::AnalysisException(status.message().to_string()),
            Code::Unknown => SparkError::Unknown(status.message().to_string()),
            Code::Aborted => SparkError::Aborted(status.message().to_string()),
            Code::NotFound => SparkError::NotFound(status.message().to_string()),
            Code::Internal => SparkError::AnalysisException(status.message().to_string()),
            Code::DataLoss => SparkError::DataLoss(status.message().to_string()),
            Code::Cancelled => SparkError::Cancelled(status.message().to_string()),
            Code::OutOfRange => SparkError::OutOfRange(status.message().to_string()),
            Code::Unavailable => SparkError::Unavailable(status.message().to_string()),
            Code::AlreadyExists => SparkError::AnalysisException(status.message().to_string()),
            Code::InvalidArgument => SparkError::InvalidArgument(status.message().to_string()),
            Code::DeadlineExceeded => SparkError::DeadlineExceeded(status.message().to_string()),
            Code::Unimplemented => SparkError::Unimplemented(status.message().to_string()),
            Code::Unauthenticated => SparkError::Unauthenticated(status.message().to_string()),
            Code::PermissionDenied => SparkError::PermissionDenied(status.message().to_string()),
            Code::ResourceExhausted => SparkError::ResourceExhausted(status.message().to_string()),
            Code::FailedPrecondition => SparkError::FailedPrecondition(status.message().to_string())
        }
    }
}