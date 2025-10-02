use core::fmt;
use uuid;


/// Different `Spark` Error types
#[derive(Debug)]
pub enum SparkError {
    InvalidConnectionUrl(String),
    InvalidUUID(uuid::Error)
}

impl fmt::Display for SparkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SparkError::InvalidConnectionUrl(msg) => write!(f, "Invalid Connection URL: {}", msg),
            SparkError::InvalidUUID(e) => write!(f, "Invalid UUID: {}", e)
        }
    }
}

impl From<uuid::Error> for SparkError {
    fn from(error: uuid::Error) -> Self {
        SparkError::InvalidUUID(error)
    }
}