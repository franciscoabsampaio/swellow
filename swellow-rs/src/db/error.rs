use std::error::Error;
use std::fmt;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;


#[derive(Debug)]
#[non_exhaustive]
pub struct EngineError {
    pub kind: EngineErrorKind
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EngineError: {}", self.kind)
    }
}

impl Error for EngineError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		Some(&self.kind)
	}
}

#[derive(Debug)]
pub enum EngineErrorKind {
    ColumnTypeMismatch {
        column_index: usize,
        expected: &'static str,
        found: DataType,
    },
    LockConflict,
    PGDump(Vec<u8>),
    Process{ source: std::io::Error, cmd: String },
    Spark(spark_connect::SparkError),
    SQLX(sqlx::Error),
}

impl fmt::Display for EngineErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnTypeMismatch { column_index, expected, found } => {
                write!(f, "Column {} has mismatched type: expected {}, found {:?}", column_index, expected, found)
            },
            Self::LockConflict => write!(f, "Lock acquisition failed - lock record is taken"),
            Self::PGDump(stderr) => write!(f, "pg_dump failed: '{stderr:?}'"),
            Self::Process{cmd, .. } => write!(f, "Failed to run a command: '{cmd}'"),
            Self::SQLX(_) => write!(f, "sqlx::Error"),
            Self::Spark(_) => write!(f, "SparkError"),
        }
    }
}

impl Error for EngineErrorKind {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::Process { source, .. } => Some(source),
			Self::SQLX(source) => Some(source),
			Self::Spark(source) => Some(source),
			_ => None,
		}
	}
}

impl From<sqlx::Error> for EngineError {
    fn from(error: sqlx::Error) -> Self {
        EngineError { kind: EngineErrorKind::SQLX(error) }
    }
}

impl From<spark_connect::SparkError> for EngineError {
    fn from(error: spark_connect::SparkError) -> Self {
        EngineError { kind: EngineErrorKind::Spark(error) }
    }
}