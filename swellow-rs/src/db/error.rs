use std::error::Error;
use std::fmt;

use arrow::datatypes::DataType;


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
    Fmt(std::fmt::Error),
    LockConflict,
    PGDump(String),
    Process{ source: std::io::Error, cmd: String },
    Spark(spark_connect::SparkError),
    SQLX(sqlx::Error),
    TransactionNotStarted,
    Utf8(std::string::FromUtf8Error),
}

impl fmt::Display for EngineErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnTypeMismatch { column_index, expected, found } => {
                write!(f, "Column {} has mismatched type: expected {}, found {:?}", column_index, expected, found)
            },
            Self::Fmt(e) => write!(f, "Formatting error: {e}"),
            Self::LockConflict => write!(f, "Lock acquisition failed - lock record is taken"),
            Self::PGDump(stderr) => write!(f, "pg_dump failed: '{stderr}'"),
            Self::Process{cmd, .. } => write!(f, "Failed to run a command: '{cmd}'"),
            Self::SQLX(e) => write!(f, "{e}"),
            Self::Spark(e) => write!(f, "1{e}"),
            Self::TransactionNotStarted => write!(f, "Transaction has not been started"),
            Self::Utf8(e) => write!(f, "UTF-8 conversion error: {e}"),
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

impl From<std::fmt::Error> for EngineError {
    fn from(error: std::fmt::Error) -> Self {
        EngineError { kind: EngineErrorKind::Fmt(error) }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, io};
    use arrow::datatypes::DataType;
    use sqlx;

    #[test]
    fn engine_error_display_formats_correctly() {
        let cases: Vec<(EngineErrorKind, &str)> = vec![
            (
                EngineErrorKind::ColumnTypeMismatch {
                    column_index: 1,
                    expected: "Int64",
                    found: DataType::Utf8,
                },
                "Column 1 has mismatched type",
            ),
            (EngineErrorKind::LockConflict, "Lock acquisition failed"),
            (EngineErrorKind::PGDump("Big mistake".to_string()), "pg_dump failed"),
            (EngineErrorKind::SQLX(sqlx::Error::RowNotFound), "no rows returned"),
        ];

        for (kind, expect) in cases {
            let text = kind.to_string();
            assert!(
                text.contains(expect),
                "Expected `{}` in `{}`",
                expect,
                text
            );
        }
    }

    #[test]
    fn engine_error_source_is_accessible() {
        let io_err = io::Error::new(io::ErrorKind::Other, "io fail");
        let kind = EngineErrorKind::Process {
            source: io_err,
            cmd: "echo".into(),
        };
        let src = kind.source().unwrap().to_string();
        assert!(src.contains("io fail"));
    }

    #[test]
    fn engine_error_from_sqlx() {
        let sqlx_err = sqlx::Error::RowNotFound;

        let e1: EngineError = sqlx_err.into();

        assert!(matches!(e1.kind, EngineErrorKind::SQLX(_)));
    }
}