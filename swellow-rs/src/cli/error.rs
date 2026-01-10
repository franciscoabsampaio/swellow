use crate::{cli::Engine, db::EngineError, parser::ParseError};

use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use tracing::subscriber::SetGlobalDefaultError;


#[derive(Debug)]
pub struct SwellowError {
    pub kind: SwellowErrorKind
}

impl fmt::Display for SwellowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SwellowError: {}", self.kind)
    }
}

impl Error for SwellowError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.kind)
	}
}

#[derive(Debug)]
pub enum SwellowErrorKind {
    DryRunUnsupportedEngine(Engine),
    Engine(EngineError),
    InvalidVersionInterval(i64, i64),
    IoDirectoryCreate { source: std::io::Error, path: PathBuf},
    IoFileWrite { source: std::io::Error, path: PathBuf},
    Parse(ParseError),
    SetGlobalDefault(SetGlobalDefaultError),
}

impl fmt::Display for SwellowErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DryRunUnsupportedEngine(engine) => write!(f, "Dry run is not supported for engine: {engine:?}"),
            Self::Engine(error) => write!(f, "{}", error.kind),
            Self::InvalidVersionInterval(from, to) => write!(f, "Invalid version interval: from ({from}) > to ({to})"),
            Self::IoDirectoryCreate { path, .. } => write!(f, "Failed to create directory: '{path:?}'"),
            Self::IoFileWrite { path, .. } => write!(f, "Failed to write to file: '{path:?}'"),
            Self::Parse(error) => write!(f, "{}", error.kind),
            Self::SetGlobalDefault(error) => write!(f, "Failed to set global default subscriber: {}", error),
        }
        
    }
}

impl Error for SwellowErrorKind {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::Engine(source) => Some(source),
			Self::IoDirectoryCreate { source, .. } => Some(source),
			Self::IoFileWrite { source, .. } => Some(source),
            Self::Parse(source) => Some(source),
            _ => None
		}
	}
}

impl From<EngineError> for SwellowError {
    fn from(error: EngineError) -> Self {
        SwellowError { kind: SwellowErrorKind::Engine(error) }
    }
}

impl From<SetGlobalDefaultError> for SwellowError {
    fn from(error: SetGlobalDefaultError) -> Self {
        SwellowError { kind: SwellowErrorKind::SetGlobalDefault(error) }
    }
}

impl From<ParseError> for SwellowError {
    fn from(error: ParseError) -> Self {
        SwellowError { kind: SwellowErrorKind::Parse(error) }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::{EngineError, EngineErrorKind}, parser::{ParseError, ParseErrorKind}};
    use std::{io, error::Error};
    
    #[test]
    fn swellow_error_display_formats_correctly() {
        let engine_err = EngineError {
            kind: EngineErrorKind::LockConflict,
        };
        let parse_err = ParseError {
            kind: ParseErrorKind::InvalidVersionNumber("abc".into()),
        };
        let path = std::path::PathBuf::from("/tmp/file.sql");

        let cases: Vec<(SwellowErrorKind, &str)> = vec![
            (SwellowErrorKind::Engine(engine_err), "Lock acquisition failed"),
            (SwellowErrorKind::InvalidVersionInterval(10, 5), "Invalid version interval"),
            (
                SwellowErrorKind::IoDirectoryCreate {
                    source: io::Error::new(io::ErrorKind::Other, "disk full"),
                    path: path.clone(),
                },
                "Failed to create directory",
            ),
            (
                SwellowErrorKind::IoFileWrite {
                    source: io::Error::new(io::ErrorKind::Other, "disk full"),
                    path: path.clone(),
                },
                "Failed to write to file",
            ),
            (SwellowErrorKind::Parse(parse_err), "Invalid version number"),
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
    fn swellow_error_source_chain_works() {
        let io_err = io::Error::new(io::ErrorKind::Other, "permission denied");
        let kind = SwellowErrorKind::IoFileWrite {
            source: io_err,
            path: std::path::PathBuf::from("/tmp/out.sql"),
        };
        let src = kind.source().unwrap().to_string();
        assert!(src.contains("permission denied"));
    }

    #[test]
    fn swellow_error_from_conversions_work() {
        let engine_err = EngineError {
            kind: EngineErrorKind::LockConflict,
        };
        let parse_err = ParseError {
            kind: ParseErrorKind::InvalidVersionFormat("bad".into()),
        };

        let s1: SwellowError = engine_err.into();
        let s2: SwellowError = parse_err.into();

        assert!(matches!(s1.kind, SwellowErrorKind::Engine(_)));
        assert!(matches!(s2.kind, SwellowErrorKind::Parse(_)));
    }
}