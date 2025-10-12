use crate::{db::EngineError, parser::ParseError};

use std::error::Error;
use std::fmt;
use std::path::PathBuf;


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
    Engine(EngineError),
    InvalidVersionInterval(i64, i64),
    IoDirectoryCreate { source: std::io::Error, path: PathBuf},
    IoFileWrite { source: std::io::Error, path: PathBuf},
    Parse(ParseError),
}

impl fmt::Display for SwellowErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => write!(f, "{}", error.kind),
            Self::InvalidVersionInterval(from, to) => write!(f, "Invalid version interval: from ({from}) > to ({to})"),
            Self::IoDirectoryCreate { path, .. } => write!(f, "Failed to create directory: '{path:?}'"),
            Self::IoFileWrite { path, .. } => write!(f, "Failed to write to file: '{path:?}'"),
            Self::Parse(error) => write!(f, "{}", error.kind)
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

impl From<ParseError> for SwellowError {
    fn from(error: ParseError) -> Self {
        SwellowError { kind: SwellowErrorKind::Parse(error) }
    }
}
