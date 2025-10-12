use sqlparser::{ast::Statement, tokenizer::Token};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;


#[derive(Debug)]
#[non_exhaustive]
pub struct ParseError {
    pub kind: ParseErrorKind
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ParseError: {}", self.kind)
    }
}

impl Error for ParseError {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		Some(&self.kind)
	}
}

#[derive(Debug)]
pub enum ParseErrorKind {
    FileNotFound(PathBuf),
    InvalidDirectory(PathBuf),
    InvalidVersionFormat(String),
    InvalidVersionNumber(String),
    Io { path: PathBuf, source: Box<dyn Error> },
    NoMigrationsInRange(PathBuf, i64, i64),
    Tokens(Vec<Token>),
    Statement(Statement),
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileNotFound(path) => write!(f, "File not found: '{path:?}'"),
            Self::InvalidDirectory(path) => write!(f, "Directory does not exist or is not a directory: '{path:?}'"),
            Self::InvalidVersionFormat(version) => write!(f, "Invalid version format: '{version}'"),
            Self::InvalidVersionNumber(version) => write!(f, "Invalid version number: '{version}'"),
            Self::Io { path, .. } => write!(f, "Failed to read file: '{path:?}'"),
            Self::NoMigrationsInRange(path, from, to) => write!(f, "No migrations found in '{path:?}' for range [{from}..={to}]"),
            Self::Tokens(tokens) => write!(f, "Failed to parse tokens into statement: {tokens:?}"),
            Self::Statement(stmt) => write!(f, "Failed to parse any resources from the statement: '{stmt}'"),
        }
    }
}

impl Error for ParseErrorKind {
	fn source(&self) -> Option<&(dyn Error + 'static)> {
		match self {
			Self::Io { source, .. } => Some(&**source),
			_ => None,
		}
	}
}