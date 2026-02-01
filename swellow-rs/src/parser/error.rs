use sqlparser::{ast::Statement, tokenizer::{Token, TokenizerError}};
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
    DuplicateVersionNumber(i64),
    InvalidDirectory(PathBuf),
    InvalidVersionFormat(String),
    InvalidVersionNumber(String),
    Io { path: PathBuf, source: Box<dyn Error> },
    NoMigrationsInRange(PathBuf, i64, i64),
    Tokenizer(TokenizerError),
    Tokens(Vec<Token>),
    Statement(Statement),
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileNotFound(path) => write!(f, "File not found: '{path:?}'"),
            Self::DuplicateVersionNumber(version) => write!(f, "More than one migration found with version: '{version}'"),
            Self::InvalidDirectory(path) => write!(f, "Directory does not exist or is not a directory: '{path:?}'"),
            Self::InvalidVersionFormat(version) => write!(f, "Invalid version format: '{version}'"),
            Self::InvalidVersionNumber(version) => write!(f, "Invalid version number: '{version}'"),
            Self::Io { path, .. } => write!(f, "Failed to read file: '{path:?}'"),
            Self::NoMigrationsInRange(path, from, to) => write!(f, "No migrations found in '{path:?}' for range [{from}..={to}]"),
            Self::Tokenizer( err ) => write!(f, "Failed to initialize tokenizer: {err:?}"),
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

#[cfg(test)]
mod tests {
    use sqlparser::ast::ObjectName;

    use super::*;
    use std::io;

    #[test]
    fn display_formats_correctly() {
        let path = PathBuf::from("/tmp/test.sql");

        let cases: Vec<(ParseErrorKind, &str)> = vec![
            (ParseErrorKind::FileNotFound(path.clone()), "File not found:"),
            (ParseErrorKind::InvalidDirectory(path.clone()), "Directory does not exist"),
            (ParseErrorKind::InvalidVersionFormat("vX.Y".into()), "Invalid version format"),
            (ParseErrorKind::InvalidVersionNumber("abc".into()), "Invalid version number"),
            (
                ParseErrorKind::Io {
                    path: path.clone(),
                    source: Box::new(io::Error::new(io::ErrorKind::Other, "fail")),
                },
                "Failed to read file",
            ),
            (
                ParseErrorKind::NoMigrationsInRange(path.clone(), 1, 10),
                "No migrations found",
            ),
            (
                ParseErrorKind::Tokens(vec![
                    Token::Word(sqlparser::tokenizer::Word {
                        value: "SELECT".into(),
                        quote_style: None,
                        keyword: sqlparser::keywords::Keyword::SELECT,
                    })
                ]),
                "Failed to parse tokens",
            ),
            (
                ParseErrorKind::Statement(sqlparser::ast::Statement::Msck {
                    table_name: ObjectName::from(vec![]),
                    repair: false,
                    partition_action: None
                }),
                "Failed to parse any resources",
            ),
        ];

        for (err, expected_substr) in cases {
            let formatted = err.to_string();
            assert!(
                formatted.contains(expected_substr),
                "Expected `{}` in `{}`",
                expected_substr,
                formatted
            );
        }
    }

    #[test]
    fn io_source_is_accessible() {
        let io_err = io::Error::new(io::ErrorKind::Other, "disk full");
        let kind = ParseErrorKind::Io {
            path: PathBuf::from("/tmp/foo.sql"),
            source: Box::new(io_err),
        };
        let src = kind.source().unwrap().to_string();
        assert!(src.contains("disk full"));
    }

    #[test]
    fn parse_error_wraps_kind() {
        let inner = ParseErrorKind::InvalidVersionNumber("abc".into());
        let err = ParseError { kind: inner };
        assert!(err.to_string().contains("Invalid version number"));
        // Ensure the source() method returns Some
        assert!(err.source().is_some());
    }
}
