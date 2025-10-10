use crate::db::EngineBackend;
use crate::parser::greedy_parse;
use crate::parser::dialect::*;

use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::{Deref, DerefMut};


#[derive(Debug, Clone)]
pub struct StatementCollection {
    inner: Vec<Vec<Token>>,
    dialect: &'static dyn Dialect,
}

impl StatementCollection {
    pub fn new(dialect: &'static dyn Dialect) -> Self {
        StatementCollection {
            inner: vec![],
            dialect,
        }
    }

    pub fn from_backend(backend: &EngineBackend) -> Self {
        StatementCollection {
            inner: vec![],
            dialect: match backend {
                EngineBackend::Postgres(_) => &DIALECT_POSTGRES,
                EngineBackend::SparkDelta(_) => &DIALECT_DATABRICKS,
                EngineBackend::SparkIceberg(_) => &DIALECT_HIVE,
            },
        }
    }

    pub fn parse_sql(&mut self, sql: &str) -> Self {
        let mut tokenizer = Tokenizer::new(&*self.dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut current_statement = vec![];

        for token in tokens {
            current_statement.push(token.clone());
            if token == Token::SemiColon {
                self.inner.push(current_statement);
                current_statement = vec![]; // Reset for the next statement
            }
        }

        // Add the last statement if it doesn't end with a semicolon
        if !current_statement.is_empty() {
            self.inner.push(current_statement);
        }

        self.clone()
    }

    pub fn parse_statements(&self) -> anyhow::Result<Vec<Statement>> {
        self.iter()
            .map(|tokens| greedy_parse(&*self.dialect, tokens.to_vec()))
            .collect::<anyhow::Result<Vec<Statement>>>()
    }
    
    pub fn to_strings(&self) -> Vec<String> {
        self.iter()
            .map(|tokens| tokens.iter().map(Token::to_string).collect())
            .collect()
    }

    pub fn checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        for tokens in self.iter() {
            for token in tokens {
                token.hash(&mut hasher);
            }
        }
        hasher.finish()
    }
}

impl IntoIterator for StatementCollection {
    type Item = Vec<Token>;
    type IntoIter = std::vec::IntoIter<Vec<Token>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a StatementCollection {
    type Item = &'a Vec<Token>;
    type IntoIter = std::slice::Iter<'a, Vec<Token>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl Deref for StatementCollection {
    type Target = Vec<Vec<Token>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for StatementCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl fmt::Display for StatementCollection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for tokens in self {
            // Build each statement
            let mut stmt = String::new();
            for token in tokens {
                use std::fmt::Write;
                write!(stmt, "{}", token)?;
            }

            // Check if it ends with a semicolon
            let ends_with_semicolon = tokens
                .last()
                .is_some_and(|t| matches!(t, Token::SemiColon));

            // Write it out
            if ends_with_semicolon {
                write!(f, "{}", stmt)?;
            } else {
                write!(f, "{};", stmt)?;
            }
        }

        Ok(())
    }
}