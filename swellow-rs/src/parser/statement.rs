use crate::parser::error::ParseErrorKind;
use crate::parser::greedy_parse;
use crate::parser::dialect::*;
use crate::parser::ParseError;

use sqlparser::ast::Statement;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::fmt::{self, Write};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::{Deref, DerefMut};


#[derive(Debug, Clone)]
pub struct ActionableStatement {
    pub tokens: Vec<Token>,
    pub statement: Statement,
}

impl ActionableStatement {
    pub fn new(dialect: ReferenceToStaticDialect, tokens: Vec<Token>) -> Result<Self, ParseError> {
        if let Some(statement) = greedy_parse(dialect, tokens.clone()) {
            Ok(ActionableStatement { tokens, statement })
        } else {
            Err(ParseError { kind: ParseErrorKind::Tokens(tokens) })
        }
    }
}

impl fmt::Display for ActionableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut statement = String::new();
        
        // Print each token
        for token in self {
            write!(statement, "{}", token)?;
        }

        // Check if the statement ends with a semicolon
        let ends_with_semicolon = self
            .into_iter()
            .last()
            .is_some_and(|t| matches!(t, Token::SemiColon));

        // Write it out
        if ends_with_semicolon {
            write!(f, "{}", statement)?;
        } else {
            write!(f, "{};", statement)?;
        }
        Ok(())
    }
}

impl IntoIterator for ActionableStatement {
    type Item = Token;
    type IntoIter = std::vec::IntoIter<Token>;

    fn into_iter(self) -> Self::IntoIter {
        self.tokens.into_iter()
    }
}

impl<'a> IntoIterator for &'a ActionableStatement {
    type Item = &'a Token;
    type IntoIter = std::slice::Iter<'a, Token>;

    fn into_iter(self) -> Self::IntoIter {
        self.tokens.iter()
    }
}


#[derive(Debug, Clone)]
pub struct StatementCollection {
    inner: Vec<ActionableStatement>,
    pub dialect: ReferenceToStaticDialect,
}

impl StatementCollection {
    pub fn new(dialect: ReferenceToStaticDialect) -> Self {
        StatementCollection {
            inner: vec![],
            dialect,
        }
    }

    pub fn parse_sql(&mut self, sql: &str) -> Result<Self, ParseError> {
        let mut tokenizer = Tokenizer::new(&*self.dialect, sql);
        let tokens = tokenizer
            .tokenize()
            .map_err(|err| ParseError {
                kind: ParseErrorKind::Tokenizer(err)
            })?;

        let mut current_tokens = vec![];

        for token in tokens {
            current_tokens.push(token.clone());
            if token == Token::SemiColon {
                // Only push actionable statements
                if let Ok(stmt) = ActionableStatement::new(self.dialect, current_tokens) {
                    self.inner.push(stmt);
                }
                current_tokens = vec![]; // Reset for the next statement
            }
        }

        // Add the last statement if it doesn't end with a semicolon
        if !current_tokens.is_empty() {
            if let Ok(stmt) = ActionableStatement::new(self.dialect, current_tokens) {
                self.inner.push(stmt);
            }
        }

        Ok(self.clone())
    }
    
    pub fn to_strings(&self) -> Vec<String> {
        self.iter()
            .map(|stmt| stmt.into_iter().map(Token::to_string).collect())
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
    type Item = ActionableStatement;
    type IntoIter = std::vec::IntoIter<ActionableStatement>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a StatementCollection {
    type Item = &'a ActionableStatement;
    type IntoIter = std::slice::Iter<'a, ActionableStatement>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl Deref for StatementCollection {
    type Target = Vec<ActionableStatement>;
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
        let mut output = String::new();

        for statement in self {
            write!(output, "{}", statement)?;
        }

        write!(f, "{}", output)?;

        Ok(())
    }
}