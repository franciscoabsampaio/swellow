use crate::parser::error::ParseErrorKind;
use crate::parser::{
    collect_versions_from_directory,
    ParseError,
    ReferenceToStaticDialect,
    ResourceCollection,
    StatementCollection
};

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;


#[derive(Clone, PartialEq)]
pub enum MigrationDirection {
    Up,
    Down,
}

impl MigrationDirection {
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Up => "Migrating",
            Self::Down => "Rolling back",
        }
    }
    pub fn noun(&self) -> &'static str {
        match self {
            Self::Up => "Migration",
            Self::Down => "Rollback",
        }
    }
    pub fn filename(&self) -> &'static str {
        match self {
            Self::Up => "up.sql",
            Self::Down => "down.sql",
        }
    }
}

pub struct Migration {
    pub path: PathBuf,
    #[allow(dead_code)]
    sql: String,
    pub statements: StatementCollection,
}

impl Migration {
    pub fn new(dialect: ReferenceToStaticDialect, path: PathBuf, sql: &str) -> Self {
        let sql = sql.to_string();
        let statements = StatementCollection::new(dialect).parse_sql(&sql);

        Migration { path, sql, statements }
    }

    pub fn from_file(dialect: ReferenceToStaticDialect, path: PathBuf) -> Result<Self, ParseError> {
        if !path.exists() {
            return Err(ParseError { kind: ParseErrorKind::FileNotFound(path) })
        }

        let sql = fs::read_to_string(&path)
            .map_err(|e| ParseError { kind: ParseErrorKind::Io { path: path.clone(), source: Box::new(e) } })?;

        Ok(Migration::new(dialect, path, &sql))
    }

    pub fn resources(&self) -> ResourceCollection {
        ResourceCollection::from_statement_collection(&self.statements)
    }
}

pub struct MigrationCollection {
    direction: MigrationDirection,
    inner: BTreeMap<i64, Migration>,
}

impl MigrationCollection {
    pub fn new(
        direction: &MigrationDirection,
        migrations: BTreeMap<i64, Migration>
    ) -> Self {
        MigrationCollection {
            direction: direction.clone(),
            inner: migrations
        }    
    }

    /// Load migrations within [from_version_id, to_version_id], checking global uniqueness first,
    /// then parsing only the filtered set. Returns results sorted by version_id.
    pub fn from_directory(
        dialect: ReferenceToStaticDialect,
        directory: &str,
        direction: &MigrationDirection,
        from_version_id: i64,
        to_version_id: i64,
    ) -> Result<Self, ParseError> {
        let versions = collect_versions_from_directory(
            directory, from_version_id, to_version_id, true
        )?;

        let migrations = versions
            .iter()
            .map(|(id, path)| {
                Ok((*id, Migration::from_file(dialect, path.join(direction.filename()))?))
            })
            .collect::<Result<BTreeMap<i64, Migration>, ParseError>>()?;

        Ok(MigrationCollection::new(direction, migrations))
    }

    pub fn iter(&self) -> Vec<(&i64, &Migration)> {
        if self.direction == MigrationDirection::Up {
            self.inner.iter().collect()
        } else {
            // Reverse execution direction if migration direction is down.
            self.inner.iter().rev().collect()
        }
    }
}