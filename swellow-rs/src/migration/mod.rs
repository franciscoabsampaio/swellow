mod collection;
mod direction;

pub use collection::MigrationCollection;
pub use direction::MigrationDirection;

use crate::sqlparser::{ReferenceToStaticDialect, ResourceCollection, StatementCollection};

use anyhow::Context;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};


/// Extract migration ID from version name: "001_create_users" -> 1
pub fn parse_id_from_version_name(version_name: &str) -> anyhow::Result<i64> {
    version_name
        .split('_')
        .next()
        .context("Invalid version format")?
        .parse::<i64>()
        .context("Version ID is not a number")
}


/// Find versions within [from_version_id, to_version_id], checking global uniqueness first,
/// then parsing only the filtered set. Returns results sorted by version_id.
pub fn collect_versions_from_directory(
    directory: &str,
    from_version_id: i64,
    to_version_id: i64
) -> anyhow::Result<BTreeMap<i64, PathBuf>> {
    let path = Path::new(directory);
    if !path.is_dir() {
        anyhow::bail!("Target directory '{}' does not exist or is not a directory", directory);
    }

    // For each subdirectory, collect (version_name, version_id)
    let versions = fs::read_dir(path)
        .map_err(|e| anyhow::format_err!("Failed to read from '{path:?}': {e}"))?
        .filter_map(|entry| {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    // Fatal: reading the directory failed.
                    return Some(Err(anyhow::anyhow!("Failed to read entry in {:?}: {}", path, e)));
                }
            };

            let dir_path = entry.path();
            if !dir_path.is_dir() {
                tracing::debug!("Skipping non-directory: {:?}", dir_path);
                return None;
            }

            let dir_name = match dir_path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s.to_string(),
                None => {
                    // Fatal: invalid directory name.
                    return Some(Err(anyhow::anyhow!("Invalid directory name in {:?}", dir_path)));
                }
            };

            let version_id = match parse_id_from_version_name(&dir_name) {
                Ok(id) => id,
                Err(e) => {
                    tracing::debug!("Skipping {:?}: failed to parse version ID: {}", dir_name, e);
                    return None;
                }
            };

            if version_id <= from_version_id || version_id > to_version_id {
                tracing::debug!(
                    "Skipping version {} (out of range {}..{})",
                    version_id, from_version_id, to_version_id
                );
                return None;
            }

            Some(Ok((version_id, path.join(&dir_name))))
        })
        .collect::<anyhow::Result<BTreeMap<i64, PathBuf>>>()?;

    if versions.is_empty() {
        anyhow::bail!(
            "No migrations found in '{}' for interval [{}..={}]",
            directory,
            from_version_id,
            to_version_id
        );
    }

    Ok(versions)
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

    pub fn from_file(dialect: ReferenceToStaticDialect, path: PathBuf) -> anyhow::Result<Self> {
        if !path.exists() {
            anyhow::bail!("No file '{path:?}' found!");
        }

        let sql = fs::read_to_string(&path)
            .map_err(|e| anyhow::format_err!("Failed to read file '{:?}': {}", path, e))?;

        Ok(Migration::new(dialect, path, &sql))
    }

    pub fn resources(&self) -> ResourceCollection {
        ResourceCollection::from_statement_collection(&self.statements)
    }
}