use crate::{
    commands::MigrationDirection,
    db::EngineBackend,
    parser::{self, ResourceCollection, StatementCollection},
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Collect (version_name, version_id) for all subdirs
pub fn collect_versions_from_directory(directory: &str) -> Result<Vec<(String, i64)>> {
    let path = Path::new(directory);
    if !path.is_dir() {
        anyhow::bail!("Target directory '{}' does not exist or is not a directory", directory);
    }

    let mut versions = fs::read_dir(path)
        .with_context(|| format!("Failed to read directory '{}'", directory))?
        .filter_map(|entry| {
            let dir_path = entry.ok()?.path();
            if !dir_path.is_dir() { return None; }

            let version_name = dir_path.file_name()?.to_str()?.to_string();
            let version_id = super::extract_version_id(&version_name).ok()?;
            Some((version_name, version_id))
        })
        .collect::<Vec<_>>();

    // Enforce global uniqueness
    let mut first_by_id = HashMap::new();
    for (name, id) in &versions {
        if let Some(first) = first_by_id.insert(*id, name.clone()) {
            anyhow::bail!(
                "Duplicate version_id {} found in directories '{}' and '{}'",
                id, first, name
            );
        }
    }

    // Sort by version_id
    versions.sort_by_key(|(_, id)| *id);
    Ok(versions)
}

/// Scan a migration version directory for a specific SQL file and return resources
fn gather_resources_from_migration_dir_with_id(
    version_path: PathBuf,
    version_id: i64,
    file_name: &str,
    backend: &EngineBackend,
) -> Result<(i64, PathBuf, StatementCollection, ResourceCollection)> {
    let target_file = version_path.join(file_name);

    if !target_file.exists() {
        return Ok((version_id, version_path, StatementCollection::from_backend(backend), ResourceCollection::new()));
    }

    let sql = fs::read_to_string(&target_file)
        .with_context(|| format!("Failed to read file {:?}", target_file))?;

    let statements = StatementCollection::from_backend(backend).parse_sql(&sql);
    let resources = ResourceCollection::from_statement_collection(&statements)?;

    Ok((version_id, version_path, statements, resources))
}

/// Load migrations within [from_version_id, to_version_id], checking uniqueness first
pub fn load_in_interval(
    base_dir: &str,
    from_version_id: i64,
    to_version_id: i64,
    direction: &MigrationDirection,
    backend: &EngineBackend,
) -> Result<Vec<(i64, PathBuf, StatementCollection, ResourceCollection)>> {
    if from_version_id > to_version_id {
        anyhow::bail!(
            "Invalid version interval: from_version_id ({}) > to_version_id ({})",
            from_version_id, to_version_id
        );
    }

    let mut versions = collect_versions_from_directory(base_dir)
        .with_context(|| format!("Failed to collect versions from directory '{}'", base_dir))?;

    if versions.is_empty() {
        anyhow::bail!("No subdirectories found in '{}'", base_dir);
    }

    versions.retain(|(_, id)| *id > from_version_id && *id <= to_version_id);

    if versions.is_empty() {
        anyhow::bail!(
            "No migrations found in interval [{}..={}]",
            from_version_id,
            to_version_id
        );
    }

    versions
        .into_iter()
        .map(|(version_name, version_id)| {
            let path = Path::new(base_dir).join(&version_name);
            gather_resources_from_migration_dir_with_id(path, version_id, direction.filename(), backend)
        })
        .collect()
}
