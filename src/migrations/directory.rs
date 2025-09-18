use crate::{commands::MigrationDirection, parser::{self, ResourceCollection}};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};


/// Collect (version_name, version_id) for all subdirs
pub fn collect_versions_from_directory(directory: &str) -> Result<Vec<(String, i64)>, String> {
    // Validate directory
    let path = Path::new(directory);
    if !path.is_dir() {
        return Err(format!(
            "Target directory '{}' does not exist or is not a directory",
            directory
        ));
    }

    // For each subdirectory, collect (version_name, version_id)
    let mut versions = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|e| format!("Failed to read directory '{}': {}", directory, e))?
    {
        let dir_path = entry.map_err(|e| format!("Failed to read entry: {}", e))?.path();
        if !dir_path.is_dir() {
            continue;
        }
        let version_name = match dir_path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let version_id = parser::extract_version_id(&version_name)
            .map_err(|e| format!("In '{}': {}", version_name, e))?;
        versions.push((version_name, version_id));
    }

    // Enforce global uniqueness across ALL subdirs (not just filtered)
    let mut first_by_id: HashMap<i64, String> = HashMap::new();
    for (name, id) in &versions {
        if let Some(first) = first_by_id.insert(*id, name.clone()) {
            return Err(format!(
                "Duplicate version_id {} found in directories '{}' and '{}'",
                id, first, name
            ));
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
    file_name: &str, // e.g. "up.sql" or "down.sql"
) -> Result<(i64, PathBuf, ResourceCollection), String> {
    let target_file = version_path.join(file_name);

    if !target_file.exists() {
        return Ok((version_id, version_path, ResourceCollection::new()))
    }

    let sql = fs::read_to_string(&target_file)
        .map_err(|e| format!("Failed to read file {:?}: {}", target_file, e))?;
    let resources = parser::parse_sql(&sql)?;

    Ok((version_id, version_path, resources))
}


/// Load migrations within [from_version_id, to_version_id], checking global uniqueness first,
/// then parsing only the filtered set. Returns results sorted by version_id.
pub fn load_in_interval(
    base_dir: &str,
    from_version_id: i64,
    to_version_id: i64,
    direction: &MigrationDirection, // e.g. "up.sql" or "down.sql"
) -> Result<Vec<(i64, PathBuf, ResourceCollection)>, String> {
    if from_version_id > to_version_id {
        return Err(format!(
            "Invalid version interval: from_version_id ({}) > to_version_id ({})",
            from_version_id, to_version_id
        ));
    }

    // 1) Collect versions from directory
    let mut versions: Vec<(String, i64)> = collect_versions_from_directory(base_dir)?;
    if versions.is_empty() {
        return Err(format!("No subdirectories found in '{}'", base_dir));
    }

    // 2) Filter to the requested interval
    versions.retain(|(_, id)| *id > from_version_id && *id <= to_version_id);
    if versions.is_empty() {
        return Err(format!(
            "No migrations found in interval [{}..={}].",
            from_version_id, to_version_id
        ));
    }

    // 3) Parse only the filtered set
    let mut migrations: Vec<(i64, PathBuf, ResourceCollection)> = Vec::new();
    for (version_name, version_id) in versions {
        let tuple = gather_resources_from_migration_dir_with_id(
            Path::new(base_dir).join(version_name),
            version_id,
            direction.filename(),
        )?;
        migrations.push(tuple);
    }

    Ok(migrations)
}
