use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub enum ResourceType {
    Table,
    Index,
    Sequence,
    Other,
}

#[derive(Debug)]
pub struct Resource {
    pub name: String,
    pub object_type: String,
    pub statement: String
}

/// Extract version ID from folder name: "001_create_users" -> 1
fn extract_version_id(version_name: &str) -> Result<i64, String> {
    version_name
        .split('_')
        .next()
        .ok_or_else(|| format!("Invalid version format: '{}'", version_name))?
        .parse::<i64>()
        .map_err(|_| format!("Version ID is not a number: '{}'", version_name))
}

/// Convert ObjectName to a string like "public.users"
fn object_name_to_string(name: &ObjectName) -> String {
    name.0.iter().map(|part| part.to_string()).collect::<Vec<_>>().join(".")
}

/// Parse SQL string from a specific file and return resources it modifies.
fn parse_sql_file(file_path: &Path) -> Result<Vec<Resource>, String> {
    let sql = fs::read_to_string(file_path)
        .map_err(|e| format!("Failed to read file {:?}: {}", file_path, e))?;

    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| format!("Failed to parse SQL in file {:?}: {}", file_path, e))?;

    let mut resources = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::CreateTable(table) => resources.push(Resource {
                name: object_name_to_string(&table.name),
                object_type: "TABLE".to_string(),
                statement: "CREATE".to_string()
            }),
            Statement::AlterTable { name, .. } => resources.push(Resource {
                name: object_name_to_string(&name),
                object_type: "TABLE".to_string(),
                statement: "ALTER".to_string()
            }),
            Statement::Drop { object_type, names, .. } => {
                for name in names {
                    let rtype = match object_type {
                        sqlparser::ast::ObjectType::Table => "TABLE",
                        sqlparser::ast::ObjectType::Index => "INDEX",
                        sqlparser::ast::ObjectType::Sequence => "SEQUENCE",
                        _ => "OTHER",
                    };
                    resources.push(Resource {
                        name: object_name_to_string(&name),
                        object_type: rtype.to_string(),
                        statement: "DROP".to_string()
                    });
                }
            }
            Statement::CreateIndex(index) => resources.push(Resource {
                name: object_name_to_string(&index.table_name),
                object_type: "INDEX".to_string(),
                statement: "CREATE".to_string()
            }),
            _ => {}
        }
    }

    Ok(resources)
}


/// Scan a migration version directory for SQL files and return all resources
fn gather_resources_from_migration_dir_with_id(
    version_path: PathBuf,
    version_id: i64,
) -> Result<(i64, PathBuf, Vec<Resource>), String> {
    let mut all_resources = Vec::new();

    for entry in fs::read_dir(&version_path)
        .map_err(|e| format!("Failed to read directory {:?}: {}", version_path, e))?
    {
        let path = entry.map_err(|e| format!("Failed to read entry: {}", e))?.path();

        if path.is_file() && path.extension().map_or(false, |ext| ext == "sql") {
            all_resources.extend(parse_sql_file(&path)?);
        }
    }

    if all_resources.is_empty() {
        return Err(format!("No SQL files found in migration directory: {:?}", version_path));
    }

    Ok((version_id, version_path, all_resources))
}

/// Load migrations within [from_version_id, to_version_id], checking global uniqueness first,
/// then parsing only the filtered set. Returns results sorted by version_id.
pub fn load_in_interval(
    base_dir: &str,
    from_version_id: i64,
    to_version_id: i64,
) -> Result<Vec<(i64, PathBuf, Vec<Resource>)>, String> {
    if from_version_id > to_version_id {
        return Err(format!(
            "Invalid version interval: from_version_id ({}) > to_version_id ({})",
            from_version_id, to_version_id
        ));
    }

    let path = Path::new(base_dir);
    if !path.is_dir() {
        return Err(format!(
            "Target directory '{}' does not exist or is not a directory",
            base_dir
        ));
    }

    // 1) Collect (version_name, version_id) for all subdirs
    let mut versions: Vec<(String, i64)> = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|e| format!("Failed to read directory '{}': {}", base_dir, e))?
    {
        let dir_path = entry.map_err(|e| format!("Failed to read entry: {}", e))?.path();
        if !dir_path.is_dir() {
            continue;
        }
        let version_name = match dir_path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let version_id = extract_version_id(&version_name)
            .map_err(|e| format!("In '{}': {}", version_name, e))?;
        versions.push((version_name, version_id));
    }

    if versions.is_empty() {
        return Err(format!("No subdirectories found in '{}'", base_dir));
    }

    // 2) Enforce global uniqueness across ALL subdirs (not just filtered)
    let mut first_by_id: HashMap<i64, String> = HashMap::new();
    for (name, id) in &versions {
        if let Some(first) = first_by_id.insert(*id, name.clone()) {
            return Err(format!(
                "Duplicate version_id {} found in directories '{}' and '{}'",
                id, first, name
            ));
        }
    }

    // 3) Filter to the requested interval (inclusive)
    versions.retain(|(_, id)| *id > from_version_id && *id <= to_version_id);

    if versions.is_empty() {
        return Err(format!(
            "No migrations found in interval [{}..={}].",
            from_version_id, to_version_id
        ));
    }

    // 4) Sort by version_id
    versions.sort_by_key(|(_, id)| *id);

    // 5) Parse only the filtered set
    let mut migrations: Vec<(i64, PathBuf, Vec<Resource>)> = Vec::new();
    for (version_name, version_id) in versions {
        let tuple = gather_resources_from_migration_dir_with_id(
            Path::new(base_dir).join(version_name),
            version_id
        )?;
        migrations.push(tuple);
    }

    Ok(migrations)
}
