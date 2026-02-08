use crate::parser::error::ParseErrorKind;
use crate::parser::ParseError;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};


/// Extract migration ID from version name: "001_create_users" -> 1
pub fn parse_id_from_version_name(version_name: &str) -> Result<i64, ParseError> {
    let prefix = match version_name.split('_').next() {
        Some(v) => v,
        None => {
            return Err(ParseError {
                kind: ParseErrorKind::InvalidVersionFormat(version_name.to_string()),
            })
        }
    };

    prefix.parse::<i64>().map_err(|_| ParseError {
        kind: ParseErrorKind::InvalidVersionNumber(version_name.to_string()),
    })
}

/// Find versions within [from_version_id, to_version_id], checking global uniqueness first,
/// then parsing only the filtered set. Returns results sorted by version_id.
pub fn collect_versions_from_directory(
    directory: &str,
    from_version_id: i64,
    to_version_id: i64,
    raise_if_empty: bool,
) -> Result<BTreeMap<i64, PathBuf>, ParseError> {
    let path = Path::new(directory);
    if !path.is_dir() {
        return Err(ParseError { kind: ParseErrorKind::InvalidDirectory(path.to_path_buf()) })
    }

    let mut versions = BTreeMap::new();

    // For each subdirectory, collect (version_id, full_path)
    for entry in fs::read_dir(path).map_err(|e| {
        ParseError { kind: ParseErrorKind::Io {
            // Fatal: reading the directory failed.
            path: path.to_path_buf(),
            source: Box::new(e)
        } }
    })? {
        let entry = entry.map_err(|e| {
            ParseError { kind: ParseErrorKind::Io {
                path: path.to_path_buf(),
                source: Box::new(e)
            }}
        })?;

        let dir_path = entry.path();
        if !dir_path.is_dir() {
            tracing::debug!("Skipping non-directory: {:?}", dir_path);
            continue;
        }

        let dir_name = dir_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| ParseError {
                // Fatal: invalid directory name.
                kind: ParseErrorKind::InvalidDirectory(dir_path.clone())
            })?
            .to_string();
        
        let version_id = parse_id_from_version_name(&dir_name)?;
        if version_id <= from_version_id || version_id > to_version_id {
            tracing::debug!(
                "Skipping version {} (out of range {}..{})",
                version_id, from_version_id, to_version_id
            );
            continue;
        }

        if versions.insert(version_id, dir_path).is_some() {
            return Err(ParseError {
                kind: ParseErrorKind::DuplicateVersionNumber(version_id)
            });
        }
    }

    if versions.is_empty() && raise_if_empty {
        return Err(ParseError { kind: ParseErrorKind::NoMigrationsInRange(path.to_path_buf(), from_version_id, to_version_id) })
    }
    
    Ok(versions)
}

