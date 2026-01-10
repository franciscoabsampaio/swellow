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

    // For each subdirectory, collect (version_name, version_id)
    let versions = fs::read_dir(path)
        .map_err(|e| {
            ParseError { kind: ParseErrorKind::Io { path: path.to_path_buf(), source: Box::new(e) } }
        })?
        .filter_map(|entry| {
            let entry = match entry {
                Ok(dir) => dir,
                Err(e) => {
                    // Fatal: reading the directory failed.
                    return Some(Err(
                        ParseError { kind: ParseErrorKind::Io { path: path.to_path_buf(), source: Box::new(e) } }
                    ));
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
                    return Some(Err(
                        ParseError { kind: ParseErrorKind::InvalidDirectory(dir_path) }
                    ));
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
        .collect::<Result<BTreeMap<i64, PathBuf>, ParseError>>()?;

    if versions.is_empty() {
        return Err(ParseError { kind: ParseErrorKind::NoMigrationsInRange(path.to_path_buf(), from_version_id, to_version_id) })
    }
    
    Ok(versions)
}

