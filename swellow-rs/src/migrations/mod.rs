pub mod directory;

use anyhow::Context;

/// Extract version ID from version name: "001_create_users" -> 1
pub fn extract_version_id(version_name: &str) -> anyhow::Result<i64> {
    version_name
        .split('_')
        .next()
        .context("Invalid version format")?
        .parse::<i64>()
        .context("Version ID is not a number")
}