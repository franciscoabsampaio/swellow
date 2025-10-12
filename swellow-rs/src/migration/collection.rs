use crate::migration::{collect_versions_from_directory, Migration, MigrationDirection};
use crate::sqlparser::ReferenceToStaticDialect;

use std::collections::BTreeMap;


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
    ) -> anyhow::Result<Self> {
        let versions = collect_versions_from_directory(
            directory, from_version_id, to_version_id
        )?;

        let migrations = versions
            .iter()
            .map(|(id, path)| {
                Ok((*id, Migration::from_file(dialect, path.join(direction.filename()))?))
            })
            .collect::<anyhow::Result<BTreeMap<i64, Migration>>>()?;

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