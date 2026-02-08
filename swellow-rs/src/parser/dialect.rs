use crate::db::EngineBackend;

use sqlparser::dialect::{Dialect, DatabricksDialect, HiveDialect, PostgreSqlDialect};

pub static DIALECT_DATABRICKS: DatabricksDialect = DatabricksDialect;
pub static DIALECT_HIVE: HiveDialect = HiveDialect {};
pub static DIALECT_POSTGRES: PostgreSqlDialect = PostgreSqlDialect {};

pub type ReferenceToStaticDialect = &'static dyn Dialect;

impl From<&EngineBackend> for ReferenceToStaticDialect {
    fn from(backend: &EngineBackend) -> Self {
        match backend {
            EngineBackend::Postgres(_) => &DIALECT_POSTGRES,
            EngineBackend::SparkDelta(_) => &DIALECT_DATABRICKS,
            EngineBackend::SparkIceberg(_) => &DIALECT_HIVE,
        }
    }
}

impl From<&mut EngineBackend> for ReferenceToStaticDialect {
    fn from(backend: &mut EngineBackend) -> Self {
        match backend {
            EngineBackend::Postgres(_) => &DIALECT_POSTGRES,
            EngineBackend::SparkDelta(_) => &DIALECT_DATABRICKS,
            EngineBackend::SparkIceberg(_) => &DIALECT_HIVE,
        }
    }
}