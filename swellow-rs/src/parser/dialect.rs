use sqlparser::dialect::{DatabricksDialect, HiveDialect, PostgreSqlDialect};

pub static DIALECT_DATABRICKS: DatabricksDialect = DatabricksDialect;
pub static DIALECT_HIVE: HiveDialect = HiveDialect {};
pub static DIALECT_POSTGRES: PostgreSqlDialect = PostgreSqlDialect {};