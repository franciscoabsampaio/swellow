use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use sqlparser::ast::{
    ObjectType,
    Statement,
    AlterTableOperation,
    AlterIndexOperation,
    AlterRoleOperation,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;


#[derive(Debug, Clone)]
pub struct Resource {
    pub object_type: ObjectType,
    pub name_before: String,
    pub name_after: String,
    pub statements: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResourceCollection(Vec<Resource>);


impl ResourceCollection {
    pub fn new() -> Self {
        ResourceCollection(Vec::new())
    }

    pub fn pop_first_match(
        &mut self,
        object_type: ObjectType,
        name_before: &String,
    ) -> Option<Resource> {
        if let Some(pos) = self.iter().position(|r| {
            r.object_type == object_type && &r.name_after == name_before
        }) {
            Some(self.remove(pos))
        } else {
            None
        }
    }
}

impl Deref for ResourceCollection {
    type Target = Vec<Resource>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResourceCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


/// Extract version ID from version name: "001_create_users" -> 1
pub fn extract_version_id(version_name: &str) -> Result<i64, String> {
    version_name
        .split('_')
        .next()
        .ok_or_else(|| format!("Invalid version format: '{}'", version_name))?
        .parse::<i64>()
        .map_err(|_| format!("Version ID is not a number: '{}'", version_name))
}


/// Parse SQL string and return resources it modifies, with list of operations applied to each resource.
pub fn parse_sql(sql: &String) -> Result<ResourceCollection, String> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| format!("Failed to parse SQL: {}", e))?;

    let mut resources = ResourceCollection::new();

    fn upsert_resource<GenericIdentifier: Display>(
        resources: &mut ResourceCollection,
        object_type: ObjectType,
        name_before: Option<GenericIdentifier>,
        name_after: Option<GenericIdentifier>,
        statement: &'static str,
        object_variant: Option<ObjectType>
    ) {
        let object_type = match object_variant {
            Some(variant) => variant,
            _ => object_type
        };
        let name_before = match name_before {
            Some(name) => name.to_string(),
            _ => "-1".to_string()
        };
        let name_after = match name_after {
            Some(name) => name.to_string(),
            _ => "-1".to_string()
        };

        let (name_before, mut statements) = match statement {
            "CREATE" => ("-1".to_string(), Vec::new()),
            _ => { match resources.pop_first_match(
                object_type, &name_before
            ) {
                Some(res) => (res.name_before, res.statements),
                None => (name_before, Vec::new())
            }}
        };

        statements.push(statement.to_string());
        resources.push(Resource {
            object_type: object_type,
            name_before: name_before,
            name_after: name_after,
            statements: statements,
        });
    }

    for stmt in statements {
        match stmt {
            // === CREATE Statements ===
            Statement::CreateTable(table) => { upsert_resource(
                &mut resources,
                ObjectType::Table,
                None,
                Some(table.name),
                "CREATE",
                None
            )}
            Statement::CreateIndex(index) => {
                upsert_resource(
                    &mut resources,
                    ObjectType::Index,
                    None,
                    index.name,
                    "CREATE",
                    None
                )
            }
            Statement::CreateView { name, materialized, .. } => {
                let object_variant = if materialized {
                    ObjectType::MaterializedView
                } else {
                    ObjectType::View
                };
                upsert_resource(
                    &mut resources,
                    ObjectType::View,
                    None,
                    Some(name),
                    "CREATE",
                    Some(object_variant)
                );
            }
            Statement::CreateSequence { name, .. }
            | Statement::CreateType { name, .. } => { upsert_resource(
                &mut resources,
                ObjectType::Type,
                None,
                Some(name),
                "CREATE",
                None
            )}
            Statement::CreateSchema { schema_name, .. } => { upsert_resource(
                &mut resources,
                ObjectType::Schema,
                None,
                Some(schema_name),
                "CREATE",
                None
            )}
            Statement::CreateRole { names, login, .. } => {
                for name in names {
                    let object_variant = match login {
                        Some(_) => ObjectType::User,
                        None => ObjectType::Role,
                    };
                    upsert_resource(
                        &mut resources,
                        ObjectType::Role,
                        None,
                        Some(name),
                        "CREATE",
                        Some(object_variant)
                    );
                }
            }
            Statement::CreateDatabase { db_name, .. } => { upsert_resource(
                &mut resources,
                ObjectType::Database,
                None,
                Some(db_name),
                "CREATE",
                None
            )}

            // === ALTER Statements ===
            Statement::AlterTable { name, operations, .. } => {
                for operation in operations {
                    let (new_name, operation) = match &operation {
                        AlterTableOperation::RenameTable { table_name } => (
                            table_name, "RENAME"
                        ),
                        _ => (&name, "ALTER")
                    };
                    upsert_resource(
                        &mut resources,
                        ObjectType::Table,
                        Some(&name),
                        Some(new_name),
                        operation,
                        None
                    );
                }
            }
            Statement::AlterIndex { name, operation, .. } => {
                let (new_name, operation) = match &operation {
                    AlterIndexOperation::RenameIndex { index_name } => (
                        index_name, "RENAME"
                    ),
                    _ => (&name, "ALTER")
                };
                upsert_resource(
                    &mut resources,
                    ObjectType::Index,
                    Some(&name),
                    Some(new_name),
                    operation,
                    None
                );
            }
            Statement::AlterRole { name, operation, .. } => {
                let (new_name, operation) = match &operation {
                    AlterRoleOperation::RenameRole { role_name } => (
                        role_name, "RENAME"
                    ),
                    _ => (&name, "ALTER")
                };
                upsert_resource(
                    &mut resources,
                    ObjectType::Role,
                    Some(&name),
                    Some(new_name),
                    operation,
                    None
                );
            }
            Statement::AlterView { name, .. } => {
                upsert_resource(
                    &mut resources,
                    ObjectType::Index,
                    Some(&name),
                    Some(&name),
                    "ALTER",
                    None
                );
            }

            // === DROP Statements ===
            Statement::Drop { object_type, names, .. } => {
                 for name in names {
                    upsert_resource(
                        &mut resources,
                        object_type,
                        Some(name),
                        None,
                        "DROP",
                        None
                    );
                }
            }
            _ => {}
        }
    }

    // The final list of resources is the values of our map.
    Ok(resources)
}