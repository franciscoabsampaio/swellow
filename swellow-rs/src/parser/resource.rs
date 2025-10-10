use crate::parser::statement::StatementCollection;

use sqlparser::ast::{
    ObjectType, Statement, AlterTableOperation, AlterIndexOperation, AlterRoleOperation,
};
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::vec;


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
        ResourceCollection(vec![])
    }

    pub fn from_statement_collection(collection: &StatementCollection) -> anyhow::Result<Self> {
        let mut resources = ResourceCollection::new();

        for statement in collection.parse_statements()? {
            resources.with_statement(statement)
        }

        Ok(resources)
    }

    pub fn pop_first_match(
        &mut self,
        object_type: ObjectType,
        name_before: &str,
    ) -> Option<Resource> {
        if let Some(pos) = self.iter().position(|r| {
            r.object_type == object_type && &r.name_after == name_before
        }) {
            Some(self.remove(pos))
        } else {
            None
        }
    }

    pub fn upsert<GenericIdentifier: Display>(
        &mut self,
        object_type: ObjectType,
        name_before: Option<GenericIdentifier>,
        name_after: Option<GenericIdentifier>,
        statement: &'static str,
        object_variant: Option<ObjectType>,
    ) {
        let object_type = object_variant.unwrap_or(object_type);
        let name_before = name_before.map(|n| n.to_string()).unwrap_or_else(|| "-1".to_string());
        let name_after = name_after.map(|n| n.to_string()).unwrap_or_else(|| "-1".to_string());

        let (name_before, mut statements_vec) = match statement {
            "CREATE" => ("-1".to_string(), Vec::new()),
            _ => self.pop_first_match(object_type, &name_before)
                    .map(|res| (res.name_before, res.statements))
                    .unwrap_or((name_before, Vec::new())),
        };

        statements_vec.push(statement.to_string());

        self.push(Resource {
            object_type,
            name_before,
            name_after,
            statements: statements_vec,
        });
    }

    /// Upserts resources from a statement into the collection
    pub fn with_statement(&mut self, stmt: Statement) {
        match stmt {
            // === CREATE Statements ===
            Statement::CreateTable(table) => { self.upsert(
                ObjectType::Table,
                None,
                Some(table.name),
                "CREATE",
                None
            )}
            Statement::CreateIndex(index) => {
                self.upsert(
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
                self.upsert(
                    ObjectType::View,
                    None,
                    Some(name),
                    "CREATE",
                    Some(object_variant)
                );
            }
            Statement::CreateSequence { name, .. }
            | Statement::CreateType { name, .. } => { self.upsert(
                ObjectType::Type,
                None,
                Some(name),
                "CREATE",
                None
            )}
            Statement::CreateSchema { schema_name, .. } => { self.upsert(
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
                    self.upsert(
                        ObjectType::Role,
                        None,
                        Some(name),
                        "CREATE",
                        Some(object_variant)
                    );
                }
            }
            Statement::CreateDatabase { db_name, .. } => { self.upsert(
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
                    self.upsert(
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
                    )
                };
                self.upsert(
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
                self.upsert(
                    ObjectType::Role,
                    Some(&name),
                    Some(new_name),
                    operation,
                    None
                );
            }
            Statement::AlterView { name, .. } => {
                self.upsert(
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
                    self.upsert(
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
}

impl Deref for ResourceCollection {
    type Target = Vec<Resource>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl DerefMut for ResourceCollection {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}