use crate::parser::error::ParseErrorKind;
use crate::parser::ParseError;
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

impl Resource {
    pub fn new(
        object_type: ObjectType,
        name_before: impl Display,
        name_after: impl Display,
        statements: Vec<String>
    ) -> Self {
        Resource {
            object_type,
            name_before: name_before.to_string(),
            name_after: name_after.to_string(),
            statements
        }
    }
}


#[derive(Debug, Clone)]
pub struct ResourceCollection(Vec<Resource>);

impl ResourceCollection {
    pub fn new() -> Self {
        ResourceCollection(vec![])
    }

    /// Parses a collection from a statement
    pub fn from_statement(stmt: Statement) -> Result<Self, ParseError> {
        Ok(ResourceCollection(match stmt {
            // === CREATE Statements ===
            Statement::CreateTable(table) => vec![Resource::new(
                ObjectType::Table,
                "-1",
                table.name,
                vec!["CREATE".to_string()],
            )],
            Statement::CreateIndex(index) => vec![Resource::new(
                ObjectType::Index,
                "-1",
                index.name.map(|n| n.to_string()).unwrap_or_else(|| "-1".to_string()),
                vec!["CREATE".to_string()],
            )],
            Statement::CreateView { name, materialized, .. } => {
                let object_variant = if materialized {
                    ObjectType::MaterializedView
                } else {
                    ObjectType::View
                };
                vec![Resource::new(
                    object_variant,
                    "-1",
                    name,
                    vec!["CREATE".to_string()],
                )]
            },
            Statement::CreateSequence { name, .. }
            | Statement::CreateType { name, .. } => vec![Resource::new(
                ObjectType::Type,
                "-1",
                name,
                vec!["CREATE".to_string()],
            )],
            Statement::CreateSchema { schema_name, .. } => vec![Resource::new(
                ObjectType::Schema,
                "-1",
                schema_name,
                vec!["CREATE".to_string()]
            )],
            Statement::CreateRole { names, login, .. } => {
                names.iter()
                    .map(|name| {
                        let object_variant = match login {
                            Some(_) => ObjectType::User,
                            None => ObjectType::Role,
                        };
                        Resource::new(
                            object_variant,
                            "-1",
                            name,
                            vec!["CREATE".to_string()],
                        )
                    })
                    .collect()
            },
            Statement::CreateDatabase { db_name, .. } => vec![Resource::new(
                ObjectType::Database,
                "-1",
                db_name,
                vec!["CREATE".to_string()]
            )],

            // === ALTER Statements ===
            Statement::AlterTable { name, operations, .. } => {
                operations.iter()
                    .map(|operation| {
                        let (new_name, operation) = match &operation {
                            AlterTableOperation::RenameTable { table_name } => (
                                table_name, "RENAME"
                            ),
                            _ => (&name, "ALTER")
                        };
                        Resource::new(
                            ObjectType::Table,
                            &name,
                            new_name,
                            vec![operation.to_string()],
                        )
                    })
                    .collect()
            },
            Statement::AlterIndex { name, operation, .. } => {
                let (new_name, operation) = match &operation {
                    AlterIndexOperation::RenameIndex { index_name } => (
                        index_name, "RENAME"
                    )
                };
                vec![Resource::new(
                    ObjectType::Index,
                    &name,
                    new_name,
                    vec![operation.to_string()],
                )]
            },
            Statement::AlterRole { name, operation, .. } => {
                let (new_name, operation) = match &operation {
                    AlterRoleOperation::RenameRole { role_name } => (
                        role_name, "RENAME"
                    ),
                    _ => (&name, "ALTER")
                };
                vec![Resource::new(
                    ObjectType::Role,
                    &name,
                    new_name,
                    vec![operation.to_string()],
                )]
            },
            Statement::AlterView { name, .. } => {
                vec![Resource::new(
                    ObjectType::Index,
                    &name,
                    &name,
                    vec!["ALTER".to_string()],
                )]
            },

            // === DROP Statements ===
            Statement::Drop { object_type, names, .. } => {
                names.iter()
                    .map(|name| {
                        Resource::new(
                            object_type,
                            name,
                            "-1",
                            vec!["DROP".to_string()],
                        )
                    })
                    .collect()
            },
            _ => return Err(ParseError { kind: ParseErrorKind::Statement(stmt) })
        }))
    }

    pub fn from_statement_collection(collection: &StatementCollection) -> Self {
        let mut resources = ResourceCollection::new();

        for stmt in collection {
            let resources_in_statement = match ResourceCollection::from_statement(stmt.statement.clone()) {
                Ok(res) => res,
                Err(_) => ResourceCollection::new()
            };
            
            for resource in resources_in_statement.iter() {
                resources.upsert(resource.clone());
            }
        }

        resources
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

    pub fn upsert(
        &mut self,
        resource: Resource
    ) {
        let statement = resource.statements.first().unwrap();

        let (name_before, mut statements_vec) = if statement == "CREATE" {
            ("-1".to_string(), Vec::new())
        } else {
            self.pop_first_match(resource.object_type, &resource.name_before)
                .map(|res| (res.name_before, res.statements))
                .unwrap_or((resource.name_before, Vec::new()))
        };

        statements_vec.extend(resource.statements);

        self.push(Resource::new(
            resource.object_type,
            name_before,
            resource.name_after,
            statements_vec,
        ));
    }
}

impl Deref for ResourceCollection {
    type Target = Vec<Resource>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl DerefMut for ResourceCollection {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}