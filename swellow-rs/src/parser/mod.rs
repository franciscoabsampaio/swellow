mod dialect;
mod resource;
mod statement;

pub use resource::{Resource, ResourceCollection};
pub use statement::StatementCollection;

use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;


/// Parse as many tokens as possible into a statement.
/// This is a bandaid to any limitation in the parser's lexicon.
/// If a statement is valid, but not yet in the parser's lexicon, it would fail.
/// This shouldn't be the case.
/// After all, parsing is only required up to the subject of the statement,
/// e.g. "CREATE TABLE table_name" - in order to track the resource being changed.
/// Everything after is irrelevant.
pub fn greedy_parse(
    dialect: &'static dyn Dialect,
    tokens: Vec<Token>
) -> anyhow::Result<Statement> {
    let mut last_ok = None;

    for i in 1..=tokens.len() {
        let partial = &tokens[..i];

        match Parser::new(dialect)
            .with_tokens(partial.to_vec())
            .parse_statements() {
                Ok(stmt) => last_ok = stmt.first().cloned(),
                Err(e) => tracing::debug!(
                    "SQL parsing failed at token {}: {:?} ({})",
                    i,
                    tokens.get(i - 1), // Use .get() to avoid panic on empty tokens
                    e
                )
            };
    }

    if let Some(stmt) = last_ok {
        Ok(stmt)
    } else {
        anyhow::bail!("Failed to parse any part of the SQL input: {tokens:?}")
    }
}


/// This test suite has a number of aims:
/// 1) Test that the collections are successfully initialized.
/// 2) Test that most SQL statements will be, to a variable extent, parsed successfully.
/// 3) Test that some SQL statements are outright impossible to parse.
/// 4) Test that regardless of how a statement is parsed, the original query can be restored from a vector of tokens.
/// 5) Test that statements that are successfully parsed can be parsed into a meaningful resource collection.
#[cfg(test)]
mod tests {
    use crate::parser::{dialect::*, ResourceCollection, StatementCollection};
    use sqlparser::dialect::Dialect;

    fn make_collection(dialect: &'static dyn Dialect, sql: &str) -> StatementCollection {
        StatementCollection::new(dialect)
            .parse_sql(sql)
    }

    #[test]
    fn test_parsed_some_part() {
        let valid_cases: &[(&'static dyn Dialect, &str, &str)] = &[
            // All dialects
            (&DIALECT_POSTGRES, "SELECT a,, b FROM t;", ""),
            (&DIALECT_HIVE, "SELECT a,, b FROM t;", ""),
            (&DIALECT_DATABRICKS, "SELECT a,, b FROM t;", ""),
            (&DIALECT_POSTGRES, "SELECT a, b, FROM t;", ""),
            (&DIALECT_HIVE, "SELECT a, b, FROM t;", ""),
            (&DIALECT_DATABRICKS, "SELECT a, b, FROM t;", ""),
            (&DIALECT_POSTGRES, "SELECT * FROM my_table WHERE;", ""),
            (&DIALECT_HIVE, "SELECT * FROM my_table WHERE;", ""),
            (&DIALECT_DATABRICKS, "SELECT * FROM my_table WHERE;", ""),
            (&DIALECT_POSTGRES, "SELECT * FROM my_table WHERE id =;", ""),
            (&DIALECT_HIVE, "SELECT * FROM my_table WHERE id =;", ""),
            (&DIALECT_DATABRICKS, "SELECT * FROM my_table WHERE id =;", ""),
            (&DIALECT_POSTGRES, "SELECT my_col;", ""),
            (&DIALECT_HIVE, "SELECT my_col;", ""),
            (&DIALECT_DATABRICKS, "SELECT my_col;", ""),
            (&DIALECT_POSTGRES, "SELECT * FROM a LEFT JOIN b;", ""),
            (&DIALECT_HIVE, "SELECT * FROM a LEFT JOIN b;", ""),
            (&DIALECT_DATABRICKS, "SELECT * FROM a LEFT JOIN b;", ""),
            (&DIALECT_POSTGRES, "SELECT * FROM a JOIN b USING ();", ""),
            (&DIALECT_HIVE, "SELECT * FROM a JOIN b USING ();", ""),
            (&DIALECT_DATABRICKS, "SELECT * FROM a JOIN b USING ();", ""),
            
            (&DIALECT_POSTGRES, "SELECT * FROM t ORDER BY;", ""),
            (&DIALECT_HIVE, "SELECT * FROM t ORDER BY;", ""),
            (&DIALECT_DATABRICKS, "SELECT * FROM t ORDER BY;", ""),
            (&DIALECT_POSTGRES, "SELECT * FROM t GROUP BY;", ""),
            (&DIALECT_HIVE, "SELECT * FROM t GROUP BY;", ""),
            (&DIALECT_DATABRICKS, "SELECT * FROM t GROUP BY;", ""),
            
            // Postgres only
            (&DIALECT_POSTGRES, "SELECT DISTINCT FROM t;", ""),
            (&DIALECT_POSTGRES, "SELECT TOP 10 FROM t;", ""),
            (&DIALECT_POSTGRES, "SELECT ARRAY[1,2,3];", ""),
            (&DIALECT_POSTGRES, "SELECT * FROM my_table LIMIT;", ""),
            (&DIALECT_POSTGRES, "INSERT INTO my_table (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;", ""),
            
            // Databricks / Delta only
            (&DIALECT_DATABRICKS, "CREATE TABLE delta_table (id INT, value STRING) USING delta TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true);", "CREATE TABLE"),
            (&DIALECT_DATABRICKS, "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value);", ""),
            (&DIALECT_DATABRICKS, "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN DELETE WHEN MATCHED AND s.value > 10 THEN UPDATE SET t.value = s.value;", ""),
            (&DIALECT_DATABRICKS, "CREATE TABLE t3 (id INT) USING delta PARTITIONED BY ();", "CREATE TABLE"),

            // Hive / Iceberg only
            (&DIALECT_HIVE, "CREATE TABLE iceberg_table (id INT, value STRING) USING iceberg PARTITIONED BY (days(ts));", "CREATE TABLE"),
            (&DIALECT_HIVE, "ALTER TABLE iceberg_table DROP COLUMN extra;", "ALTER TABLE"),
        ];
        
        for (dialect, sql, operation) in valid_cases {
            let collection = make_collection(*dialect, sql);
            let result = collection.parse_statements();
            assert!(result.is_ok(), "Failed parsing for dialect {:?}: {:?}", dialect, sql);

            assert!(sql == &collection.to_string());

            let resources = ResourceCollection::from_statement_collection(&collection)
                .expect("Failed to parse into a resource collection!");

            for resource in resources.iter() {
                // We take the first statement, because these queries are single-statement.
                assert!(operation == &format!("{} {}", resource.statements.first().unwrap(), resource.object_type))
            }
        }
    }

    #[test]
    fn test_failed_to_parse_any_part() {
        let invalid_cases: &[(&'static dyn Dialect, &str)] = &[
            // Postgres only
            (&DIALECT_POSTGRES, "INSERT INTO my_table (id, name) VALUES ();"),
            (&DIALECT_POSTGRES, "UPDATE my_table SET name =;"),

            // Databricks only
            (&DIALECT_DATABRICKS, "SELECT TOP 10 FROM t;"),
            (&DIALECT_DATABRICKS, "ALTER TABLE delta_table ADD COLUMNS (extra STRING);"),
            (&DIALECT_DATABRICKS, "SELECT DISTINCT FROM t;"),

            // Hive only
            (&DIALECT_HIVE, "SELECT TOP 10 FROM t;"),
            (&DIALECT_HIVE, "SELECT DISTINCT FROM t;"),
        ];
        
        for (dialect, sql) in invalid_cases {
            let collection = make_collection(*dialect, sql);
            let result = collection.parse_statements();
            assert!(result.is_err(), "Parsing should have failed for dialect {:?}: {:?}", dialect, sql);

            for s in collection.to_strings() {
                assert!(&s == sql)
            }
        }
    }

    #[test]
    fn test_multi_statement_queries() {
        let cases: &[(&'static dyn Dialect, &str)] = &[(
            &DIALECT_POSTGRES,
            "CREATE TABLE my_table (id INT, name TEXT); INSERT INTO my_table VALUES (1, 'Alice'); SELECT * FROM my_table;",
        ), (
            &DIALECT_DATABRICKS,
            "CREATE TABLE delta_table (id INT, name STRING) USING delta; INSERT INTO delta_table VALUES (1, 'Bob'); MERGE INTO delta_table AS t USING updates AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name;",
        ), (
            &DIALECT_HIVE,
            "CREATE TABLE iceberg_table (id INT, value STRING) USING iceberg; ALTER TABLE iceberg_table ADD COLUMN new_col STRING; SELECT * FROM iceberg_table;",
        )];

        for (dialect, sql) in cases {
            let collection = make_collection(*dialect, sql);
            let result = collection.parse_statements();
            assert!(
                result.is_ok(),
                "Failed to parse multi-statement query for dialect {:?}: {:?}",
                dialect,
                sql
            );

            let statements = collection.to_strings();
            assert!(
                statements.len() > 1,
                "Expected multiple statements, got {} for dialect {:?}",
                statements.len(),
                dialect
            );

            // Make sure each statement round-trips cleanly
            for s in &statements {
                assert!(!s.trim().is_empty(), "Parsed empty statement for {:?}", dialect);
            }
            // Assert .to_string() maintains multi-statement integrity
            assert!(sql == &collection.to_string());

            // Ensure we can collect resources for each statement
            let resources = ResourceCollection::from_statement_collection(&collection)
                .expect("Failed to collect resources from multi-statement query");
            assert!(
                !resources.is_empty(),
                "No resources parsed from multi-statement query for {:?}",
                dialect
            );
        }
    }

}