use crate::parser::{Resource, ResourceCollection};
use crate::commands::MigrationDirection;
use std::path::PathBuf;
use std::fmt::Write;


pub fn setup_logging(verbose: u8, quiet: bool) {
    let level = if quiet {
        tracing::Level::ERROR
    } else { match verbose {
        0 => tracing::Level::INFO,
        1 => tracing::Level::DEBUG,
        _ => tracing::Level::TRACE,
    }};

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default subscriber failed!");
}


pub fn show_migration_changes(
    migrations: &Vec<(i64, PathBuf, ResourceCollection)>,
    direction: &MigrationDirection
) -> () {
    let operation = direction.noun();
    let mut output = "Generating migration plan...\n--- Migration plan ---".to_string();

    for (version_id, version_path, resources) in migrations {
        // writeln! appends to the String
        writeln!(
            &mut output,
            "\n---\n{} {}: '{}' -> {} change(s)",
            operation,
            version_id,
            version_path.display(),
            resources.len(),
        ).unwrap();

        let mut destructive_found = false;

        for Resource { object_type, name_before, name_after, statements } in resources.iter() {
            let object_name = if name_before != "-1" { name_before } else {
                if name_after != "-1" { name_after } else {
                    "NULL"
                }
            };
            
            writeln!(
                &mut output,
                "-> {} {}:",
                // name_after,
                object_type,
                object_name,
            ).unwrap();

            for stmt in statements {
                writeln!(
                    &mut output,
                    "\t-> {}",
                    stmt,
                ).unwrap();
                
                // Check for destructive statements
                if stmt == "DROP" {
                    destructive_found = true;
                }
            }

        }

        if destructive_found {
            tracing::warn!("{} {} contains destructive actions!", operation, version_id);
            writeln!(
                &mut output,
                "\n\tWARNING: {} {} contains destructive actions!",
                operation,
                version_id
            ).unwrap();
        }
    }

    tracing::info!("{}\n--- End of migration plan ---", output);
}