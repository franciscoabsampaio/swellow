use crate::migration::{MigrationCollection, MigrationDirection};
use crate::parser::Resource;
use std::fmt::Write;


pub fn setup_logging(verbose: u8, quiet: bool, json: bool) {
    if json {
        // Mute all logging if JSON output is enabled
        // TODO: Log to a file instead or always
        tracing::subscriber::set_global_default(tracing::subscriber::NoSubscriber::default())
            .expect("Setting no-op subscriber failed");
        return;
    }

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
    migrations: &MigrationCollection,
    direction: &MigrationDirection
) -> () {
    let operation = direction.noun();
    let mut output = "Generating migration plan...\n--- Migration plan ---".to_string();

    for (version_id, migration) in migrations.iter() {
        let resources = migration.resources();

        // writeln! appends to the String
        writeln!(
            &mut output,
            "\n---\n{} {}: '{}' -> {:?} change(s)",
            operation,
            version_id,
            migration.path.display(),
            resources,
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