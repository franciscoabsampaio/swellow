use std::path::PathBuf;
use std::fmt::Write;

use crate::migrations::Resource;
use crate::MigrationDirection;

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
    migrations: &Vec<(i64, PathBuf, Vec<Resource>)>,
    direction: &MigrationDirection
) -> () {
    let operation = match direction {
        MigrationDirection::Up => "Migration",
        MigrationDirection::Down => "Rollback"
    };

    let mut output = "Generating migration plan...\n--- Migration plan ---\n".to_string();

    for (version_id, version_path, resources) in migrations {
        // writeln! appends to the String
        writeln!(
            &mut output,
            "\n{} {}: '{}' -> {} change(s)",
            operation,
            version_id,
            version_path.display(),
            resources.len(),
        ).unwrap();

        let mut destructive_found = false;

        for Resource { name, object_type, statement } in resources {
            writeln!(
                &mut output,
                "-> {} {} {}",
                statement,
                object_type,
                name,
            ).unwrap();

            // Check for destructive statements
            if statement.trim_start().to_uppercase().starts_with("DROP") {
                destructive_found = true;
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