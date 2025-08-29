use std::path::PathBuf;
use std::fmt::Write;

use crate::migrations::Resource;

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
    migrations: Vec<(i64, PathBuf, Vec<Resource>)>
) -> () {
    let mut output = "\nThe following migrations will be applied:\n".to_string();

    for (version_id, version_path, resources) in migrations {
        // writeln! appends to the String
        writeln!(
            &mut output,
            "\nMigration {}: '{}' -> {} change(s)",
            version_id,
            version_path.display(),
            resources.len(),
        ).unwrap();

        for Resource { name, object_type, statement } in resources {
            writeln!(
                &mut output,
                "-> {} {} {}",
                statement,
                object_type,
                name,
            ).unwrap();
        }
    }

    tracing::info!("{}", output);
}