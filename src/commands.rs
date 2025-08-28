use crate::{db, migrations};
use sqlx::{Pool, PgPool, Postgres};


pub async fn peck(db_connection_string: &String) -> sqlx::Result<Pool<Postgres>> {
    println!("Pecking database...");
    let pool: Pool<Postgres> = PgPool::connect(&db_connection_string).await?;

    db::ensure_table(&pool).await?;

    return Ok(pool)
}


pub async fn plan(
    db_connection_string: &String,
    migration_directory: &String,
    max_version_id: i32
) -> sqlx::Result<Vec<db::Record>> {
    let pool: Pool<Postgres> = peck(db_connection_string).await?;
    let tx = pool.begin().await?;
    let records = db::begin(tx).await?;

    // Get the latest migration version.
    let from_version_id: i32 = match {
        records
            .iter()
            .map(|m| m.migration_version_id)
            .max()
    } {
        Some(v) => v,
        None => 0
    };
    
    // Get version names in migration_directory.
    let migrations = match migrations::load_in_interval(
        migration_directory,
        from_version_id,
        max_version_id
    ) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    for (version_id, resources) in migrations {
        println!("Migration {}: {:?}", version_id, resources);
    }

    return Ok(records)
}