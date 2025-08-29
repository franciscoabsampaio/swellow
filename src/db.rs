use sqlx::{FromRow, Pool, Postgres, Transaction};

#[derive(Debug, FromRow)]
pub struct Record {
    pub oid: i32,
    pub migration_version_id: i64,
    pub migration_version_name: String,
    pub object_name_after: String,
}

pub async fn ensure_table(pool: &Pool<Postgres>) -> sqlx::Result<()> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS swellow_records (
            id SERIAL PRIMARY KEY,
            oid OID NOT NULL,
            object_name_before TEXT NOT NULL,
            object_name_after TEXT,
            migration_version_id BIGINT NOT NULL,
            migration_version_name TEXT NOT NULL,
            status TEXT NOT NULL,
            checksum INTEGER NOT NULL,
            dtm_created_at TIMESTAMP DEFAULT now(),
            dtm_updated_at TIMESTAMP DEFAULT now()
        )
    "#)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn begin(
    mut tx: Transaction<'static, Postgres>
) -> sqlx::Result<Vec<Record>> {
    tracing::info!("Acquiring lock on records table...");
    // Acquire a lock on the swellow_records table
    // To ensure no other migration process is underway.
    sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    tracing::info!("Getting latest migrations from records...");
    // Get the latest migrations for each object.
    let rows: Vec<Record> = sqlx::query_as::<_, Record>("
    SELECT
        last.oid::int AS oid,  -- Cast to int for compatibility with Rust
        last.migration_version_id,
        b.migration_version_name,
        b.object_name_after
    FROM (
        SELECT
            oid,
            MAX(migration_version_id) migration_version_id
        FROM swellow_records
        WHERE status IN ('EXECUTED', 'TESTED')
        GROUP BY oid
    ) last
    INNER JOIN swellow_records b
        ON last.oid=b.oid
        AND last.migration_version_id=b.migration_version_id
    ")
        .fetch_all(&mut *tx)
        .await?;

    return Ok(rows)
}
