use anyhow::Result;
use sqlx::{FromRow, Pool, Postgres, Transaction};

#[derive(Debug, FromRow)]
pub struct Record {
    pub oid: i32,  // OID in Postgres is int4
    pub version_id: i32,
    pub version: String,
    pub object_name_after: String,
}

pub async fn ensure_table(pool: &Pool<Postgres>) -> Result<()> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS swellow_records (
            id SERIAL PRIMARY KEY,
            oid OID NOT NULL,
            object_name_before TEXT NOT NULL,
            object_name_after TEXT,
            migration_version TEXT NOT NULL,
            migration_version_id INTEGER NOT NULL,
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

pub async fn begin(mut tx: Transaction<'static, Postgres>) -> Result<Vec<Record>> {
    // Acquire a lock on the swellow_records table
    // To ensure no other migration process is underway.
    sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    let rows: Vec<Record> = sqlx::query_as::<_, Record>("
    SELECT
        last.oid,
        last.migration_version_id,
        b.migration_version,
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