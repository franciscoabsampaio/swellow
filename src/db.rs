use std::path::PathBuf;
use std::thread::current;

use sqlx::{FromRow, Pool, Postgres, Transaction};

#[derive(Debug, FromRow)]
pub struct Record {
    pub oid: i32,
    pub migration_version_id: i64,
    pub migration_version_name: String,
    pub object_type: String,
    pub object_name_after: String,
}

pub async fn ensure_table(
    pool: &Pool<Postgres>
) -> sqlx::Result<()> {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        .execute(pool)
        .await?;

    sqlx::query(r#"        
        CREATE TABLE IF NOT EXISTS swellow_records (
            id SERIAL PRIMARY KEY,
            oid OID NOT NULL,
            object_type TEXT NOT NULL,
            object_name_before TEXT NOT NULL,
            object_name_after TEXT,
            migration_version_id BIGINT NOT NULL,
            migration_version_name TEXT NOT NULL,
            status TEXT NOT NULL,
            checksum TEXT NOT NULL,
            dtm_created_at TIMESTAMP DEFAULT now(),
            dtm_updated_at TIMESTAMP DEFAULT now()
        );
    "#)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn begin(
    tx: &mut Transaction<'static, Postgres>
) -> sqlx::Result<Vec<Record>> {
    tracing::info!("Acquiring lock on records table...");
    // Acquire a lock on the swellow_records table
    // To ensure no other migration process is underway.
    sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await?;

    tracing::info!("Getting latest migrations from records...");
    // Get the latest migrations for each object.
    let rows: Vec<Record> = sqlx::query_as::<_, Record>("
    SELECT
        last.oid::int AS oid,  -- Cast to int for compatibility with Rust
        last.migration_version_id,
        b.migration_version_name,
        b.object_type,
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
        .fetch_all(&mut **tx)
        .await?;

    return Ok((rows))
}

use sha2::{Sha256, Digest};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

fn file_checksum(path: &Path) -> Result<String, std::io::Error> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 4096];

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    // Convert result to hex string
    Ok(format!("{:x}", hasher.finalize()))
}

pub async fn disable_records(
    tx: &mut Transaction<'static, Postgres>,
    current_version_id: i64
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        UPDATE swellow_records
        SET status='DISABLED'
        WHERE migration_version_id>$1
        "#,
    )
        .bind(current_version_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}


pub async fn insert_record(
    tx: &mut Transaction<'static, Postgres>,
    oid: Option<i32>,
    object_type: &String,
    object_name_before: &String,
    version_id: i64,
    version_name: &String,
    version_path: PathBuf
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO swellow_records(
            oid,
            object_type,
            object_name_before,
            migration_version_id,
            migration_version_name,
            status,
            checksum
        )
        VALUES (
            $1::oid,
            $2,
            $3,
            $4,
            $5,
            $6,
            md5($7)
        )
        RETURNING oid, migration_version_id, status
        "#,
    )
        .bind(oid)
        .bind(object_type)
        .bind(object_name_before)
        .bind(version_id)
        .bind(version_name)
        .bind("READY")
        .bind(file_checksum(&version_path)?)
        .execute(&mut **tx)
        .await?;

    Ok(())
}