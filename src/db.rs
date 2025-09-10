use crate::{postgres, MigrationDirection};

use sha2::{Sha256, Digest};
use sqlparser::ast::ObjectType;
use sqlx::{FromRow, Pool, Postgres, Transaction};
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};


#[derive(Debug, FromRow)]
pub struct Record {
    pub oid: i32,
    pub version_id: i64,
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
            oid OID,
            object_type TEXT NOT NULL,
            object_name_before TEXT NOT NULL,
            object_name_after TEXT,
            version_id BIGINT NOT NULL,
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
    sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE;")
        .execute(&mut **tx)
        .await?;

    tracing::info!("Getting latest migrations from records...");
    // Get the latest migrations for each object.
    let rows: Vec<Record> = sqlx::query_as::<_, Record>("
    SELECT
        last.oid::int AS oid,  -- Cast to int for compatibility with Rust
        last.version_id,
        b.object_type,
        COALESCE(b.object_name_after, '') object_name_after
    FROM (
        SELECT
            oid,
            MAX(version_id) version_id
        FROM swellow_records
        WHERE status IN ('APPLIED', 'TESTED')
        GROUP BY oid
    ) last
    INNER JOIN swellow_records b
        ON last.oid=b.oid
        AND last.version_id=b.version_id
    ")
        .fetch_all(&mut **tx)
        .await?;

    return Ok(rows)
}


fn file_checksum(path: &Path) -> Result<String, std::io::Error> {
    let file = fs::File::open(path)?;
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
        WHERE version_id>$1
        "#,
    )
        .bind(current_version_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}


pub async fn get_oid(
    tx: &mut Transaction<'static, Postgres>,
    object_type: &ObjectType,
    object_name_before: &String,
) -> sqlx::Result<i32> {
    let query = postgres::OID_QUERIES.get(&object_type.to_string()).expect(
        &format!("Unsupported object type: {}", &object_type)
    );

    sqlx::query_scalar(query)
        .bind(object_name_before)
        .fetch_one(&mut **tx).await
}


pub async fn insert_record(
    tx: &mut Transaction<'static, Postgres>,
    oid: Option<i32>,
    object_type: &ObjectType,
    object_name_before: &String,
    version_id: i64,
    file_path: &PathBuf
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO swellow_records(
            oid,
            object_type,
            object_name_before,
            version_id,
            status,
            checksum
        )
        VALUES (
            $1::oid,
            $2,
            $3,
            $4,
            $5,
            md5($6)
        )
        RETURNING oid, version_id, status
        "#,
    )
        .bind(oid)
        .bind(object_type.to_string())
        .bind(object_name_before)
        .bind(version_id)
        .bind("READY")
        .bind(file_checksum(&file_path)?)
        .execute(&mut **tx)
        .await?;

    Ok(())
}


pub async fn execute_sql_script(
    tx: &mut Transaction<'static, Postgres>,
    file_path: &PathBuf
) -> sqlx::Result<()> {
    let sql = fs::read_to_string(file_path)
        .expect(&format!("Failed to read SQL file: {:?}", file_path));
    
    // Execute migration
    sqlx::raw_sql(&sql)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

pub async fn update_record(
    tx: &mut Transaction<'static, Postgres>,
    direction: &MigrationDirection,
    version_id: i64,
    object_type: &ObjectType,
    object_name_before: &String,
    oid: Option<i32>
) -> sqlx::Result<()> {
    let oid = get_oid(
        tx,
        object_type,
        object_name_before
    ).await?;
    sqlx::query(
        r#"
        UPDATE swellow_records
        SET
            oid=$1::oid,
            --object_name_after=$2,
            status=$2
        WHERE
            object_type=$3
            AND object_name_before=$4
            AND version_id=$5
        "#,
    )
        .bind(oid)
        // .bind()
        .bind(match direction {
            MigrationDirection::Up => "APPLIED",
            MigrationDirection::Down => "ROLLED_BACK"
        })
        .bind(object_type.to_string())
        .bind(object_name_before)
        .bind(version_id)
        .execute(&mut **tx)
        .await?;
    
    Ok(())
}