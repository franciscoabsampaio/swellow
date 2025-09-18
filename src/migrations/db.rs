use crate::commands::MigrationDirection;

use sha2::{Sha256, Digest};
use sqlparser::ast::ObjectType;
use sqlx::{Pool, Postgres, Transaction};
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};


pub async fn ensure_table(
    pool: &Pool<Postgres>
) -> sqlx::Result<()> {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        .execute(pool)
        .await?;

    sqlx::query(r#"        
        CREATE TABLE IF NOT EXISTS swellow_records (
            oid OID,
            version_id BIGINT NOT NULL,
            object_type TEXT NOT NULL,
            object_name_before TEXT NOT NULL,
            object_name_after TEXT NOT NULL,
            status TEXT NOT NULL,
            checksum TEXT NOT NULL,
            dtm_created_at TIMESTAMP DEFAULT now(),
            dtm_updated_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (version_id, object_type, object_name_before, object_name_after)
        );
    "#)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn begin(
    tx: &mut Transaction<'static, Postgres>
) -> sqlx::Result<Option<i64>> {
    tracing::info!("Acquiring lock on records table...");
    // Acquire a lock on the swellow_records table
    // To ensure no other migration process is underway.
    sqlx::query("LOCK TABLE swellow_records IN ACCESS EXCLUSIVE MODE;")
        .execute(&mut **tx)
        .await?;

    tracing::info!("Getting latest migration version from records...");
    let version: Option<i64> = sqlx::query_scalar("
    SELECT
        MAX(version_id) version_id
    FROM swellow_records
    WHERE status IN ('APPLIED', 'TESTED')
    ")
        .fetch_one(&mut **tx)
        .await?;

    return Ok(version)
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


pub async fn upsert_record(
    tx: &mut Transaction<'static, Postgres>,
    object_type: &ObjectType,
    object_name_before: &String,
    object_name_after: &String,
    version_id: i64,
    file_path: &PathBuf
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO swellow_records(
            object_type,
            object_name_before,
            object_name_after,
            version_id,
            status,
            checksum
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            md5($6)
        )
        ON CONFLICT (version_id, object_type, object_name_before, object_name_after)
        DO UPDATE SET
            status = EXCLUDED.status,
            checksum = EXCLUDED.checksum
        "#,
    )
        .bind(object_type.to_string())
        .bind(object_name_before)
        .bind(object_name_after)
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
    let sql = match fs::read_to_string(file_path) {
        Ok(sql) => sql,
        Err(e) => {
            tracing::error!("Error processing {:?}: {}", file_path, e);
            std::process::exit(1);
        }
    };
    
    // Execute migration
    sqlx::raw_sql(&sql)
        .execute(&mut **tx)
        .await?;

    Ok(())
}


pub async fn update_record(
    tx: &mut Transaction<'static, Postgres>,
    direction: &MigrationDirection,
    version_id: i64
) -> sqlx::Result<()> {
    let status = match direction {
        MigrationDirection::Up => "APPLIED",
        MigrationDirection::Down => "ROLLED_BACK"
    };

    sqlx::query(
        r#"
        UPDATE swellow_records
        SET
            status=$1
        WHERE
            version_id=$2
        "#,
    )
        .bind(status)
        .bind(version_id)
        .execute(&mut **tx)
        .await?;
    
    Ok(())
}