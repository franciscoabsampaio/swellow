use anyhow::Result;
use crate::db;
use sqlx::{Pool, PgPool, Postgres};

pub async fn peck(db_connection_string: &String) -> Result<Pool<Postgres>> {
    println!("Pecking database...");
    let pool: Pool<Postgres> = PgPool::connect(&db_connection_string).await?;

    db::ensure_table(&pool).await?;

    return Ok(pool)
}

pub async fn plan(db_connection_string: &String) -> Result<Vec<db::Record>> {
    let pool: Pool<Postgres> = peck(db_connection_string).await?;
    let tx = pool.begin().await?;
    let records = db::begin(tx).await?;

    return Ok(records)
}