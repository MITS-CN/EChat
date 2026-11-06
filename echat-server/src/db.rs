use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;

pub async fn init_db(path: &str) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}", path))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(opts)
        .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            name          TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            created_at    TEXT NOT NULL
        );
    "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS servers (
            owner      TEXT NOT NULL,
            name       TEXT NOT NULL,
            ip         TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (owner, name)
        );
    "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bans (
            kind       TEXT NOT NULL,
            target     TEXT NOT NULL,
            reason     TEXT,
            banned_at  TEXT NOT NULL,
            PRIMARY KEY (kind, target)
        );
    "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ops (
            name       TEXT PRIMARY KEY,
            granted_at TEXT NOT NULL
        );
    "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}