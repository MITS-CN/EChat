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
            password   TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (owner, name)
        );
    "#,
    )
    .execute(&pool)
    .await?;

    let cols: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM pragma_table_info('servers')")
            .fetch_all(&pool)
            .await?;
    if !cols.iter().any(|(n,)| n == "password") {
        sqlx::query("ALTER TABLE servers ADD COLUMN password TEXT")
            .execute(&pool)
            .await?;
    }

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

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

pub async fn get_setting(pool: &SqlitePool, key: &str) -> Option<String> {
    sqlx::query_as::<_, (String,)>("SELECT value FROM settings WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|(v,)| v)
}

pub async fn set_setting(pool: &SqlitePool, key: &str, value: &str) -> anyhow::Result<()> {
    sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)")
        .bind(key)
        .bind(value)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn del_setting(pool: &SqlitePool, key: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM settings WHERE key = ?")
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

// 兼容别名
#[allow(dead_code)]
pub async fn delete_setting(pool: &SqlitePool, key: &str) -> anyhow::Result<()> {
    del_setting(pool, key).await
}