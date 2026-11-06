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

    // ===== 一期二期已有 =====
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            name          TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            created_at    TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;

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
    ).execute(&pool).await?;

    let cols: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM pragma_table_info('servers')")
            .fetch_all(&pool).await?;
    if !cols.iter().any(|(n,)| n == "password") {
        sqlx::query("ALTER TABLE servers ADD COLUMN password TEXT")
            .execute(&pool).await?;
    }

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bans (
            kind TEXT NOT NULL, target TEXT NOT NULL,
            reason TEXT, banned_at TEXT NOT NULL,
            PRIMARY KEY (kind, target)
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ops (
            name TEXT PRIMARY KEY, granted_at TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;

    // ===== 三期 A 新增 =====

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS user_prefs (
            name       TEXT PRIMARY KEY,
            take_on    INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS friends (
            user   TEXT NOT NULL,
            friend TEXT NOT NULL,
            since  TEXT NOT NULL,
            PRIMARY KEY (user, friend)
        );
    "#,
    ).execute(&pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_friends_user ON friends(user)")
        .execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS friend_requests (
            from_user  TEXT NOT NULL,
            to_user    TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (from_user, to_user)
        );
    "#,
    ).execute(&pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_freq_to ON friend_requests(to_user)")
        .execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS friend_reminders (
            watcher TEXT NOT NULL,
            target  TEXT NOT NULL,
            PRIMARY KEY (watcher, target)
        );
    "#,
    ).execute(&pool).await?;

    // ===== 三期 B：群聊新增 =====
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            uuid        TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            owner       TEXT NOT NULL,
            intro       TEXT DEFAULT '',
            join_mode   TEXT NOT NULL DEFAULT 'A',
            searchable  INTEGER NOT NULL DEFAULT 1,
            question    TEXT,
            answer_hash TEXT,
            created_at  TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_groups_name ON groups(name)")
        .execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_members (
            group_uuid TEXT NOT NULL,
            user       TEXT NOT NULL,
            role       TEXT NOT NULL DEFAULT 'member',
            joined_at  TEXT NOT NULL,
            PRIMARY KEY (group_uuid, user)
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_invites (
            group_uuid TEXT NOT NULL,
            user       TEXT NOT NULL,
            from_user  TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (group_uuid, user)
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_join_requests (
            group_uuid TEXT NOT NULL,
            user       TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (group_uuid, user)
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_join_attempts (
            group_uuid TEXT NOT NULL,
            user       TEXT NOT NULL,
            answer     TEXT NOT NULL,
            success    INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            kind       TEXT NOT NULL,
            target     TEXT NOT NULL,
            from_user  TEXT NOT NULL,
            content    TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
    "#,
    ).execute(&pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_target ON messages(kind, target, id DESC)")
        .execute(&pool).await?;

    Ok(pool)
}

pub async fn get_setting(pool: &SqlitePool, key: &str) -> Option<String> {
    sqlx::query_as::<_, (String,)>("SELECT value FROM settings WHERE key = ?")
        .bind(key).fetch_optional(pool).await.ok().flatten().map(|(v,)| v)
}

pub async fn set_setting(pool: &SqlitePool, key: &str, value: &str) -> anyhow::Result<()> {
    sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)")
        .bind(key).bind(value).execute(pool).await?;
    Ok(())
}

pub async fn del_setting(pool: &SqlitePool, key: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM settings WHERE key = ?")
        .bind(key).execute(pool).await?;
    Ok(())
}



/// 启动时扫描用户名大小写冲突。返回冲突组。
pub async fn scan_case_conflicts(pool: &SqlitePool) -> Vec<Vec<String>> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM users ORDER BY LOWER(name), name")
            .fetch_all(pool).await.unwrap_or_default();
    let mut groups: Vec<Vec<String>> = Vec::new();
    let mut cur: Vec<String> = Vec::new();
    let mut last_lower = String::new();
    for (name,) in rows {
        let lower = name.to_lowercase();
        if !cur.is_empty() && lower == last_lower {
            cur.push(name);
        } else {
            if cur.len() > 1 {
                groups.push(cur.clone());
            }
            cur = vec![name];
            last_lower = lower;
        }
    }
    if cur.len() > 1 {
        groups.push(cur);
    }
    groups
}