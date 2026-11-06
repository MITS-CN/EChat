use std::sync::Arc;

use argon2::{Argon2, PasswordHash, PasswordVerifier};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{get_session, require_user};
use crate::protocol::Outgoing;
use crate::state::AppState;

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat OP <Login|banlist|list|kick|ban|ban-ip|pardon|pardon-ip> ...",
        ));
        return;
    }
    match args[0].to_lowercase().as_str() {
        "login" => op_login(state, session_id, &args[1..], tx).await,
        "banlist" => op_banlist(state, session_id, tx).await,
        "list" => op_list(state, session_id, tx).await,
        "kick" => op_kick(state, session_id, &args[1..], tx).await,
        "ban" => op_ban(state, session_id, &args[1..], tx).await,
        "ban-ip" => op_ban_ip(state, session_id, &args[1..], tx).await,
        "pardon" => op_pardon(state, session_id, &args[1..], tx).await,
        "pardon-ip" => op_pardon_ip(state, session_id, &args[1..], tx).await,
        other => {
            let _ = tx.send(Outgoing::err(format!("未知 OP 子命令: {}", other)));
        }
    }
}

async fn op_login(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 3 {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat OP Login [名称] [密码] [OP密码]",
        ));
        return;
    }
    let (name, password, op_password) = (args[0], args[1], args[2]);

    // 1) 校验 OP 密码
    let stored_op_hash = state.op_password_hash.read().await.clone();
    let op_parsed = match PasswordHash::new(&stored_op_hash) {
        Ok(p) => p,
        Err(_) => {
            let _ = tx.send(Outgoing::err(
                "服务端 OP 密码数据损坏，请用控制台 /Echat password-op 重置",
            ));
            return;
        }
    };
    if Argon2::default()
        .verify_password(op_password.as_bytes(), &op_parsed)
        .is_err()
    {
        let _ = tx.send(Outgoing::err("OP 密码错误"));
        return;
    }

    // 2) 校验用户密码
    let row: Option<(String,)> =
        sqlx::query_as("SELECT password_hash FROM users WHERE name = ?")
            .bind(name)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();
    let hash = match row {
        Some((h,)) => h,
        None => {
            let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", name)));
            return;
        }
    };

    let parsed = match PasswordHash::new(&hash) {
        Ok(p) => p,
        Err(_) => {
            let _ = tx.send(Outgoing::err("存储的密码数据损坏"));
            return;
        }
    };
    if Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_err()
    {
        let _ = tx.send(Outgoing::err("密码错误"));
        return;
    }

    // 3) 授予 OP
    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let mut guard = session.lock().await;
    guard.is_op = true;
    guard.user = Some(name.to_string());
    drop(guard);

    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query("INSERT OR IGNORE INTO ops (name, granted_at) VALUES (?, ?)")
        .bind(name)
        .bind(&now)
        .execute(&state.db)
        .await;

    let _ = tx.send(Outgoing::ok(format!("{} 已获得 OP 权限", name)));
}

async fn require_op(
    state: &Arc<AppState>,
    session_id: Uuid,
) -> Result<String, String> {
    let user = require_user(state, session_id).await?;
    let session = get_session(state, session_id).await?;
    let guard = session.lock().await;
    if !guard.is_op {
        return Err("需要 OP 权限".to_string());
    }
    Ok(user)
}

async fn op_banlist(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    let rows = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT kind, target, reason, banned_at FROM bans ORDER BY banned_at DESC",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    if rows.is_empty() {
        let _ = tx.send(Outgoing::ok("封禁列表为空"));
        return;
    }
    let mut msg = format!("封禁列表 ({} 条):\n", rows.len());
    for (kind, target, reason, _) in rows {
        msg.push_str(&format!(
            "  [{}] {}{}\n",
            kind,
            target,
            reason.map(|r| format!(" - {}", r)).unwrap_or_default()
        ));
    }
    let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
}

async fn op_list(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    let snapshot: Vec<Uuid> = state.sessions.iter().map(|e| *e.key()).collect();
    let mut items = Vec::new();
    for sid in snapshot {
        if let Some(entry) = state.sessions.get(&sid) {
            let sess = entry.value().clone();
            drop(entry);
            let g = sess.lock().await;
            items.push(serde_json::json!({
                "session": sid.to_string(),
                "user": g.user,
                "is_op": g.is_op,
                "connected": g.connected.len(),
            }));
        }
    }
    let _ = tx.send(Outgoing::data("session_list", serde_json::json!(items)));
    let _ = tx.send(Outgoing::ok(format!("在线会话: {} 个", items.len())));
}

async fn op_kick(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP kick [名称] [原因...]"));
        return;
    }
    let name = args[0];
    let reason = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "被管理员踢出".into()
    };
    match state.find_session_by_user(name).await {
        Some((sid, _)) => {
            state.kick(sid, &reason).await;
            let _ = tx.send(Outgoing::ok(format!("已踢出 {}", name)));
        }
        None => {
            let _ = tx.send(Outgoing::err(format!("用户 {} 不在线", name)));
        }
    }
}

async fn op_ban(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP ban [名称] [原因...]"));
        return;
    }
    let name = args[0];
    let reason = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "被管理员封禁".into()
    };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO bans (kind, target, reason, banned_at) VALUES ('name', ?, ?, ?)",
    )
    .bind(name)
    .bind(&reason)
    .bind(&now)
    .execute(&state.db)
    .await;

    if let Some((sid, _)) = state.find_session_by_user(name).await {
        state.kick(sid, &reason).await;
    }
    let _ = tx.send(Outgoing::ok(format!("已封禁 {}", name)));
}

async fn op_ban_ip(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP ban-ip [IP] [原因...]"));
        return;
    }
    let ip = args[0];
    let reason = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "IP 被管理员封禁".into()
    };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO bans (kind, target, reason, banned_at) VALUES ('ip', ?, ?, ?)",
    )
    .bind(ip)
    .bind(&reason)
    .bind(&now)
    .execute(&state.db)
    .await;
    let _ = tx.send(Outgoing::ok(format!("已封禁 IP {}", ip)));
}

async fn op_pardon(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP pardon [名称]"));
        return;
    }
    let name = args[0];
    let r = sqlx::query("DELETE FROM bans WHERE kind = 'name' AND target = ?")
        .bind(name)
        .execute(&state.db)
        .await;
    match r {
        Ok(r) if r.rows_affected() > 0 => {
            let _ = tx.send(Outgoing::ok(format!("已解封 {}", name)));
        }
        _ => {
            let _ = tx.send(Outgoing::err(format!("{} 不在封禁名单中", name)));
        }
    }
}

async fn op_pardon_ip(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e));
        return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP pardon-ip [IP]"));
        return;
    }
    let ip = args[0];
    let r = sqlx::query("DELETE FROM bans WHERE kind = 'ip' AND target = ?")
        .bind(ip)
        .execute(&state.db)
        .await;
    match r {
        Ok(r) if r.rows_affected() > 0 => {
            let _ = tx.send(Outgoing::ok(format!("已解封 IP {}", ip)));
        }
        _ => {
            let _ = tx.send(Outgoing::err(format!("IP {} 不在封禁名单中", ip)));
        }
    }
}