use std::sync::Arc;

use argon2::{Argon2, PasswordHash, PasswordVerifier};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{get_session, normalize_friend_target, require_user, resolve_user_name};
use crate::protocol::Outgoing;
use crate::state::{AppState, MsgKind};

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat OP <Login|banlist|list|kick|ban|ban-ip|pardon|pardon-ip|friends|grups> ..."
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
        "friends" => op_friends(state, session_id, &args[1..], tx).await,
        "grups" | "groups" => op_grups(state, session_id, &args[1..], tx).await,
        other => { let _ = tx.send(Outgoing::err(format!("未知 OP 子命令: {}", other))); }
    }
}

async fn op_login(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 3 {
        let _ = tx.send(Outgoing::err("用法: /Echat OP Login [名称] [密码] [OP密码]"));
        return;
    }
    let (name_input, password, op_password) = (args[0], args[1], args[2]);

    let stored = state.op_password_hash.read().await.clone();
    let op_hash = match stored {
        Some(h) => h,
        None => {
            let _ = tx.send(Outgoing::err(
                "OP 密码未设置，请联系管理员使用控制台 password-op [PWD] 设置"
            ));
            return;
        }
    };
    let op_parsed = match PasswordHash::new(&op_hash) {
        Ok(p) => p,
        Err(_) => { let _ = tx.send(Outgoing::err("服务端 OP 密码数据损坏")); return; }
    };
    if Argon2::default().verify_password(op_password.as_bytes(), &op_parsed).is_err() {
        let _ = tx.send(Outgoing::err("OP 密码错误"));
        return;
    }

    let name = match resolve_user_name(&state.db, name_input).await {
        Some(n) => n,
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", name_input))); return; }
    };
    let row: Option<(String,)> =
        sqlx::query_as("SELECT password_hash FROM users WHERE name = ?")
            .bind(&name).fetch_optional(&state.db).await.ok().flatten();
    let user_hash = match row {
        Some((h,)) => h,
        None => { let _ = tx.send(Outgoing::err("用户数据异常")); return; }
    };
    let user_parsed = match PasswordHash::new(&user_hash) {
        Ok(p) => p,
        Err(_) => { let _ = tx.send(Outgoing::err("密码数据损坏")); return; }
    };
    if Argon2::default().verify_password(password.as_bytes(), &user_parsed).is_err() {
        let _ = tx.send(Outgoing::err("用户密码错误"));
        return;
    }

    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    {
        let mut guard = session.lock().await;
        guard.is_op = true;
        guard.user = Some(name.clone());
    }

    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query("INSERT OR IGNORE INTO ops (name, granted_at) VALUES (?, ?)")
        .bind(&name).bind(&now).execute(&state.db).await;

    let _ = tx.send(Outgoing::ok(format!("{} 已获得 OP 权限", name)));
}

async fn require_op(state: &Arc<AppState>, session_id: Uuid) -> Result<String, String> {
    let user = require_user(state, session_id).await?;
    let session = get_session(state, session_id).await?;
    let guard = session.lock().await;
    if !guard.is_op {
        return Err("需要 OP 权限（先执行 /Echat OP Login ...）".to_string());
    }
    Ok(user)
}

async fn op_banlist(state: &Arc<AppState>, session_id: Uuid, tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    let rows = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT kind, target, reason, banned_at FROM bans ORDER BY banned_at DESC"
    ).fetch_all(&state.db).await.unwrap_or_default();
    if rows.is_empty() {
        let _ = tx.send(Outgoing::ok("封禁列表为空"));
        return;
    }
    let mut msg = format!("封禁列表 ({} 条):\n", rows.len());
    let mut items = Vec::new();
    for (kind, target, reason, at) in rows {
        msg.push_str(&format!("  [{}] {}{}\n", kind, target,
            reason.as_deref().map(|r| format!(" - {}", r)).unwrap_or_default()));
        items.push(serde_json::json!({"kind": kind, "target": target, "reason": reason, "banned_at": at}));
    }
    let _ = tx.send(Outgoing::data("ban_list", serde_json::json!(items)));
    let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
}

async fn op_list(state: &Arc<AppState>, session_id: Uuid, tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    let snapshot: Vec<Uuid> = state.sessions.iter().map(|e| *e.key()).collect();
    let mut items = Vec::new();
    for sid in snapshot {
        if let Some(entry) = state.sessions.get(&sid) {
            let sess = entry.value().clone(); drop(entry);
            let g = sess.lock().await;
            items.push(serde_json::json!({
                "session": sid.to_string(), "user": g.user,
                "is_op": g.is_op, "connected": g.connected.len(),
            }));
        }
    }
    let _ = tx.send(Outgoing::data("session_list", serde_json::json!(items)));
    let _ = tx.send(Outgoing::ok(format!("在线会话: {} 个", items.len())));
}

async fn op_kick(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP kick [名称] [原因...]"));
        return;
    }
    let name = args[0];
    let reason = if args.len() > 1 { args[1..].join(" ") } else { "被管理员踢出".into() };
    match state.find_session_by_user(name).await {
        Some((sid, _)) => {
            state.kick(sid, &reason).await;
            let _ = tx.send(Outgoing::ok(format!("已踢出 {}", name)));
        }
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不在线", name))); }
    }
}

async fn op_ban(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP ban [名称] [原因...]"));
        return;
    }
    let name = args[0];
    let reason = if args.len() > 1 { args[1..].join(" ") } else { "被管理员封禁".into() };
    if name.eq_ignore_ascii_case("System") || name.eq_ignore_ascii_case("Console") || name.eq_ignore_ascii_case("Global") {
        let _ = tx.send(Outgoing::err("不能封禁系统保留名称"));
        return;
    }
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO bans (kind, target, reason, banned_at) VALUES ('name', ?, ?, ?)"
    ).bind(name).bind(&reason).bind(&now).execute(&state.db).await;
    if let Some((sid, _)) = state.find_session_by_user(name).await {
        state.kick(sid, &reason).await;
    }
    let _ = tx.send(Outgoing::ok(format!("已封禁 {}", name)));
}

async fn op_ban_ip(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP ban-ip [IP] [原因...]"));
        return;
    }
    let ip = args[0];
    let reason = if args.len() > 1 { args[1..].join(" ") } else { "IP 被管理员封禁".into() };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO bans (kind, target, reason, banned_at) VALUES ('ip', ?, ?, ?)"
    ).bind(ip).bind(&reason).bind(&now).execute(&state.db).await;
    let _ = tx.send(Outgoing::ok(format!("已封禁 IP {}", ip)));
}

async fn op_pardon(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP pardon [名称]"));
        return;
    }
    let name = args[0];
    let r = sqlx::query("DELETE FROM bans WHERE kind = 'name' AND target = ?")
        .bind(name).execute(&state.db).await;
    match r {
        Ok(r) if r.rows_affected() > 0 => {
            let _ = tx.send(Outgoing::ok(format!("已解封 {}", name)));
        }
        _ => { let _ = tx.send(Outgoing::err(format!("{} 不在封禁名单中", name))); }
    }
}

async fn op_pardon_ip(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP pardon-ip [IP]"));
        return;
    }
    let ip = args[0];
    let r = sqlx::query("DELETE FROM bans WHERE kind = 'ip' AND target = ?")
        .bind(ip).execute(&state.db).await;
    match r {
        Ok(r) if r.rows_affected() > 0 => {
            let _ = tx.send(Outgoing::ok(format!("已解封 IP {}", ip)));
        }
        _ => { let _ = tx.send(Outgoing::err(format!("IP {} 不在封禁名单中", ip))); }
    }
}

async fn op_friends(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat OP friends <del|get> [A] to [B] ..."));
        return;
    }
    if let Err(e) = require_op(state, session_id).await {
        let _ = tx.send(Outgoing::err(e)); return;
    }
    match args[0].to_lowercase().as_str() {
        "del" => {
            if args.len() < 4 || !args[2].eq_ignore_ascii_case("to") {
                let _ = tx.send(Outgoing::err("用法: /Echat OP friends del [A] to [B]"));
                return;
            }
            let a = match resolve_user_name(&state.db, args[1]).await {
                Some(n) => n,
                None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", args[1]))); return; }
            };
            let b = match resolve_user_name(&state.db, args[3]).await {
                Some(n) => n,
                None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", args[3]))); return; }
            };
            let _ = sqlx::query(
                "DELETE FROM friends WHERE (user = ? AND friend = ?) OR (user = ? AND friend = ?)"
            ).bind(&a).bind(&b).bind(&b).bind(&a).execute(&state.db).await;
            let _ = sqlx::query(
                "DELETE FROM friend_reminders WHERE (watcher = ? AND target = ?) OR (watcher = ? AND target = ?)"
            ).bind(&a).bind(&b).bind(&b).bind(&a).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已解除 {} 和 {} 的好友关系", a, b)));
            if let Some((sid, _)) = state.find_session_by_user(&a).await {
                state.send_event_filtered(
                    sid, MsgKind::Force, "Notify:System",
                    &format!("你与 {} 的好友关系已被管理员解除", b),
                ).await;
            }
            if let Some((sid, _)) = state.find_session_by_user(&b).await {
                state.send_event_filtered(
                    sid, MsgKind::Force, "Notify:System",
                    &format!("你与 {} 的好友关系已被管理员解除", a),
                ).await;
            }
        }
        "get" => {
            if args.len() < 6
                || !args[2].eq_ignore_ascii_case("to")
                || !args[4].eq_ignore_ascii_case("limit")
            {
                let _ = tx.send(Outgoing::err("用法: /Echat OP friends get [A] to [B] Limit [N]"));
                return;
            }
            let a = match resolve_user_name(&state.db, args[1]).await {
                Some(n) => n,
                None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", args[1]))); return; }
            };
            let b = match resolve_user_name(&state.db, args[3]).await {
                Some(n) => n,
                None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", args[3]))); return; }
            };
            let limit: usize = match args[5].parse() {
                Ok(n) if n > 0 => n,
                _ => { let _ = tx.send(Outgoing::err("Limit 必须是正整数")); return; }
            };
            if limit > 5000 { let _ = tx.send(Outgoing::err("Limit 不能超过 5000")); return; }
            let pair = normalize_friend_target(&a, &b);
            let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
                "SELECT id, from_user, content, created_at FROM messages \
                 WHERE kind='friend' AND target=? ORDER BY id DESC LIMIT ?"
            ).bind(&pair).bind(limit as i64).fetch_all(&state.db).await.unwrap_or_default();

            let mut items = Vec::new();
            for (id, from, content, at) in rows {
                items.push(serde_json::json!({"id": id, "from": from, "content": content, "at": at}));
            }
            items.reverse();
            let _ = tx.send(Outgoing::data("friend_messages", serde_json::json!({
                "a": a, "b": b, "messages": items
            })));
            let _ = tx.send(Outgoing::ok(format!("返回 {} 和 {} 最近 {} 条消息", a, b, items.len())));
        }
        other => { let _ = tx.send(Outgoing::err(format!("未知 OP friends 子命令: {}", other))); }
    }
}

async fn op_grups(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() { let _ = tx.send(Outgoing::err("用法: /Echat op grups <del|get> [UUID] ...")); return; }
    if let Err(e) = require_op(state, session_id).await { let _ = tx.send(Outgoing::err(e)); return; }
    
    match args[0].to_lowercase().as_str() {
        "del" => {
            if args.len() < 2 { let _ = tx.send(Outgoing::err("用法: op grups del [UUID]")); return; }
            let uuid = args[1];
            let g: Option<(String,)> = sqlx::query_as("SELECT name FROM groups WHERE uuid = ?").bind(uuid).fetch_optional(&state.db).await.ok().flatten();
            if g.is_none() { let _ = tx.send(Outgoing::err("群不存在")); return; }
            let g_name = g.unwrap().0;
            
            let _ = sqlx::query("DELETE FROM groups WHERE uuid = ?").bind(uuid).execute(&state.db).await;
            let _ = sqlx::query("DELETE FROM group_members WHERE group_uuid = ?").bind(uuid).execute(&state.db).await;
            let _ = sqlx::query("DELETE FROM group_invites WHERE group_uuid = ?").bind(uuid).execute(&state.db).await;
            let _ = sqlx::query("DELETE FROM group_join_requests WHERE group_uuid = ?").bind(uuid).execute(&state.db).await;
            let _ = sqlx::query("DELETE FROM messages WHERE kind='group' AND target = ?").bind(uuid).execute(&state.db).await;
            
            let _ = tx.send(Outgoing::ok(format!("群 {} 已被解散", g_name)));
            let _ = state.broadcast_filtered(MsgKind::Force, "Notify:System", &format!("群聊 {} 已被管理员解散", g_name)).await;
        }
        "get" => {
            if args.len() < 5 || !args[2].eq_ignore_ascii_case("limit") { let _ = tx.send(Outgoing::err("用法: op grups get [UUID] Limit [行数]")); return; }
            let uuid = args[1];
            let limit: usize = match args[3].parse() { Ok(n) if n > 0 && n <= 5000 => n, _ => { let _ = tx.send(Outgoing::err("行数必须 1-5000")); return; } };
            
            let g: Option<(String,)> = sqlx::query_as("SELECT name FROM groups WHERE uuid = ?").bind(uuid).fetch_optional(&state.db).await.ok().flatten();
            if g.is_none() { let _ = tx.send(Outgoing::err("群不存在")); return; }
            
            let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
                "SELECT id, from_user, content, created_at FROM messages WHERE kind='group' AND target=? ORDER BY id DESC LIMIT ?"
            ).bind(uuid).bind(limit as i64).fetch_all(&state.db).await.unwrap_or_default();
            
            let mut items = Vec::new();
            for (id, from, content, at) in rows { items.push(serde_json::json!({"id": id, "from": from, "content": content, "at": at})); }
            items.reverse();
            let _ = tx.send(Outgoing::data("group_messages", serde_json::json!({"messages": items})));
            let _ = tx.send(Outgoing::ok(format!("返回群 {} 最近 {} 条消息", g.unwrap().0, items.len())));
        }
        _ => { let _ = tx.send(Outgoing::err("未知 op grups 子命令")); }
    }
}