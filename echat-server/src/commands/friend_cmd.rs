use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{require_user, resolve_user_name};
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
            "用法: /Echat online <take|friends|friends-go-online-Reminder> ..."
        ));
        return;
    }
    match args[0].to_lowercase().as_str() {
        "take" => take(state, session_id, &args[1..], tx).await,
        "friends" => friends(state, session_id, &args[1..], tx).await,
        "friends-go-online-reminder" | "friends-go-onile-reminder" => {
            reminder(state, session_id, &args[1..], tx).await
        }
        "groups" | "grups" => {
            let _ = tx.send(Outgoing::err("群聊功能在三期 B 实现"));
        }
        other => {
            let _ = tx.send(Outgoing::err(format!("未知 online 子命令: {}", other)));
        }
    }
}

// ===== take on/off =====

async fn take(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat online take <on|off>"));
        return;
    }
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let on = match args[0].to_lowercase().as_str() {
        "on" => 1,
        "off" => 0,
        _ => { let _ = tx.send(Outgoing::err("用法: /Echat online take <on|off>")); return; }
    };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query("INSERT OR REPLACE INTO user_prefs (name, take_on, updated_at) VALUES (?, ?, ?)")
        .bind(&user).bind(on).bind(&now)
        .execute(&state.db).await;
    let _ = tx.send(Outgoing::ok(
        if on == 1 { "上线提醒已开启（好友上线时会通知你）" } else { "上线提醒已关闭（任何人不会收到你的上线通知）" }
    ));
}

// ===== friends =====

async fn friends(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat online friends <list|search|[NAME] add|[NAME] del|[NAME] Y|[NAME] N> ..."
        ));
        return;
    }
    match args[0].to_lowercase().as_str() {
        "list" => list(state, session_id, tx).await,
        "search" => search(state, session_id, &args[1..], tx).await,
        _ => {
            // [NAME] add/del/Y/N
            if args.len() < 2 {
                let _ = tx.send(Outgoing::err(
                    "用法: /Echat online friends [NAME] <add|del|Y|N>"
                ));
                return;
            }
            let target = args[0];
            match args[1].to_lowercase().as_str() {
                "add" => add_request(state, session_id, target, tx).await,
                "del" => del_friend(state, session_id, target, tx).await,
                "y" => accept_request(state, session_id, target, tx).await,
                "n" => reject_request(state, session_id, target, tx).await,
                other => {
                    let _ = tx.send(Outgoing::err(format!("未知好友操作: {}", other)));
                }
            }
        }
    }
}

async fn list(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT friend, since FROM friends WHERE user = ? ORDER BY friend"
    ).bind(&user).fetch_all(&state.db).await.unwrap_or_default();

    if rows.is_empty() {
        let _ = tx.send(Outgoing::ok("好友列表为空"));
        return;
    }

    let mut items = Vec::new();
    let mut msg = format!("好友列表 ({} 个):\n", rows.len());
    for (fname, since) in rows {
        let online = state.find_session_by_user(&fname).await.is_some();
        msg.push_str(&format!("  [{}] {} (since {})\n", if online { "在线" } else { "离线" }, fname, since));
        items.push(serde_json::json!({"name": fname, "online": online, "since": since}));
    }
    let _ = tx.send(Outgoing::data("friend_list", serde_json::json!(items)));
    let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
}

async fn search(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat online friends search [NAME]"));
        return;
    }
    let me = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let query = args[0];
    let like = format!("%{}%", query);
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM users WHERE LOWER(name) LIKE LOWER(?) LIMIT 30"
    ).bind(&like).fetch_all(&state.db).await.unwrap_or_default();

    if rows.is_empty() {
        let _ = tx.send(Outgoing::ok(format!("未找到匹配 \"{}\" 的用户", query)));
        return;
    }

    let mut items = Vec::new();
    let mut msg = format!("搜索 \"{}\" 结果 ({} 条):\n", query, rows.len());
    for (name,) in rows {
        let online = state.find_session_by_user(&name).await.is_some();
        let is_friend: Option<(String,)> = sqlx::query_as(
            "SELECT friend FROM friends WHERE user = ? AND friend = ?"
        ).bind(&me).bind(&name).fetch_optional(&state.db).await.ok().flatten();
        let i_req: Option<(String,)> = sqlx::query_as(
            "SELECT from_user FROM friend_requests WHERE from_user = ? AND to_user = ?"
        ).bind(&me).bind(&name).fetch_optional(&state.db).await.ok().flatten();
        let they_req: Option<(String,)> = sqlx::query_as(
            "SELECT from_user FROM friend_requests WHERE from_user = ? AND to_user = ?"
        ).bind(&name).bind(&me).fetch_optional(&state.db).await.ok().flatten();

        let mut tags = Vec::new();
        tags.push(if online { "[在线]" } else { "[离线]" }.to_string());
        if is_friend.is_some() { tags.push("[已好友]".into()); }
        if i_req.is_some() { tags.push("[已申请]".into()); }
        if they_req.is_some() { tags.push("[对方申请中]".into()); }

        msg.push_str(&format!("  {} {}\n", tags.join(" "), name));
        items.push(serde_json::json!({
            "name": name, "online": online,
            "is_friend": is_friend.is_some(),
            "i_requested": i_req.is_some(),
            "they_requested": they_req.is_some(),
        }));
    }
    let _ = tx.send(Outgoing::data("friend_search", serde_json::json!(items)));
    let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
}

async fn add_request(
    state: &Arc<AppState>,
    session_id: Uuid,
    target_input: &str,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let from = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let target = match resolve_user_name(&state.db, target_input).await {
        Some(n) => n,
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", target_input))); return; }
    };
    if target.eq_ignore_ascii_case(&from) {
        let _ = tx.send(Outgoing::err("不能加自己为好友"));
        return;
    }

    let exists: Option<(String,)> = sqlx::query_as(
        "SELECT friend FROM friends WHERE user = ? AND friend = ?"
    ).bind(&from).bind(&target).fetch_optional(&state.db).await.ok().flatten();
    if exists.is_some() {
        let _ = tx.send(Outgoing::err(format!("{} 已是你的好友", target)));
        return;
    }

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM friends WHERE user = ?")
        .bind(&from).fetch_one(&state.db).await.unwrap_or((0,));
    if count.0 >= 1000 {
        let _ = tx.send(Outgoing::err("好友数量已达上限（1000）"));
        return;
    }

    // 反向申请存在 → 自动成为好友
    let reverse: Option<(String,)> = sqlx::query_as(
        "SELECT from_user FROM friend_requests WHERE from_user = ? AND to_user = ?"
    ).bind(&target).bind(&from).fetch_optional(&state.db).await.ok().flatten();

    if reverse.is_some() {
        let now = chrono::Utc::now().to_rfc3339();
        let _ = sqlx::query(
            "DELETE FROM friend_requests WHERE (from_user = ? AND to_user = ?) OR (from_user = ? AND to_user = ?)"
        ).bind(&from).bind(&target).bind(&target).bind(&from).execute(&state.db).await;
        let _ = sqlx::query("INSERT OR IGNORE INTO friends (user, friend, since) VALUES (?, ?, ?)")
            .bind(&from).bind(&target).bind(&now).execute(&state.db).await;
        let _ = sqlx::query("INSERT OR IGNORE INTO friends (user, friend, since) VALUES (?, ?, ?)")
            .bind(&target).bind(&from).bind(&now).execute(&state.db).await;
        let _ = tx.send(Outgoing::ok(format!("你和 {} 已成为好友（互相申请自动通过）", target)));
        if let Some((target_sid, _)) = state.find_session_by_user(&target).await {
            state.send_event_filtered(
                target_sid, MsgKind::FriendNotice,
                "Notify:FriendAccept", &format!("你和 {} 已成为好友", from),
            ).await;
        }
        return;
    }

    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO friend_requests (from_user, to_user, created_at) VALUES (?, ?, ?)"
    ).bind(&from).bind(&target).bind(&now).execute(&state.db).await;

    let _ = tx.send(Outgoing::ok(format!("已向 {} 发送好友申请", target)));
    if let Some((target_sid, _)) = state.find_session_by_user(&target).await {
        state.send_event_filtered(
            target_sid, MsgKind::FriendNotice,
            "Notify:FriendRequest",
            &format!("{} 请求添加你为好友，用 /Echat online friends {} Y 同意", from, from),
        ).await;
    }
}

async fn del_friend(
    state: &Arc<AppState>,
    session_id: Uuid,
    target_input: &str,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let from = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let target = match resolve_user_name(&state.db, target_input).await {
        Some(n) => n,
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", target_input))); return; }
    };

    let r1 = sqlx::query("DELETE FROM friends WHERE user = ? AND friend = ?")
        .bind(&from).bind(&target).execute(&state.db).await;
    let r2 = sqlx::query("DELETE FROM friends WHERE user = ? AND friend = ?")
        .bind(&target).bind(&from).execute(&state.db).await;
    let affected = r1.map(|r| r.rows_affected()).unwrap_or(0)
                 + r2.map(|r| r.rows_affected()).unwrap_or(0);
    if affected == 0 {
        let _ = tx.send(Outgoing::err(format!("{} 不是你的好友", target)));
        return;
    }

    let _ = sqlx::query(
        "DELETE FROM friend_reminders WHERE (watcher = ? AND target = ?) OR (watcher = ? AND target = ?)"
    ).bind(&from).bind(&target).bind(&target).bind(&from).execute(&state.db).await;

    let _ = tx.send(Outgoing::ok(format!("已删除好友 {}", target)));
    if let Some((target_sid, _)) = state.find_session_by_user(&target).await {
        state.send_event_filtered(
            target_sid, MsgKind::Force, "Notify:System",
            &format!("{} 已将你从好友列表移除", from),
        ).await;
    }
}

async fn accept_request(
    state: &Arc<AppState>,
    session_id: Uuid,
    target_input: &str,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let me = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let target = match resolve_user_name(&state.db, target_input).await {
        Some(n) => n,
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", target_input))); return; }
    };

    let req: Option<(String,)> = sqlx::query_as(
        "SELECT from_user FROM friend_requests WHERE from_user = ? AND to_user = ?"
    ).bind(&target).bind(&me).fetch_optional(&state.db).await.ok().flatten();
    if req.is_none() {
        let _ = tx.send(Outgoing::err(format!("{} 没有向你发送好友申请", target)));
        return;
    }

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM friends WHERE user = ?")
        .bind(&me).fetch_one(&state.db).await.unwrap_or((0,));
    if count.0 >= 1000 {
        let _ = tx.send(Outgoing::err("好友数量已达上限（1000）"));
        return;
    }

    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query("DELETE FROM friend_requests WHERE from_user = ? AND to_user = ?")
        .bind(&target).bind(&me).execute(&state.db).await;
    let _ = sqlx::query("INSERT OR IGNORE INTO friends (user, friend, since) VALUES (?, ?, ?)")
        .bind(&me).bind(&target).bind(&now).execute(&state.db).await;
    let _ = sqlx::query("INSERT OR IGNORE INTO friends (user, friend, since) VALUES (?, ?, ?)")
        .bind(&target).bind(&me).bind(&now).execute(&state.db).await;

    let _ = tx.send(Outgoing::ok(format!("你和 {} 已成为好友", target)));
    if let Some((target_sid, _)) = state.find_session_by_user(&target).await {
        state.send_event_filtered(
            target_sid, MsgKind::FriendNotice,
            "Notify:FriendAccept", &format!("{} 同意了你的好友申请", me),
        ).await;
    }
}

async fn reject_request(
    state: &Arc<AppState>,
    session_id: Uuid,
    target_input: &str,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let me = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let target = match resolve_user_name(&state.db, target_input).await {
        Some(n) => n,
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", target_input))); return; }
    };

    let r = sqlx::query("DELETE FROM friend_requests WHERE from_user = ? AND to_user = ?")
        .bind(&target).bind(&me).execute(&state.db).await;
    match r {
        Ok(r) if r.rows_affected() > 0 => {
            let _ = tx.send(Outgoing::ok(format!("已拒绝 {} 的好友申请", target)));
        }
        _ => {
            let _ = tx.send(Outgoing::err(format!("{} 没有向你发送好友申请", target)));
        }
    }
}

// ===== friends-go-online-Reminder =====

async fn reminder(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat online friends-go-online-Reminder <on|off> [NAME]"
        ));
        return;
    }
    let action = args[0].to_lowercase();
    let target_input = args[1];
    let me = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let target = match resolve_user_name(&state.db, target_input).await {
        Some(n) => n,
        None => { let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", target_input))); return; }
    };

    let is_friend: Option<(String,)> = sqlx::query_as(
        "SELECT friend FROM friends WHERE user = ? AND friend = ?"
    ).bind(&me).bind(&target).fetch_optional(&state.db).await.ok().flatten();
    if is_friend.is_none() {
        let _ = tx.send(Outgoing::err(format!("{} 不是你的好友", target)));
        return;
    }

    match action.as_str() {
        "on" => {
            let _ = sqlx::query(
                "INSERT OR IGNORE INTO friend_reminders (watcher, target) VALUES (?, ?)"
            ).bind(&me).bind(&target).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("{} 上线时将提醒你", target)));
        }
        "off" => {
            let _ = sqlx::query(
                "DELETE FROM friend_reminders WHERE watcher = ? AND target = ?"
            ).bind(&me).bind(&target).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已关闭 {} 的上线提醒", target)));
        }
        _ => {
            let _ = tx.send(Outgoing::err(
                "用法: /Echat online friends-go-online-Reminder <on|off> [NAME]"
            ));
        }
    }
}