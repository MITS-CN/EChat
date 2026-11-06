use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;
use argon2::{Argon2, PasswordHash, PasswordVerifier, password_hash::{SaltString, rand_core::OsRng}, PasswordHasher};

use super::{require_user, resolve_user_name, trim_messages};
use crate::protocol::Outgoing;
use crate::state::{AppState, MsgKind};

// 解析群标识：UUID、6位ShortID、或 群名#ShortID
pub async fn resolve_group(state: &AppState, input: &str) -> Option<(String, String, String)> {
    let uuid_res: Option<(String, String)> = if input.contains('#') {
        let parts: Vec<&str> = input.split('#').collect();
        if parts.len() != 2 { return None; }
        sqlx::query_as("SELECT uuid, name FROM groups WHERE name = ? AND substr(replace(uuid, '-', ''), 1, 6) = ?")
            .bind(parts[0]).bind(parts[1]).fetch_optional(&state.db).await.ok().flatten()
    } else if input.len() == 36 && input.contains('-') {
        sqlx::query_as("SELECT uuid, name FROM groups WHERE uuid = ?")
            .bind(input).fetch_optional(&state.db).await.ok().flatten()
    } else if input.len() == 6 && !input.contains('-') {
        sqlx::query_as("SELECT uuid, name FROM groups WHERE substr(replace(uuid, '-', ''), 1, 6) = ?")
            .bind(input).fetch_optional(&state.db).await.ok().flatten()
    } else {
        sqlx::query_as("SELECT uuid, name FROM groups WHERE name = ? LIMIT 1")
            .bind(input).fetch_optional(&state.db).await.ok().flatten()
    };
    if let Some((uuid, name)) = uuid_res {
        let short_id = uuid.replace("-", "")[..6].to_string();
        Some((uuid, name, short_id))
    } else { None }
}

fn format_group_tag(name: &str, short_id: &str) -> String {
    format!("{}#{}", name, short_id)
}

pub async fn get_member_role(state: &AppState, group_uuid: &str, user: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT role FROM group_members WHERE group_uuid = ? AND user = ?"
    ).bind(group_uuid).bind(user).fetch_optional(&state.db).await.ok().flatten()
}

async fn broadcast_to_group(state: &AppState, group_uuid: &str, source: &str, content: &str) {
    let members: Vec<(String,)> = sqlx::query_as(
        "SELECT user FROM group_members WHERE group_uuid = ?"
    ).bind(group_uuid).fetch_all(&state.db).await.unwrap_or_default();

    let g_name: Option<(String,)> = sqlx::query_as("SELECT name FROM groups WHERE uuid = ?")
        .bind(group_uuid).fetch_one(&state.db).await.ok();
    let kind = MsgKind::Group(g_name.map(|(n,)| n).unwrap_or_default());

    for (m,) in members {
        if let Some((sid, _)) = state.find_session_by_user(&m).await {
            state.send_event_filtered(sid, kind.clone(), source, content).await;
        }
    }
}

// ======== 路由入口 ========

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat online groups <new|uuid|setting|kick|user|join|search|in|history> ..."));
        return;
    }
    match args[0].to_lowercase().as_str() {
        "new" => cmd_new(state, session_id, &args[1..], tx).await,
        "uuid" => cmd_uuid(state, session_id, &args[1..], tx).await,
        "setting" => cmd_setting(state, session_id, &args[1..], tx).await,
        "kick" => cmd_kick(state, session_id, &args[1..], tx).await,
        "user" => cmd_user(state, session_id, &args[1..], tx).await,
        "join" => cmd_join(state, session_id, &args[1..], tx).await,
        "search" => cmd_search(state, session_id, &args[1..], tx).await,
        "in" => cmd_in(state, session_id, &args[1..], tx).await,
        "history" => cmd_history(state, session_id, &args[1..], tx).await,
        other => { let _ = tx.send(Outgoing::err(format!("未知 groups 子命令: {}", other))); }
    }
}

pub async fn handle_gsay(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat gsay [群标识] [要说的话]"));
        return;
    }
    let group_input = args[0];
    let content = args[1..].join(" ");
    if content.chars().count() > 2000 {
        let _ = tx.send(Outgoing::err("群消息不能超过 2000 字符")); return;
    }
    let me = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };
    let (uuid, name, short_id) = match resolve_group(state, group_input).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("找不到该群")); return; } };

    if get_member_role(state, &uuid, &me).await.is_none() {
        let _ = tx.send(Outgoing::err("你不在该群中，无法发言")); return;
    }

    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query("INSERT INTO messages (kind, target, from_user, content, created_at) VALUES ('group', ?, ?, ?, ?)")
        .bind(&uuid).bind(&me).bind(&content).bind(&now).execute(&state.db).await;
    trim_messages(&state.db, "group", &uuid, 1000).await;

    let tag = format_group_tag(&name, &short_id);
    let msg = format!("<{}> {}", me, content);
    let _ = tx.send(Outgoing::ok("已发送"));
    broadcast_to_group(state, &uuid, &format!("Group:{}", tag), &msg).await;
}

// ======== 子命令实现 ========

async fn cmd_new(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.is_empty() { let _ = tx.send(Outgoing::err("用法: groups new [群名]")); return; }
    let name = args[0];
    if name.len() > 32 || name.contains(' ') || name.contains('#') || name.is_empty() {
        let _ = tx.send(Outgoing::err("群名须为 1-32 字符，不含空格或 #")); return;
    }
    let owner = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };

    let uuid = Uuid::new_v4().to_string();
    let short_id = uuid.replace("-", "")[..6].to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let _ = sqlx::query("INSERT INTO groups (uuid, name, owner, created_at) VALUES (?, ?, ?, ?)")
        .bind(&uuid).bind(name).bind(&owner).bind(&now).execute(&state.db).await;
    let _ = sqlx::query("INSERT INTO group_members (group_uuid, user, role, joined_at) VALUES (?, ?, 'owner', ?)")
        .bind(&uuid).bind(&owner).bind(&now).execute(&state.db).await;

    let tag = format_group_tag(name, &short_id);
    let _ = tx.send(Outgoing::ok(format!("群聊 {} 创建成功。请用 {} 进行日常操作", tag, tag)));
}

async fn cmd_uuid(state: &Arc<AppState>, _session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.is_empty() { let _ = tx.send(Outgoing::err("用法: groups uuid [群标识]")); return; }
    if let Some((uuid, _, short_id)) = resolve_group(state, args[0]).await {
        let _ = tx.send(Outgoing::ok(format!("群 {} 的 UUID: {} (ShortID: {})", args[0], uuid, short_id)));
    } else { let _ = tx.send(Outgoing::err("找不到该群")); }
}

async fn cmd_setting(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.len() < 2 { let _ = tx.send(Outgoing::err("用法: groups setting [群] <admin|unadmin|owner|search|rename|everyone|question|Introduction> ...")); return; }
    let user = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };
    let (uuid, name, _) = match resolve_group(state, args[0]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };
    
    let role = get_member_role(state, &uuid, &user).await;
    let is_owner = role.as_deref() == Some("owner");
    let is_admin = role.as_deref() == Some("admin") || is_owner;
    if !is_admin { let _ = tx.send(Outgoing::err("需要群管理员权限")); return; }

    match args[1].to_lowercase().as_str() {
        "admin" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] admin [NAME]")); return; }
            let target = match resolve_user_name(&state.db, args[2]).await { Some(n) => n, None => { let _ = tx.send(Outgoing::err("用户不存在")); return; } };
            let _ = sqlx::query("UPDATE group_members SET role = 'admin' WHERE group_uuid = ? AND user = ?")
                .bind(&uuid).bind(&target).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已将 {} 设为管理员", target)));
        }
        "unadmin" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] unadmin [NAME]")); return; }
            let target = match resolve_user_name(&state.db, args[2]).await { Some(n) => n, None => { let _ = tx.send(Outgoing::err("用户不存在")); return; } };
            let _ = sqlx::query("UPDATE group_members SET role = 'member' WHERE group_uuid = ? AND user = ? AND role = 'admin'")
                .bind(&uuid).bind(&target).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已取消 {} 的管理员权限", target)));
        }
        "owner" => {
            if !is_owner { let _ = tx.send(Outgoing::err("只有群主能转让")); return; }
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] owner [NAME]")); return; }
            let target = match resolve_user_name(&state.db, args[2]).await { Some(n) => n, None => { let _ = tx.send(Outgoing::err("用户不存在")); return; } };
            let _ = sqlx::query("UPDATE groups SET owner = ? WHERE uuid = ?").bind(&target).bind(&uuid).execute(&state.db).await;
            let _ = sqlx::query("UPDATE group_members SET role = 'owner' WHERE group_uuid = ? AND user = ?").bind(&uuid).bind(&target).execute(&state.db).await;
            let _ = sqlx::query("UPDATE group_members SET role = 'admin' WHERE group_uuid = ? AND user = ?").bind(&uuid).bind(&user).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已将群主转让给 {}", target)));
        }
        "search" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] search Y|N")); return; }
            let allow = if args[2].eq_ignore_ascii_case("Y") { 1 } else { 0 };
            let _ = sqlx::query("UPDATE groups SET searchable = ? WHERE uuid = ?").bind(allow).bind(&uuid).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("搜索允许状态已更新")));
        }
        "rename" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] rename [新名字]")); return; }
            let new_name = args[2];
            if new_name.len() > 32 || new_name.contains(' ') || new_name.contains('#') { let _ = tx.send(Outgoing::err("名字格式错误")); return; }
            let _ = sqlx::query("UPDATE groups SET name = ? WHERE uuid = ?").bind(new_name).bind(&uuid).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已将群 {} 重命名为 {}", name, new_name)));
        }
        "everyone" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] everyone Y|A|N|B")); return; }
            let mode = args[2].to_uppercase();
            if !["Y","A","N","B"].contains(&mode.as_str()) { let _ = tx.send(Outgoing::err("模式必须是 Y/A/N/B")); return; }
            if mode == "B" {
                let has_q: Option<(String,)> = sqlx::query_as("SELECT question FROM groups WHERE uuid = ? AND question IS NOT NULL")
                    .bind(&uuid).fetch_optional(&state.db).await.ok().flatten();
                if has_q.is_none() { let _ = tx.send(Outgoing::err("请先设置问题: setting [群] question set [问题] answer [答案]")); return; }
            }
            let _ = sqlx::query("UPDATE groups SET join_mode = ? WHERE uuid = ?").bind(&mode).bind(&uuid).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("加入模式已设为 {}", mode)));
        }
        "question" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] question <set [问题] answer [答案]|clear>")); return; }
            if args[2].eq_ignore_ascii_case("clear") {
                let _ = sqlx::query("UPDATE groups SET question = NULL, answer_hash = NULL WHERE uuid = ?").bind(&uuid).execute(&state.db).await;
                let _ = tx.send(Outgoing::ok("问题已清除"));
            } else if args[2].eq_ignore_ascii_case("set") && args.len() >= 6 && args[4].eq_ignore_ascii_case("answer") {
                let q = args[3];
                let a = args[5].to_lowercase(); // 不区分大小写
                let salt = SaltString::generate(&mut OsRng);
                let hash = Argon2::default().hash_password(a.as_bytes(), &salt).unwrap().to_string();
                let _ = sqlx::query("UPDATE groups SET question = ?, answer_hash = ? WHERE uuid = ?")
                    .bind(q).bind(&hash).bind(&uuid).execute(&state.db).await;
                let _ = tx.send(Outgoing::ok("问题和答案已设置"));
            } else { let _ = tx.send(Outgoing::err("格式错误")); }
        }
        "introduction" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: setting [群] Introduction [简介]")); return; }
            let intro = args[2..].join(" ");
            if intro.chars().count() > 200 { let _ = tx.send(Outgoing::err("简介不能超过 200 字符")); return; }
            let _ = sqlx::query("UPDATE groups SET intro = ? WHERE uuid = ?").bind(&intro).bind(&uuid).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok("简介已更新"));
        }
        _ => { let _ = tx.send(Outgoing::err("未知设置项")); }
    }
}

async fn cmd_kick(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.len() < 4 || !args[2].eq_ignore_ascii_case("by") { let _ = tx.send(Outgoing::err("用法: groups kick [NAME] by [群]")); return; }
    let me = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };
    let target = match resolve_user_name(&state.db, args[0]).await { Some(n) => n, None => { let _ = tx.send(Outgoing::err("用户不存在")); return; } };
    let (uuid, name, _) = match resolve_group(state, args[3]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };

    let my_role = get_member_role(state, &uuid, &me).await;
    let target_role = get_member_role(state, &uuid, &target).await;

    if target_role.is_none() { let _ = tx.send(Outgoing::err("该用户不在群里")); return; }
    if my_role.as_deref() == Some("admin") && target_role.as_deref() == Some("admin") { let _ = tx.send(Outgoing::err("管理员无法踢出同级管理员")); return; }
    if my_role.as_deref() != Some("owner") && target_role.as_deref() == Some("owner") { let _ = tx.send(Outgoing::err("不能踢出群主")); return; }
    if my_role.as_deref() != Some("owner") && my_role.as_deref() != Some("admin") { let _ = tx.send(Outgoing::err("需要管理权限")); return; }

    let _ = sqlx::query("DELETE FROM group_members WHERE group_uuid = ? AND user = ?").bind(&uuid).bind(&target).execute(&state.db).await;
    let _ = tx.send(Outgoing::ok(format!("已将 {} 踢出群聊 {}", target, name)));
    broadcast_to_group(state, &uuid, "Notify:System", &format!("{} 已被管理员踢出群聊", target)).await;
}

async fn cmd_user(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.len() < 2 { let _ = tx.send(Outgoing::err("用法: groups user <add|Agree|unagree> ...")); return; }
    let me = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };

    match args[0].to_lowercase().as_str() {
        "add" => {
            if args.len() < 4 || !args[2].eq_ignore_ascii_case("to") { let _ = tx.send(Outgoing::err("用法: groups user add [NAME] to [群]")); return; }
            let target = match resolve_user_name(&state.db, args[1]).await { Some(n) => n, None => { let _ = tx.send(Outgoing::err("用户不存在")); return; } };
            let (uuid, name, _) = match resolve_group(state, args[3]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };
            
            if get_member_role(state, &uuid, &me).await.is_none() { let _ = tx.send(Outgoing::err("你不在该群")); return; }
            if get_member_role(state, &uuid, &target).await.is_some() { let _ = tx.send(Outgoing::err("对方已在群内")); return; }
            
            let now = chrono::Utc::now().to_rfc3339();
            let _ = sqlx::query("INSERT OR REPLACE INTO group_invites (group_uuid, user, from_user, created_at) VALUES (?, ?, ?, ?)")
                .bind(&uuid).bind(&target).bind(&me).bind(&now).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已邀请 {} 加入群聊", target)));
            if let Some((sid, _)) = state.find_session_by_user(&target).await {
                let _ = state.send_to(sid, Outgoing::event("Notify:GroupInvite", format!("{} 邀请你加入群 {}，用 /Echat online groups user Agree to {} 同意", me, name, name))).await;
            }
        }
        "agree" | "unagree" => {
            if args.len() < 3 || !args[1].eq_ignore_ascii_case("to") { let _ = tx.send(Outgoing::err("用法: groups user Agree to [群]")); return; }
            let (uuid, name, _) = match resolve_group(state, args[2]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };
            
            let inv: Option<(String,)> = sqlx::query_as("SELECT from_user FROM group_invites WHERE group_uuid = ? AND user = ?")
                .bind(&uuid).bind(&me).fetch_optional(&state.db).await.ok().flatten();
            if inv.is_none() { let _ = tx.send(Outgoing::err("没有待处理的邀请")); return; }

            if args[0].eq_ignore_ascii_case("agree") {
                let now = chrono::Utc::now().to_rfc3339();
                let _ = sqlx::query("INSERT IGNORE INTO group_members (group_uuid, user, role, joined_at) VALUES (?, ?, 'member', ?)")
                    .bind(&uuid).bind(&me).bind(&now).execute(&state.db).await;
                let _ = tx.send(Outgoing::ok(format!("已加入群聊 {}", name)));
                broadcast_to_group(state, &uuid, "Notify:System", &format!("{} 加入了群聊", me)).await;
            } else {
                let _ = tx.send(Outgoing::ok(format!("已拒绝加入 {}", name)));
            }
            let _ = sqlx::query("DELETE FROM group_invites WHERE group_uuid = ? AND user = ?").bind(&uuid).bind(&me).execute(&state.db).await;
        }
        _ => { let _ = tx.send(Outgoing::err("未知 user 子命令")); }
    }
}

async fn cmd_join(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.is_empty() { let _ = tx.send(Outgoing::err("用法: groups join [群标识] [answer [答案]]")); return; }
    let me = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };
    let (uuid, name, _) = match resolve_group(state, args[0]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };

    if get_member_role(state, &uuid, &me).await.is_some() { let _ = tx.send(Outgoing::err("你已在群内")); return; }

    let row: Option<(String, Option<String>)> = sqlx::query_as("SELECT join_mode, answer_hash FROM groups WHERE uuid = ?")
        .bind(&uuid).fetch_one(&state.db).await.ok();
    let (mode, ans_hash) = match row { Some(r) => r, None => { let _ = tx.send(Outgoing::err("群配置异常")); return; } };

    match mode.as_str() {
        "Y" => {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = sqlx::query("INSERT INTO group_members (group_uuid, user, role, joined_at) VALUES (?, ?, 'member', ?)")
                .bind(&uuid).bind(&me).bind(&now).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已成功加入群聊 {}", name)));
            broadcast_to_group(state, &uuid, "Notify:System", &format!("{} 加入了群聊", me)).await;
        }
        "N" => { let _ = tx.send(Outgoing::err("该群不允许任何人主动加入，只能被邀请")); }
        "A" => {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = sqlx::query("INSERT OR REPLACE INTO group_join_requests (group_uuid, user, created_at) VALUES (?, ?, ?)")
                .bind(&uuid).bind(&me).bind(&now).execute(&state.db).await;
            let _ = tx.send(Outgoing::ok(format!("已申请加入群 {}，请等待管理员审批", name)));
        }
        "B" => {
            if args.len() < 4 || !args[2].eq_ignore_ascii_case("answer") {
                let q: Option<(Option<String>,)> = sqlx::query_as("SELECT question FROM groups WHERE uuid = ?")
                    .bind(&uuid).fetch_one(&state.db).await.ok();
                let q_str = q.and_then(|(s,)| s).unwrap_or_default();
                let _ = tx.send(Outgoing::err(format!("该群需要回答问题才能加入。请使用: groups join {} answer [答案]。问题: {}", name, q_str)));
                return;
            }
            let ans = args[3].to_lowercase();
            let parsed = match PasswordHash::new(ans_hash.as_deref().unwrap_or("")) {
                Ok(p) => p, Err(_) => { let _ = tx.send(Outgoing::err("群配置错误")); return; }
            };
            let ok = Argon2::default().verify_password(ans.as_bytes(), &parsed).is_ok();
            let now = chrono::Utc::now().to_rfc3339();
            let _ = sqlx::query("INSERT INTO group_join_attempts (group_uuid, user, answer, success, created_at) VALUES (?, ?, ?, ?, ?)")
                .bind(&uuid).bind(&me).bind(&ans).bind(if ok {1} else {0}).bind(&now).execute(&state.db).await;

            if ok {
                let _ = sqlx::query("INSERT INTO group_members (group_uuid, user, role, joined_at) VALUES (?, ?, 'member', ?)")
                    .bind(&uuid).bind(&me).bind(&now).execute(&state.db).await;
                let _ = tx.send(Outgoing::ok(format!("回答正确，已加入群聊 {}", name)));
                broadcast_to_group(state, &uuid, "Notify:System", &format!("{} 加入了群聊", me)).await;
            } else {
                let _ = tx.send(Outgoing::err("答案错误，无法加入"));
            }
        }
        _ => {}
    }
}

async fn cmd_search(state: &Arc<AppState>, _session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.is_empty() { let _ = tx.send(Outgoing::err("用法: groups search [UUID] 或 groups search name [群名]")); return; }
    let mut items = Vec::new();
    
    if args[0].eq_ignore_ascii_case("name") && args.len() >= 2 {
        let like = format!("%{}%", args[1]);
        let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
            "SELECT uuid, name, intro, (SELECT COUNT(*) FROM group_members WHERE group_uuid = groups.uuid) FROM groups WHERE searchable = 1 AND LOWER(name) LIKE LOWER(?) LIMIT 20"
        ).bind(&like).fetch_all(&state.db).await.unwrap_or_default();
        
        for (uuid, name, intro, cnt) in rows {
            let short = uuid.replace("-", "")[..6].to_string();
            items.push(serde_json::json!({"uuid": uuid, "tag": format_group_tag(&name, &short), "intro": intro, "members": cnt}));
        }
    } else {
        if let Some((uuid, name, short)) = resolve_group(state, args[0]).await {
            let row: Option<(String, i64, i64)> = sqlx::query_as("SELECT intro, searchable, (SELECT COUNT(*) FROM group_members WHERE group_uuid = groups.uuid) FROM groups WHERE uuid = ?")
                .bind(&uuid).fetch_one(&state.db).await.ok();
            if let Some((intro, searchable, cnt)) = row {
                if searchable == 1 || args[0].contains('-') { // 精确 UUID 不受 searchable 限制
                    items.push(serde_json::json!({"uuid": uuid, "tag": format_group_tag(&name, &short), "intro": intro, "members": cnt}));
                }
            }
        }
    }

    if items.is_empty() {
        let _ = tx.send(Outgoing::ok("没有找到匹配的群聊"));
    } else {
        let _ = tx.send(Outgoing::data("group_search", serde_json::json!(items)));
        let _ = tx.send(Outgoing::ok(format!("找到 {} 个群聊", items.len())));
    }
}

async fn cmd_in(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.len() < 2 { let _ = tx.send(Outgoing::err("用法: groups in <list [群]|agree [NAME] [群]|unagree [NAME] [群]>")); return; }
    let me = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };
    
    match args[0].to_lowercase().as_str() {
        "list" => {
            let (uuid, name, _) = match resolve_group(state, args[1]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };
            let role = get_member_role(state, &uuid, &me).await;
            if role.as_deref() != Some("owner") && role.as_deref() != Some("admin") { let _ = tx.send(Outgoing::err("需要管理员权限")); return; }
            
            let rows: Vec<(String, String)> = sqlx::query_as("SELECT user, created_at FROM group_join_requests WHERE group_uuid = ? ORDER BY created_at")
                .bind(&uuid).fetch_all(&state.db).await.unwrap_or_default();
            if rows.is_empty() { let _ = tx.send(Outgoing::ok("没有待批准的入群申请")); return; }
            
            let mut msg = format!("群 {} 待批准 ({} 人):\n", name, rows.len());
            for (u, t) in rows { msg.push_str(&format!("  {} ({})\n", u, t)); }
            let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
        }
        "agree" | "unagree" => {
            if args.len() < 3 { let _ = tx.send(Outgoing::err("用法: groups in agree/unagree [NAME] [群]")); return; }
            let target = match resolve_user_name(&state.db, args[1]).await { Some(n) => n, None => { let _ = tx.send(Outgoing::err("用户不存在")); return; } };
            let (uuid, name, _) = match resolve_group(state, args[2]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };
            
            let role = get_member_role(state, &uuid, &me).await;
            if role.as_deref() != Some("owner") && role.as_deref() != Some("admin") { let _ = tx.send(Outgoing::err("需要管理员权限")); return; }
            
            let req: Option<(String,)> = sqlx::query_as("SELECT user FROM group_join_requests WHERE group_uuid = ? AND user = ?")
                .bind(&uuid).bind(&target).fetch_optional(&state.db).await.ok().flatten();
            if req.is_none() { let _ = tx.send(Outgoing::err("该用户没有申请加入")); return; }
            
            if args[0].eq_ignore_ascii_case("agree") {
                let now = chrono::Utc::now().to_rfc3339();
                let _ = sqlx::query("INSERT INTO group_members (group_uuid, user, role, joined_at) VALUES (?, ?, 'member', ?)")
                    .bind(&uuid).bind(&target).bind(&now).execute(&state.db).await;
                let _ = tx.send(Outgoing::ok(format!("已同意 {} 加入群聊 {}", target, name)));
                broadcast_to_group(state, &uuid, "Notify:System", &format!("{} 被管理员批准加入群聊", target)).await;
            } else {
                let _ = tx.send(Outgoing::ok(format!("已拒绝 {} 加入群聊 {}", target, name)));
            }
            let _ = sqlx::query("DELETE FROM group_join_requests WHERE group_uuid = ? AND user = ?").bind(&uuid).bind(&target).execute(&state.db).await;
        }
        _ => { let _ = tx.send(Outgoing::err("未知 in 子命令")); }
    }
}

async fn cmd_history(state: &Arc<AppState>, session_id: Uuid, args: &[&str], tx: &mpsc::UnboundedSender<Outgoing>) {
    if args.len() < 2 { let _ = tx.send(Outgoing::err("用法: groups history [群] [行数(最大100)]")); return; }
    let me = match require_user(state, session_id).await { Ok(u) => u, Err(e) => { let _ = tx.send(Outgoing::err(e)); return; } };
    let (uuid, name, _) = match resolve_group(state, args[0]).await { Some(g) => g, None => { let _ = tx.send(Outgoing::err("群不存在")); return; } };
    
    if get_member_role(state, &uuid, &me).await.is_none() { let _ = tx.send(Outgoing::err("你不在该群中")); return; }
    let limit: usize = match args[1].parse() { Ok(n) if n > 0 && n <= 100 => n, _ => { let _ = tx.send(Outgoing::err("行数必须在 1-100 之间")); return; } };
    
    let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT id, from_user, content, created_at FROM messages WHERE kind='group' AND target=? ORDER BY id DESC LIMIT ?"
    ).bind(&uuid).bind(limit as i64).fetch_all(&state.db).await.unwrap_or_default();
    
    let mut items = Vec::new();
    for (id, from, content, at) in rows {
        items.push(serde_json::json!({"id": id, "from": from, "content": content, "at": at}));
    }
    items.reverse();
    let _ = tx.send(Outgoing::data("group_messages", serde_json::json!({"group": name, "messages": items})));
    let _ = tx.send(Outgoing::ok(format!("返回群 {} 最近 {} 条消息", name, items.len())));
}