pub mod console;
pub mod friend_cmd;
pub mod op_cmd;
pub mod say;
pub mod server_cmd;
pub mod show_cmd;
pub mod tell;
pub mod user_cmd;

use std::sync::Arc;
use sqlx::SqlitePool;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::protocol::Outgoing;
use crate::session::Session;
use crate::state::AppState;

pub async fn require_user(state: &Arc<AppState>, session_id: Uuid) -> Result<String, String> {
    let session = state.sessions.get(&session_id)
        .ok_or_else(|| "会话不存在".to_string())?.clone();
    let guard = session.lock().await;
    guard.user.clone()
        .ok_or_else(|| "请先登录（/Echat user login [名称] [密码]）".to_string())
}

pub async fn get_session(
    state: &AppState,
    session_id: Uuid,
) -> Result<Arc<tokio::sync::Mutex<Session>>, String> {
    state.sessions.get(&session_id).map(|r| r.clone())
        .ok_or_else(|| "会话不存在".to_string())
}

async fn is_logged_in(state: &AppState, session_id: Uuid) -> bool {
    if let Some(s) = state.sessions.get(&session_id) {
        let sess = s.clone();
        drop(s);
        let g = sess.lock().await;
        g.user.is_some()
    } else {
        false
    }
}

/// 用户名大小写不敏感：从 users 表找出原样 name
pub async fn resolve_user_name(db: &SqlitePool, input: &str) -> Option<String> {
    sqlx::query_as::<_, (String,)>("SELECT name FROM users WHERE LOWER(name) = LOWER(?)")
        .bind(input)
        .fetch_optional(db).await.ok().flatten().map(|(n,)| n)
}

/// 好友私聊 target 归一化：小写排序后拼接，原始名存入
pub fn normalize_friend_target(a: &str, b: &str) -> String {
    if a.to_lowercase() <= b.to_lowercase() {
        format!("{}|{}", a, b)
    } else {
        format!("{}|{}", b, a)
    }
}

/// 只保留 target 下最近 keep 条消息
pub async fn trim_messages(db: &SqlitePool, kind: &str, target: &str, keep: i64) {
    let _ = sqlx::query(
        "DELETE FROM messages WHERE kind = ? AND target = ? AND id NOT IN ( \
            SELECT id FROM messages WHERE kind = ? AND target = ? ORDER BY id DESC LIMIT ?)"
    )
    .bind(kind).bind(target).bind(kind).bind(target).bind(keep)
    .execute(db).await;
}

/// 用户上线时，检查是否有人正在观察此用户，并对在线 watcher 推送通知
pub async fn trigger_online_notify(state: &Arc<AppState>, user: &str) {
    // 1. 该用户是否愿意被提醒（take_on）
    let take_on: Option<i64> = sqlx::query_scalar(
        "SELECT take_on FROM user_prefs WHERE name = ?"
    ).bind(user).fetch_optional(&state.db).await.ok().flatten();

    if take_on.unwrap_or(1) == 0 {
        return;
    }

    // 2. 谁开启了对 user 的观察
    let watchers: Vec<(String,)> = sqlx::query_as(
        "SELECT watcher FROM friend_reminders WHERE target = ?"
    ).bind(user).fetch_all(&state.db).await.unwrap_or_default();

    for (watcher,) in watchers {
        if let Some((sid, _)) = state.find_session_by_user(&watcher).await {
            state.send_event_filtered(
                sid,
                crate::state::MsgKind::FriendNotice,
                "Notify:Online",
                &format!("你的好友 {} 上线了", user),
            ).await;
        }
    }
}

pub async fn dispatch(
    state: Arc<AppState>,
    session_id: Uuid,
    line: &str,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }

    let logged_in = is_logged_in(&state, session_id).await;

    // 非斜杠 → 全局 say（需登录）
    if !trimmed.starts_with('/') {
        if !logged_in {
            let _ = tx.send(Outgoing::err("请先登录：/Echat user login [名称] [密码]"));
            return;
        }
        say::say_text(state, session_id, trimmed).await;
        return;
    }

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.is_empty() {
        return;
    }
    if !tokens[0].eq_ignore_ascii_case("/echat") {
        let _ = tx.send(Outgoing::err(format!("未知命令: {}", tokens[0])));
        return;
    }
    if tokens.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat <server|user|say|tell|online|show|OP> ..."));
        return;
    }

    fn print_help(tx: &mpsc::UnboundedSender<Outgoing>) {
        let _ = tx.send(Outgoing::ok(
            "Echat 客户端命令（用法前缀 /Echat）\n\
             \n\
             [登录]\n\
               /Echat user reg name [名字] Echat\n\
               /Echat user reg password [密码] [重复密码] Echat\n\
               /Echat user login [名称] [密码]\n\
               /Echat user logout\n\
             \n\
             [好友]\n\
               /Echat online friends list\n\
               /Echat online friends search [名字]\n\
               /Echat online friends [名字] add|del|Y|N\n\
             \n\
             [提醒]\n\
               /Echat online take on|off\n\
               /Echat online friends-go-online-Reminder on|off [名字]\n\
             \n\
             [消息]\n\
               /Echat say [内容]\n\
               /Echat tell [名字] [内容]\n\
               /Echat show ALL|LAN|friends [名字]"
        ));
    }

    if trimmed.eq_ignore_ascii_case("help") {
        print_help(tx);
        return;
    }

    // 未登录时：放行 help 和全部 user 子命令（user_cmd 自己会判断状态并给出精确提示）
    if !logged_in {
        let allowed = tokens.len() >= 2
            && (tokens[1].eq_ignore_ascii_case("help")
                || tokens[1].eq_ignore_ascii_case("user"));
        if !allowed {
            let _ = tx.send(Outgoing::err(
                "请先登录。\n\
                 新用户注册（两阶段）：\n\
                   1) /Echat user reg name [名字] Echat\n\
                   2) /Echat user reg password [密码] [重复密码] Echat\n\
                 老用户登录：/Echat user login [名称] [密码]\n\
                 查看全部命令：/Echat help"
            ));
            return;
        }
    }

    match tokens[1].to_lowercase().as_str() {
        "help" => print_help(tx),
        "server" => server_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        "user" => user_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        "say" => say::handle(&state, session_id, &tokens[2..], tx).await,
        "tell" => tell::handle(&state, session_id, &tokens[2..], tx).await,
        "online" | "onile" => friend_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        "show" => show_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        "op" => op_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        other => {
            let _ = tx.send(Outgoing::err(format!("未知命令类别: {}", other)));
        }
    }
}