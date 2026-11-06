use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{normalize_friend_target, require_user, resolve_user_name, trim_messages};
use crate::protocol::Outgoing;
use crate::state::{AppState, MsgKind};

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat tell [名称] [内容]"));
        return;
    }
    let target_input = args[0];
    let content = args[1..].join(" ");
    if content.trim().is_empty() {
        let _ = tx.send(Outgoing::err("内容不能为空"));
        return;
    }
    if content.chars().count() > 10000 {
        let _ = tx.send(Outgoing::err("内容超过 10000 字符上限"));
        return;
    }

    let from = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };

    let target = match resolve_user_name(&state.db, target_input).await {
        Some(n) => n,
        None => {
            let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", target_input)));
            return;
        }
    };

    if target.eq_ignore_ascii_case(&from) {
        let _ = tx.send(Outgoing::err("不能给自己发私聊"));
        return;
    }

    // 存库
    let now = chrono::Utc::now().to_rfc3339();
    let pair = normalize_friend_target(&from, &target);
    let _ = sqlx::query(
        "INSERT INTO messages (kind, target, from_user, content, created_at) \
         VALUES ('friend', ?, ?, ?, ?)"
    )
    .bind(&pair).bind(&from).bind(&content).bind(&now)
    .execute(&state.db).await;
    trim_messages(&state.db, "friend", &pair, 500).await;

    // 推送（离线也存，对方上线可查历史）
    if let Some((target_sid, _)) = state.find_session_by_user(&target).await {
        state.send_event_filtered(
            target_sid,
            MsgKind::Friend(from.clone()),
            &format!("Private:<{}>", from),
            &content,
        ).await;
        let _ = tx.send(Outgoing::ok(format!("已发送给 {}", target)));
    } else {
        let _ = tx.send(Outgoing::ok(format!("{} 不在线，消息已保存，对方上线后可查看", target)));
    }
}