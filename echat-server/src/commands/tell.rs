use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::require_user;
use crate::protocol::Outgoing;
use crate::state::AppState;

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
    let target = args[0];
    let content = args[1..].join(" ");
    if content.trim().is_empty() {
        let _ = tx.send(Outgoing::err("内容不能为空"));
        return;
    }

    let from = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };

    if target == from {
        let _ = tx.send(Outgoing::err("不能给自己发私聊"));
        return;
    }

    match state.find_session_by_user(target).await {
        Some((sid, _)) => {
            state
                .send_to(
                    sid,
                    Outgoing::event(format!("Private:<{}>", from), content.clone()),
                )
                .await;
            let _ = tx.send(Outgoing::ok(format!("已发送给 {}", target)));
        }
        None => {
            let _ = tx.send(Outgoing::err(format!("用户 {} 不在线", target)));
        }
    }
}