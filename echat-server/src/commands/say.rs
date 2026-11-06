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
    let text = args.join(" ");
    if text.trim().is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat say <消息>"));
        return;
    }
    say_text(state.clone(), session_id, &text).await;
}

/// 直接广播文本（也会用于非斜杠开头的裸聊天）
pub async fn say_text(state: Arc<AppState>, session_id: Uuid, text: &str) {
    let user = match require_user(&state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            state.send_to(session_id, Outgoing::err(e)).await;
            return;
        }
    };
    let content = format!("<{}> {}", user, text);
    state.broadcast("Global", &content).await;
}