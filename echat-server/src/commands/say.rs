use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{require_user, trim_messages};
use crate::protocol::Outgoing;
use crate::state::{AppState, MsgKind};

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

pub async fn say_text(state: Arc<AppState>, session_id: Uuid, text: &str) {
    let user = match require_user(&state, session_id).await {
        Ok(u) => u,
        Err(e) => { state.send_to(session_id, Outgoing::err(e)).await; return; }
    };

    // 存库：kind='global', target='global'，保留 50 条
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT INTO messages (kind, target, from_user, content, created_at) \
         VALUES ('global', 'global', ?, ?, ?)"
    ).bind(&user).bind(text).bind(&now).execute(&state.db).await;
    trim_messages(&state.db, "global", "global", 50).await;

    let content = format!("<{}> {}", user, text);
    state.broadcast_filtered(MsgKind::Global, "Global", &content).await;
}