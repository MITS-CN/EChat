pub mod console;
pub mod op_cmd;
pub mod say;
pub mod server_cmd;
pub mod user_cmd;

use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::protocol::Outgoing;
use crate::session::Session;
use crate::state::AppState;

/// 检查当前会话是否已完成注册，返回用户名
pub async fn require_user(
    state: &Arc<AppState>,
    session_id: Uuid,
) -> Result<String, String> {
    let session = state
        .sessions
        .get(&session_id)
        .ok_or_else(|| "会话不存在".to_string())?
        .clone();
    let guard = session.lock().await;
    guard
        .user
        .clone()
        .ok_or_else(|| "请先注册（/Echat user reg name ...）".to_string())
}

pub async fn get_session(
    state: &AppState,
    session_id: Uuid,
) -> Result<Arc<tokio::sync::Mutex<Session>>, String> {
    state
        .sessions
        .get(&session_id)
        .map(|r| r.clone())
        .ok_or_else(|| "会话不存在".to_string())
}

/// 顶层命令分发
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

    // 不以 "/" 开头 → 当作 say
    if !trimmed.starts_with('/') {
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
        let _ = tx.send(Outgoing::err(
            "用法: /Echat <server|user|say|OP> ...".to_string(),
        ));
        return;
    }

    match tokens[1].to_lowercase().as_str() {
        "server" => server_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        "user" => user_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        "say" => say::handle(&state, session_id, &tokens[2..], tx).await,
        "op" => op_cmd::handle(&state, session_id, &tokens[2..], tx).await,
        other => {
            let _ = tx.send(Outgoing::err(format!("未知命令类别: {}", other)));
        }
    }
}