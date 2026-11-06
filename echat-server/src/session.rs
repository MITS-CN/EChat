use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::commands;
use crate::protocol::Outgoing;
use crate::state::AppState;

pub struct Session {
    pub id: Uuid,
    pub user: Option<String>,
    pub is_op: bool,
    pub reg_state: RegState,
    pub connected: HashMap<String, ConnectedServer>,
}

pub enum RegState {
    Idle,
    PendingName { name: String, target: String },
}

pub struct ConnectedServer {
    pub ip: String,
    pub cancel: CancellationToken,
}

impl Session {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            user: None,
            is_op: false,
            reg_state: RegState::Idle,
            connected: HashMap::new(),
        }
    }
}

pub async fn handle_ws(socket: WebSocket, state: Arc<AppState>) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Outgoing>();

    let session = Arc::new(tokio::sync::Mutex::new(Session::new()));
    let session_id = session.lock().await.id;
    let close_token = CancellationToken::new();

    state.sessions.insert(session_id, session.clone());
    state.channels.insert(session_id, tx.clone());
    state.close_tokens.insert(session_id, close_token.clone());

    tracing::info!("新连接 {} 已建立", session_id);

    let ct_out = close_token.clone();
    let out_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = ct_out.cancelled() => {
                    let _ = ws_tx.send(Message::Close(None)).await;
                    break;
                }
                msg = rx.recv() => {
                    match msg {
                        Some(Outgoing::Close { reason }) => {
                            let _ = ws_tx.send(Message::Text(
                                Outgoing::err(format!("连接关闭: {}", reason)).to_json()
                            )).await;
                            let _ = ws_tx.send(Message::Close(None)).await;
                            break;
                        }
                        Some(m) => {
                            if ws_tx.send(Message::Text(m.to_json())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    let ct_in = close_token.clone();
    loop {
        tokio::select! {
            _ = ct_in.cancelled() => break,
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        commands::dispatch(state.clone(), session_id, &text, &tx).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }

    cleanup(&state, session_id).await;
    out_task.abort();
    tracing::info!("连接 {} 已断开", session_id);
}

async fn cleanup(state: &Arc<AppState>, session_id: Uuid) {
    if let Some(sess) = state.sessions.get(&session_id) {
        let guard = sess.lock().await;
        for (_, c) in guard.connected.iter() {
            c.cancel.cancel();
        }
    }
    state.sessions.remove(&session_id);
    state.channels.remove(&session_id);
    state.close_tokens.remove(&session_id);
}