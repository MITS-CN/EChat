use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use argon2::{Argon2, PasswordHash, PasswordVerifier};

use crate::commands;
use crate::protocol::Outgoing;
use crate::state::AppState;

#[derive(Debug, Clone, PartialEq)]
pub enum AuthState {
    Pending,
    Authed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowFilter {
    All,
    Lan,
    Friends(String),
    Groups(String), // 三期 B
}

pub struct Session {
    pub id: Uuid,
    pub user: Option<String>,
    pub is_op: bool,
    pub is_federation: bool,
    pub reg_state: RegState,
    pub connected: HashMap<String, ConnectedServer>,
    pub auth_state: AuthState,
    pub show_filter: ShowFilter,
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
    pub fn new(auth_state: AuthState) -> Self {
        Self {
            id: Uuid::new_v4(),
            user: None,
            is_op: false,
            is_federation: false,
            reg_state: RegState::Idle,
            connected: HashMap::new(),
            auth_state,
            show_filter: ShowFilter::All,
        }
    }
}

pub async fn handle_ws(socket: WebSocket, state: Arc<AppState>) {
    let needs_auth = state.access_password_hash.read().await.is_some();
    let initial_auth = if needs_auth { AuthState::Pending } else { AuthState::Authed };

    let (mut ws_tx, mut ws_rx) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Outgoing>();

    let session = Arc::new(tokio::sync::Mutex::new(Session::new(initial_auth)));
    let session_id = session.lock().await.id;
    let close_token = CancellationToken::new();

    state.sessions.insert(session_id, session.clone());
    state.channels.insert(session_id, tx.clone());
    state.close_tokens.insert(session_id, close_token.clone());

    tracing::info!("新连接 {} 已建立 (needs_auth={})", session_id, needs_auth);

    if needs_auth {
        let _ = tx.send(Outgoing::auth_required());
    }

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
                            if ws_tx.send(Message::Text(m.to_json())).await.is_err() { break; }
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
                        let is_auth = serde_json::from_str::<serde_json::Value>(&text).ok()
                            .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(|s| s == "auth"))
                            .unwrap_or(false);
                        if is_auth {
                            handle_auth_frame(&state, session_id, &text, &tx, &session).await;
                        } else {
                            let authed = {
                                let s = session.lock().await;
                                s.auth_state == AuthState::Authed
                            };
                            if authed {
                                commands::dispatch(state.clone(), session_id, &text, &tx).await;
                            } else {
                                let _ = tx.send(Outgoing::err(
                                    "连接未认证，请发送 {\"type\":\"auth\",\"password\":\"...\"}",
                                ));
                            }
                        }
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

async fn handle_auth_frame(
    state: &Arc<AppState>,
    session_id: Uuid,
    text: &str,
    tx: &mpsc::UnboundedSender<Outgoing>,
    session: &Arc<tokio::sync::Mutex<Session>>,
) {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => { let _ = tx.send(Outgoing::err("认证帧格式错误")); return; }
    };
    let password = v.get("password").and_then(|p| p.as_str()).unwrap_or("");
    let is_fed_flag = v.get("federation").and_then(|x| x.as_bool()).unwrap_or(false);

    let current = { session.lock().await.auth_state.clone() };
    if current == AuthState::Authed {
        let _ = tx.send(Outgoing::ok("已认证"));
        return;
    }

    if is_fed_flag {
        let fed = state.federation_password.read().await.clone();
        match fed {
            None => { let _ = tx.send(Outgoing::err("服务端未启用联邦")); return; }
            Some(hash) => {
                let ok = PasswordHash::new(&hash)
                    .map(|p| Argon2::default().verify_password(password.as_bytes(), &p).is_ok())
                    .unwrap_or(false);
                if !ok {
                    let _ = tx.send(Outgoing::err("联邦密码错误"));
                    return;
                }
                let mut s = session.lock().await;
                s.auth_state = AuthState::Authed;
                s.is_federation = true;
                let _ = tx.send(Outgoing::ok("联邦认证成功"));
                return;
            }
        }
    }

    let stored = state.access_password_hash.read().await.clone();
    match stored {
        None => {
            session.lock().await.auth_state = AuthState::Authed;
            let _ = tx.send(Outgoing::ok("认证成功（服务器开放）"));
        }
        Some(hash) => {
            let parsed = match PasswordHash::new(&hash) {
                Ok(p) => p,
                Err(_) => { let _ = tx.send(Outgoing::err("服务端接入密码数据损坏")); return; }
            };
            if Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok() {
                session.lock().await.auth_state = AuthState::Authed;
                let _ = tx.send(Outgoing::ok("认证成功"));
            } else {
                let _ = tx.send(Outgoing::err("接入密码错误"));
                if let Some(ct) = state.close_tokens.get(&session_id) {
                    ct.cancel();
                }
            }
        }
    }
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