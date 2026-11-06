use dashmap::DashMap;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::Config;
use crate::protocol::Outgoing;
use crate::session::Session;

pub struct AppState {
    pub db: SqlitePool,
    pub config: Config,
    pub op_password_hash: RwLock<String>,
    pub sessions: DashMap<Uuid, Arc<tokio::sync::Mutex<Session>>>,
    pub channels: DashMap<Uuid, mpsc::UnboundedSender<Outgoing>>,
    pub close_tokens: DashMap<Uuid, CancellationToken>,
}

impl AppState {
    pub fn new(db: SqlitePool, config: Config, op_password_hash: String) -> Self {
        Self {
            db,
            config,
            op_password_hash: RwLock::new(op_password_hash),
            sessions: DashMap::new(),
            channels: DashMap::new(),
            close_tokens: DashMap::new(),
        }
    }

    pub async fn send_to(&self, session_id: Uuid, msg: Outgoing) {
        if let Some(tx) = self.channels.get(&session_id) {
            let _ = tx.send(msg);
        }
    }

    pub async fn broadcast(&self, source: &str, content: &str) {
        let msg = Outgoing::event(source, content);
        for entry in self.channels.iter() {
            let _ = entry.value().send(msg.clone());
        }
    }

    pub async fn kick(&self, session_id: Uuid, reason: &str) {
        self.send_to(session_id, Outgoing::close(format!("被服务器踢出: {}", reason)))
            .await;
        if let Some(ct) = self.close_tokens.get(&session_id) {
            ct.cancel();
        }
    }

    pub async fn find_session_by_user(
        &self,
        username: &str,
    ) -> Option<(Uuid, Arc<tokio::sync::Mutex<Session>>)> {
        let snapshot: Vec<(Uuid, Arc<tokio::sync::Mutex<Session>>)> = self
            .sessions
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect();

        for (sid, sess) in snapshot {
            let guard = sess.lock().await;
            if guard.user.as_deref() == Some(username) {
                drop(guard);
                return Some((sid, sess));
            }
        }
        None
    }
}