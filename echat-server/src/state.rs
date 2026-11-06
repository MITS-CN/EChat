use dashmap::DashMap;
use sqlx::SqlitePool;
use std::sync::atomic::{AtomicBool, AtomicU8};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::Config;
use crate::protocol::Outgoing;
use crate::session::{Session, ShowFilter};

pub type LogControl = Box<dyn Fn(&str) + Send + Sync>;

/// 消息分类，用于 show 过滤
#[derive(Debug, Clone)]
pub enum MsgKind {
    Global,
    Friend(String),   // from_user
    FriendNotice,     // 好友申请 / 同意 / 上线提醒
    Group(String),    // 三期 B：群名
    Force,            // 系统强制（踢出、管理员操作等），永不过滤
}

pub struct AppState {
    pub db: SqlitePool,
    pub config: Config,
    pub op_password_hash: RwLock<Option<String>>,
    pub access_password_hash: RwLock<Option<String>>,
    pub federation_password: RwLock<Option<String>>,
    pub tls_enabled: Arc<AtomicBool>,
    pub log_level: Arc<AtomicU8>,
    pub log_control: LogControl,
    pub sessions: DashMap<Uuid, Arc<tokio::sync::Mutex<Session>>>,
    pub channels: DashMap<Uuid, mpsc::UnboundedSender<Outgoing>>,
    pub close_tokens: DashMap<Uuid, CancellationToken>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: SqlitePool,
        config: Config,
        op_password_hash: Option<String>,
        access_password_hash: Option<String>,
        federation_password: Option<String>,
        tls_enabled: Arc<AtomicBool>,
        log_level: Arc<AtomicU8>,
        log_control: LogControl,
    ) -> Self {
        Self {
            db, config,
            op_password_hash: RwLock::new(op_password_hash),
            access_password_hash: RwLock::new(access_password_hash),
            federation_password: RwLock::new(federation_password),
            tls_enabled, log_level, log_control,
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

    /// 检查 filter 决定是否推送某类消息
    async fn check_filter(&self, session_id: Uuid, kind: &MsgKind) -> bool {
        if matches!(kind, MsgKind::Force) {
            return true;
        }
        let session = match self.sessions.get(&session_id) {
            Some(s) => s.clone(),
            None => return true,
        };
        let g = session.lock().await;
        match &g.show_filter {
            ShowFilter::All => true,
            ShowFilter::Lan => matches!(kind, MsgKind::Global | MsgKind::FriendNotice),
            ShowFilter::Friends(name) => {
                if let MsgKind::Friend(from) = kind {
                    from.to_lowercase() == name.to_lowercase()
                } else {
                    false
                }
            }
            ShowFilter::Groups(gname) => {
                if let MsgKind::Group(g) = kind {
                    g.to_lowercase() == gname.to_lowercase()
                } else {
                    false
                }
            }
        }
    }

    /// 广播并按 filter 过滤
    pub async fn broadcast_filtered(&self, kind: MsgKind, source: &str, content: &str) {
        let msg = Outgoing::event(source, content);
        let sids: Vec<Uuid> = self.channels.iter().map(|e| *e.key()).collect();
        for sid in sids {
            let allowed = self.check_filter(sid, &kind).await;
            if allowed {
                if let Some(tx) = self.channels.get(&sid) {
                    let _ = tx.send(msg.clone());
                }
            }
        }
    }

    /// 单点推送并按 filter 过滤
    pub async fn send_event_filtered(
        &self,
        session_id: Uuid,
        kind: MsgKind,
        source: &str,
        content: &str,
    ) {
        let allowed = self.check_filter(session_id, &kind).await;
        if allowed {
            self.send_to(session_id, Outgoing::event(source, content)).await;
        }
    }

    pub async fn kick(&self, session_id: Uuid, reason: &str) {
        self.send_to(session_id, Outgoing::close(format!("被服务器踢出: {}", reason))).await;
        if let Some(ct) = self.close_tokens.get(&session_id) {
            ct.cancel();
        }
    }

    pub async fn find_session_by_user(
        &self,
        username: &str,
    ) -> Option<(Uuid, Arc<tokio::sync::Mutex<Session>>)> {
        let snapshot: Vec<(Uuid, Arc<tokio::sync::Mutex<Session>>)> = self
            .sessions.iter().map(|e| (*e.key(), e.value().clone())).collect();
        for (sid, sess) in snapshot {
            let guard = sess.lock().await;
            if guard.user.as_deref()
                .map(|u| u.eq_ignore_ascii_case(username))
                .unwrap_or(false)
            {
                drop(guard);
                return Some((sid, sess));
            }
        }
        None
    }

    pub async fn drop_all_federation(&self) {
        let snapshot: Vec<(Uuid, Arc<tokio::sync::Mutex<Session>>)> = self
            .sessions.iter().map(|e| (*e.key(), e.value().clone())).collect();
        for (sid, sess) in snapshot {
            let is_fed = { let g = sess.lock().await; g.is_federation };
            if is_fed {
                self.kick(sid, "联邦密码已重设").await;
            }
        }
    }
}