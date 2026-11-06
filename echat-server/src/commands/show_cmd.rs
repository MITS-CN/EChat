use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{require_user, resolve_user_name};
use crate::protocol::Outgoing;
use crate::session::ShowFilter;
use crate::state::AppState;

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat show <ALL|LAN|friends [NAME]|groups [群标识]>"));
        return;
    }
    let me = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };

    match args[0].to_lowercase().as_str() {
        "all" => set_filter(state, session_id, ShowFilter::All, tx).await,
        "lan" => set_filter(state, session_id, ShowFilter::Lan, tx).await,
        "friends" => {
            if args.len() < 2 {
                let _ = tx.send(Outgoing::err("用法: /Echat show friends [NAME]"));
                return;
            }
            let target = match resolve_user_name(&state.db, args[1]).await {
                Some(n) => n,
                None => {
                    let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", args[1])));
                    return;
                }
            };
            // 要求是好友
            let is_friend: Option<(String,)> = sqlx::query_as(
                "SELECT friend FROM friends WHERE user = ? AND friend = ?"
            ).bind(&me).bind(&target).fetch_optional(&state.db).await.ok().flatten();
            if is_friend.is_none() {
                let _ = tx.send(Outgoing::err(format!("{} 不是你的好友", target)));
                return;
            }
            set_filter(state, session_id, ShowFilter::Friends(target), tx).await
        }
        "groups" | "grups" => {
            if args.len() < 2 {
                let _ = tx.send(Outgoing::err("用法: /Echat show groups [群标识]"));
                return;
            }
            let input = args[1];
            // 内联解析群标识
            let uuid_res: Option<(String, String)> = if input.contains('#') {
                let parts: Vec<&str> = input.split('#').collect();
                if parts.len() != 2 { None } else {
                    sqlx::query_as("SELECT uuid, name FROM groups WHERE name = ? AND substr(replace(uuid, '-', ''), 1, 6) = ?")
                        .bind(parts[0]).bind(parts[1]).fetch_optional(&state.db).await.ok().flatten()
                }
            } else if input.len() == 36 && input.contains('-') {
                sqlx::query_as("SELECT uuid, name FROM groups WHERE uuid = ?")
                    .bind(input).fetch_optional(&state.db).await.ok().flatten()
            } else if input.len() == 6 && !input.contains('-') {
                sqlx::query_as("SELECT uuid, name FROM groups WHERE substr(replace(uuid, '-', ''), 1, 6) = ?")
                    .bind(input).fetch_optional(&state.db).await.ok().flatten()
            } else {
                sqlx::query_as("SELECT uuid, name FROM groups WHERE name = ? LIMIT 1")
                    .bind(input).fetch_optional(&state.db).await.ok().flatten()
            };

            let (uuid, name) = match uuid_res {
                Some(r) => r,
                None => { let _ = tx.send(Outgoing::err("找不到该群")); return; }
            };

            // 检查是否在群内
            let role: Option<(String,)> = sqlx::query_as(
                "SELECT role FROM group_members WHERE group_uuid = ? AND user = ?"
            ).bind(&uuid).bind(&me).fetch_optional(&state.db).await.ok().flatten();

            if role.is_none() {
                let _ = tx.send(Outgoing::err("你不在该群中，无法过滤"));
                return;
            }
            set_filter(state, session_id, ShowFilter::Groups(name), tx).await
        }
        other => {
            let _ = tx.send(Outgoing::err(format!("未知 show 选项: {}", other)));
        }
    }
}

async fn set_filter(
    state: &Arc<AppState>,
    session_id: Uuid,
    filter: ShowFilter,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let label = match &filter {
        ShowFilter::All => "全部".to_string(),
        ShowFilter::Lan => "仅 LAN（除好友和群聊外的所有消息）".to_string(),
        ShowFilter::Friends(n) => format!("仅好友 {}", n),
        ShowFilter::Groups(n) => format!("仅群聊 {}", n),
    };

    if let Some(s) = state.sessions.get(&session_id) {
        let sess = s.clone();
        drop(s);
        let mut g = sess.lock().await;
        g.show_filter = filter;
    }
    let _ = tx.send(Outgoing::ok(format!("消息显示：{}", label)));
}