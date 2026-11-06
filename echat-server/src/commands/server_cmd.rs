use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use uuid::Uuid;

use super::{get_session, require_user};
use crate::protocol::Outgoing;
use crate::session::ConnectedServer;
use crate::state::AppState;

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat server <list|add|test|del|contact|contact-all|uncontact|uncontact-all|contact-list>",
        ));
        return;
    }
    match args[0].to_lowercase().as_str() {
        "list" => list(state, session_id, tx).await,
        "add" => add(state, session_id, &args[1..], tx).await,
        "test" => test(state, session_id, &args[1..], tx).await,
        "del" => del(state, session_id, &args[1..], tx).await,
        "contact" => contact(state, session_id, &args[1..], tx).await,
        "contact-all" => contact_all(state, session_id, tx).await,
        "uncontact" => uncontact(state, session_id, &args[1..], tx).await,
        "uncontact-all" => uncontact_all(state, session_id, tx).await,
        "contact-list" => contact_list(state, session_id, tx).await,
        other => {
            let _ = tx.send(Outgoing::err(format!("未知 server 子命令: {}", other)));
        }
    }
}

async fn list(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT name, ip FROM servers WHERE owner = ? ORDER BY name",
    )
    .bind(&user)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(rows) => {
            if rows.is_empty() {
                let _ = tx.send(Outgoing::ok("地址簿为空"));
                return;
            }
            let mut msg = format!("地址簿 ({} 个):\n", rows.len());
            let mut items = Vec::new();
            for (n, ip) in rows {
                msg.push_str(&format!("  {}  ->  {}\n", n, ip));
                items.push(serde_json::json!({"name": n, "ip": ip}));
            }
            let _ = tx.send(Outgoing::data("server_list", serde_json::json!(items)));
            let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
        }
        Err(e) => {
            let _ = tx.send(Outgoing::err(format!("查询失败: {}", e)));
        }
    }
}

async fn add(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat server add [IP] [NAME]"));
        return;
    }
    let (ip, name) = (args[0], args[1]);
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };

    let exists: Option<(String,)> =
        sqlx::query_as("SELECT name FROM servers WHERE owner = ? AND name = ?")
            .bind(&user)
            .bind(name)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();
    if exists.is_some() {
        let _ = tx.send(Outgoing::err(format!("服务器名 {} 已存在", name)));
        return;
    }

    let now = chrono::Utc::now().to_rfc3339();
    match sqlx::query("INSERT INTO servers (owner, name, ip, created_at) VALUES (?, ?, ?, ?)")
        .bind(&user)
        .bind(name)
        .bind(ip)
        .bind(&now)
        .execute(&state.db)
        .await
    {
        Ok(_) => {
            let _ = tx.send(Outgoing::ok(format!("已添加服务器 {} -> {}", name, ip)));
        }
        Err(e) => {
            let _ = tx.send(Outgoing::err(format!("添加失败: {}", e)));
        }
    }
}

async fn test(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat server test [NAME/IP]"));
        return;
    }
    let target = args[0];
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let (name, ip) = match resolve_server(state, &user, target).await {
        Ok(v) => v,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };

    let addr = parse_host_port(&ip);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr),
    )
    .await;

    match result {
        Ok(Ok(_)) => {
            let _ = tx.send(Outgoing::ok(format!(
                "服务器 {} ({}) 可达，加密状态: 待二期实现",
                name, ip
            )));
        }
        Ok(Err(e)) => {
            let _ = tx.send(Outgoing::err(format!("服务器 {} 不可达: {}", name, e)));
        }
        Err(_) => {
            let _ = tx.send(Outgoing::err(format!("服务器 {} 连接超时", name)));
        }
    }
}

async fn del(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat server del [NAME/IP]"));
        return;
    }
    let target = args[0];
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let res = sqlx::query("DELETE FROM servers WHERE owner = ? AND (name = ? OR ip = ?)")
        .bind(&user)
        .bind(target)
        .bind(target)
        .execute(&state.db)
        .await;
    match res {
        Ok(r) if r.rows_affected() > 0 => {
            let _ = tx.send(Outgoing::ok(format!("已删除服务器 {}", target)));
        }
        Ok(_) => {
            let _ = tx.send(Outgoing::err(format!("未找到服务器 {}", target)));
        }
        Err(e) => {
            let _ = tx.send(Outgoing::err(format!("删除失败: {}", e)));
        }
    }
}

async fn contact(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat server contact [NAME/IP]"));
        return;
    }
    let target = args[0];
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let (name, ip) = match resolve_server(state, &user, target).await {
        Ok(v) => v,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };

    // 已连接？
    {
        let session = match get_session(state, session_id).await {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Outgoing::err(e));
                return;
            }
        };
        let guard = session.lock().await;
        if guard.connected.contains_key(&name) {
            let _ = tx.send(Outgoing::err(format!("已连接到 {}", name)));
            return;
        }
    }

    let url = if ip.starts_with("ws://") || ip.starts_with("wss://") {
        ip.clone()
    } else {
        format!("ws://{}/echat", ip)
    };

    match tokio::time::timeout(Duration::from_secs(5), connect_async(&url)).await {
        Ok(Ok((ws_stream, _))) => {
            let (_, mut read) = ws_stream.split();
            let cancel = tokio_util::sync::CancellationToken::new();
            let cancel_bg = cancel.clone();
            let sid = session_id;
            let state_bg = state.clone();
            let name_bg = name.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_bg.cancelled() => break,
                        msg = read.next() => {
                            match msg {
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                                    let content = extract_event_content(&t)
                                        .unwrap_or_else(|| t.to_string());
                                    state_bg
                                        .send_to(sid, Outgoing::event(&name_bg, content))
                                        .await;
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))
                                | None
                                | Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                    }
                }
            });

            let session = state.sessions.get(&session_id).unwrap().clone();
            let mut guard = session.lock().await;
            guard.connected.insert(
                name.clone(),
                ConnectedServer { name: name.clone(), ip: ip.clone(), cancel },
            );
            drop(guard);

            let _ = tx.send(Outgoing::ok(format!("已连接到服务器 {} ({})", name, ip)));
        }
        Ok(Err(e)) => {
            let _ = tx.send(Outgoing::err(format!("连接 {} 失败: {}", name, e)));
        }
        Err(_) => {
            let _ = tx.send(Outgoing::err(format!("连接 {} 超时", name)));
        }
    }
}

async fn contact_all(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let user = match require_user(state, session_id).await {
        Ok(u) => u,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT name, ip FROM servers WHERE owner = ? ORDER BY name",
    )
    .bind(&user)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    if rows.is_empty() {
        let _ = tx.send(Outgoing::err("地址簿为空"));
        return;
    }

    let mut ok_count = 0;
    let mut fail_count = 0;
    for (name, ip) in rows {
        let already = {
            let session = state.sessions.get(&session_id).unwrap().clone();
            let guard = session.lock().await;
            guard.connected.contains_key(&name)
        };
        if already {
            ok_count += 1;
            continue;
        }
        // 逐个连接，保持简单
        let ok = try_contact_one(state, session_id, &name, &ip).await;
        if ok {
            ok_count += 1;
        } else {
            fail_count += 1;
        }
    }
    let _ = tx.send(Outgoing::ok(format!(
        "contact-all 完成：成功 {} / 失败 {}",
        ok_count, fail_count
    )));
}

async fn try_contact_one(
    state: &Arc<AppState>,
    session_id: Uuid,
    name: &str,
    ip: &str,
) -> bool {
    let url = if ip.starts_with("ws://") || ip.starts_with("wss://") {
        ip.to_string()
    } else {
        format!("ws://{}/echat", ip)
    };
    match tokio::time::timeout(Duration::from_secs(3), connect_async(&url)).await {
        Ok(Ok((ws_stream, _))) => {
            let (_, mut read) = ws_stream.split();
            let cancel = tokio_util::sync::CancellationToken::new();
            let cancel_bg = cancel.clone();
            let sid = session_id;
            let state_bg = state.clone();
            let name_bg = name.to_string();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_bg.cancelled() => break,
                        msg = read.next() => {
                            match msg {
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                                    let content = extract_event_content(&t)
                                        .unwrap_or_else(|| t.to_string());
                                    state_bg
                                        .send_to(sid, Outgoing::event(&name_bg, content))
                                        .await;
                                }
                                _ => break,
                            }
                        }
                    }
                }
            });
            if let Some(sess) = state.sessions.get(&session_id) {
                let sess = sess.clone();
                let mut guard = sess.lock().await;
                guard.connected.insert(
                    name.to_string(),
                    ConnectedServer { name: name.to_string(), ip: ip.to_string(), cancel },
                );
            }
            true
        }
        _ => false,
    }
}

async fn uncontact(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat server uncontact [NAME/IP]"));
        return;
    }
    let target = args[0];
    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let mut guard = session.lock().await;
    // 匹配名字或 ip
    let key = guard
        .connected
        .iter()
        .find(|(k, v)| k.as_str() == target || v.ip == target)
        .map(|(k, _)| k.clone());
    match key {
        Some(k) => {
            if let Some(c) = guard.connected.remove(&k) {
                c.cancel.cancel();
            }
            drop(guard);
            let _ = tx.send(Outgoing::ok(format!("已断开服务器 {}", target)));
        }
        None => {
            drop(guard);
            let _ = tx.send(Outgoing::err(format!("未连接服务器 {}", target)));
        }
    }
}

async fn uncontact_all(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let mut guard = session.lock().await;
    let n = guard.connected.len();
    for (_, c) in guard.connected.drain() {
        c.cancel.cancel();
    }
    drop(guard);
    let _ = tx.send(Outgoing::ok(format!("已断开全部 {} 个连接", n)));
}

async fn contact_list(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let guard = session.lock().await;
    if guard.connected.is_empty() {
        let _ = tx.send(Outgoing::ok("当前没有已连接的服务器"));
        return;
    }
    let mut msg = format!("已连接服务器 ({} 个):\n", guard.connected.len());
    for (name, c) in guard.connected.iter() {
        msg.push_str(&format!("  {}  ->  {}\n", name, c.ip));
    }
    let _ = tx.send(Outgoing::ok(msg.trim_end().to_string()));
}

/// 解析 target 是名字还是 IP，返回 (name, ip)
async fn resolve_server(
    state: &AppState,
    user: &str,
    target: &str,
) -> Result<(String, String), String> {
    // 按名字
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT name, ip FROM servers WHERE owner = ? AND name = ?")
            .bind(user)
            .bind(target)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| e.to_string())?;
    if let Some((n, ip)) = row {
        return Ok((n, ip));
    }
    // 按 IP
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT name, ip FROM servers WHERE owner = ? AND ip = ?")
            .bind(user)
            .bind(target)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| e.to_string())?;
    if let Some((n, ip)) = row {
        return Ok((n, ip));
    }
    // 直接给 IP
    if target.contains(':') || target.contains('.') {
        return Ok((target.to_string(), target.to_string()));
    }
    Err(format!("找不到服务器 {}", target))
}

fn parse_host_port(ip: &str) -> String {
    // 去重 ws/wss 前缀
    let s = ip
        .trim_start_matches("ws://")
        .trim_start_matches("wss://");
    // 去掉路径
    s.split('/').next().unwrap_or(s).to_string()
}

fn extract_event_content(raw: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(raw).ok()?;
    if v.get("type")?.as_str()? == "event" {
        Some(v.get("content")?.as_str()?.to_string())
    } else {
        None
    }
}