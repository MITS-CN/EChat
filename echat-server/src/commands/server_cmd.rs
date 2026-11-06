use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
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
    let rows = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT name, ip, password FROM servers WHERE owner = ? ORDER BY name",
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
            for (n, ip, pwd) in rows {
                let lock = if pwd.is_some() { " 🔒" } else { "" };
                msg.push_str(&format!("  {}  ->  {}{}\n", n, ip, lock));
                items.push(serde_json::json!({
                    "name": n, "ip": ip, "has_password": pwd.is_some()
                }));
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
        let _ = tx.send(Outgoing::err(
            "用法: /Echat server add [IP] [NAME] [PASSWORD（可选）]",
        ));
        return;
    }
    let (ip, name) = (args[0], args[1]);
    let password = args.get(2).copied();

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
    match sqlx::query(
        "INSERT INTO servers (owner, name, ip, password, created_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&user)
    .bind(name)
    .bind(ip)
    .bind(password)
    .bind(&now)
    .execute(&state.db)
    .await
    {
        Ok(_) => {
            let lock = if password.is_some() { "（已设置接入密码）" } else { "" };
            let _ = tx.send(Outgoing::ok(format!(
                "已添加服务器 {} -> {}{}",
                name, ip, lock
            )));
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

    let url = format!("ws://{}/echat", ip);
    let hello = match tokio::time::timeout(Duration::from_secs(5), connect_async(&url)).await {
        Ok(Ok((ws, _))) => {
            let (_, mut read) = ws.split();
            match tokio::time::timeout(Duration::from_secs(3), read.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => Some(t),
                _ => None,
            }
        }
        _ => None,
    };

    match hello {
        Some(text) => {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if v.get("type").and_then(|x| x.as_str()) == Some("hello_ack") {
                    let encrypted = v
                        .get("encrypted")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false);
                    let server_name = v
                        .get("server_name")
                        .and_then(|x| x.as_str())
                        .unwrap_or("?");
                    let needs_auth = v
                        .get("needs_auth")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false);
                    let _ = tx.send(Outgoing::ok(format!(
                        "服务器 {} ({}) 可达\n  真实名称: {}\n  加密: {}\n  需要接入密码: {}",
                        name,
                        ip,
                        server_name,
                        if encrypted { "✅ 是 (TLS)" } else { "❌ 否" },
                        if needs_auth { "是" } else { "否" }
                    )));
                    return;
                }
            }
            let _ = tx.send(Outgoing::err("对方服务器响应格式异常"));
        }
        None => {
            let _ = tx.send(Outgoing::err(format!(
                "无法连接 {} ({})，请检查地址或对方是否启用了 wss（TLS-only）",
                name, ip
            )));
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
    let (name, ip, password) = match resolve_server_full(state, &user, target).await {
        Ok(v) => v,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };

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

    match do_contact(state, session_id, &name, &ip, password.as_deref()).await {
        Ok(()) => {
            let _ = tx.send(Outgoing::ok(format!("已连接到服务器 {} ({})", name, ip)));
        }
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
        }
    }
}

async fn do_contact(
    state: &Arc<AppState>,
    session_id: Uuid,
    name: &str,
    ip: &str,
    password: Option<&str>,
) -> Result<(), String> {
    let url = if ip.starts_with("ws://") || ip.starts_with("wss://") {
        ip.to_string()
    } else {
        format!("ws://{}/echat", ip)
    };

    let ws = tokio::time::timeout(Duration::from_secs(5), connect_async(&url))
        .await
        .map_err(|_| format!("连接 {} 超时", name))?
        .map_err(|e| format!("连接 {} 失败: {}", name, e))?
        .0;

    let (mut sink, mut stream) = ws.split();

    // 读 hello_ack
    let hello_text = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .map_err(|_| "读取握手超时".to_string())?
        .ok_or_else(|| "对方关闭连接".to_string())?
        .map_err(|e| format!("握手错误: {}", e))?;

    let hello: serde_json::Value = match hello_text {
        Message::Text(t) => serde_json::from_str(&t).map_err(|e| format!("握手格式错误: {}", e))?,
        _ => return Err("握手首帧不是文本".into()),
    };

    if hello.get("type").and_then(|x| x.as_str()) != Some("hello_ack") {
        return Err("对方服务器响应格式异常".into());
    }

    let needs_auth = hello
        .get("needs_auth")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);

    if needs_auth {
        let Some(pwd) = password else {
            return Err(format!(
                "服务器 {} 需要接入密码，请先 /Echat server del {} 再带密码重新 add",
                name, name
            ));
        };
        let auth_msg = serde_json::json!({"type":"auth","password":pwd}).to_string();
        sink.send(Message::Text(auth_msg))
            .await
            .map_err(|e| format!("发送认证失败: {}", e))?;

        let result_text = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .map_err(|_| "等待认证结果超时".to_string())?
            .ok_or_else(|| "对方关闭连接".to_string())?
            .map_err(|e| format!("读取认证结果失败: {}", e))?;

        let result: serde_json::Value = match result_text {
            Message::Text(t) => serde_json::from_str(&t)
                .map_err(|e| format!("认证结果格式错误: {}", e))?,
            _ => return Err("认证结果不是文本".into()),
        };

        let ok = result
            .get("data")
            .and_then(|d| d.get("ok"))
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        if !ok {
            return Err(format!("接入密码被 {} 拒绝", name));
        }
    }

    // 后台任务：持续读取对方广播
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_bg = cancel.clone();
    let sid = session_id;
    let state_bg = state.clone();
    let name_bg = name.to_string();
    tokio::spawn(async move {
        let _sink = sink; // 保活
        loop {
            tokio::select! {
                _ = cancel_bg.cancelled() => break,
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(t))) => {
                            let content = extract_event_content(&t)
                                .unwrap_or_else(|| t.to_string());
                            state_bg
                                .send_to(sid, Outgoing::event(&name_bg, content))
                                .await;
                        }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                        _ => {}
                    }
                }
            }
        }
    });

    // 记录连接
    let sess_arc = {
        let entry = state
            .sessions
            .get(&session_id)
            .ok_or_else(|| "会话丢失".to_string())?;
        let s = entry.value().clone();
        drop(entry);
        s
    };
    let mut guard = sess_arc.lock().await;
    guard.connected.insert(
        name.to_string(),
        ConnectedServer { ip: ip.to_string(), cancel },
    );

    Ok(())
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
    let rows = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT name, ip, password FROM servers WHERE owner = ? ORDER BY name",
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
    for (name, ip, pwd) in rows {
        let already = {
            let entry = state.sessions.get(&session_id).unwrap();
            let sess = entry.value().clone();
            drop(entry);
            let guard = sess.lock().await;
            guard.connected.contains_key(&name)
        };
        if already {
            ok_count += 1;
            continue;
        }
        match do_contact(state, session_id, &name, &ip, pwd.as_deref()).await {
            Ok(()) => ok_count += 1,
            Err(_) => fail_count += 1,
        }
    }
    let _ = tx.send(Outgoing::ok(format!(
        "contact-all 完成：成功 {} / 失败 {}",
        ok_count, fail_count
    )));
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

/// 一期兼容：仅名字/IP，返回 (name, ip)
async fn resolve_server(
    state: &AppState,
    user: &str,
    target: &str,
) -> Result<(String, String), String> {
    let (n, ip, _) = resolve_server_full(state, user, target).await?;
    Ok((n, ip))
}

/// 二期：返回 (name, ip, password)
async fn resolve_server_full(
    state: &AppState,
    user: &str,
    target: &str,
) -> Result<(String, String, Option<String>), String> {
    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT name, ip, password FROM servers WHERE owner = ? AND name = ?",
    )
    .bind(user)
    .bind(target)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| e.to_string())?;
    if let Some(v) = row {
        return Ok(v);
    }
    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT name, ip, password FROM servers WHERE owner = ? AND ip = ?",
    )
    .bind(user)
    .bind(target)
    .fetch_optional(&state.db)
    .await
    .map_err(|e| e.to_string())?;
    if let Some(v) = row {
        return Ok(v);
    }
    if target.contains(':') || target.contains('.') {
        return Ok((target.to_string(), target.to_string(), None));
    }
    Err(format!("找不到服务器 {}", target))
}

fn extract_event_content(raw: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(raw).ok()?;
    if v.get("type")?.as_str()? == "event" {
        Some(v.get("content")?.as_str()?.to_string())
    } else {
        None
    }
}