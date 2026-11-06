use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher, PasswordHash, PasswordVerifier};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{get_session, resolve_user_name, trigger_online_notify};
use crate::protocol::Outgoing;
use crate::session::RegState;
use crate::state::AppState;

pub async fn handle(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.is_empty() {
        let _ = tx.send(Outgoing::err("用法: /Echat user <reg|login|logout> ..."));
        return;
    }
    match args[0].to_lowercase().as_str() {

        "reg" => {
            if args.len() < 2 {
                let _ = tx.send(Outgoing::err(
                    "注册分两步：\n\
                     1) /Echat user reg name [名字] Echat\n\
                     2) /Echat user reg password [密码] [重复密码] Echat",
                ));
                return;
            }
            match args[1].to_lowercase().as_str() {
                "name" => reg_name(state, session_id, &args[2..], tx).await,
                "password" => reg_password(state, session_id, &args[2..], tx).await,
                other => {
                    let _ = tx.send(Outgoing::err(format!(
                        "「{}」不是 reg 的子命令。注册分两步：\n\
                         1) /Echat user reg name [名字] Echat\n\
                         2) /Echat user reg password [密码] [重复密码] Echat",
                        other
                    )));
                }
            }
        }
        "login" => login(state, session_id, &args[1..], tx).await,
        "logout" => logout(state, session_id, tx).await,
        other => { let _ = tx.send(Outgoing::err(format!("未知 user 子命令: {}", other))); }
    }
}

async fn reg_name(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat user reg name [名称] [NAME/IP]"));
        return;
    }
    let name = args[0];
    let target = args[1];

    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        let _ = tx.send(Outgoing::err("名称只允许字母、数字和下划线"));
        return;
    }
    if name.len() > 32 {
        let _ = tx.send(Outgoing::err("名称长度不能超过 32"));
        return;
    }

    if !is_current_server(state, target) {
        let _ = tx.send(Outgoing::err(format!(
            "一期仅支持在当前服务端注册，[NAME/IP] 需为 '{}' 或当前服务端地址",
            state.config.server_name
        )));
        return;
    }

    // 大小写不敏感冲突检查
    if let Some(existing) = resolve_user_name(&state.db, name).await {
        let _ = tx.send(Outgoing::err(format!("名称 {} 已被注册（已有 {}）", name, existing)));
        return;
    }

    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };
    let mut guard = session.lock().await;
    guard.reg_state = RegState::PendingName {
        name: name.to_string(), target: target.to_string(),
    };
    drop(guard);

    let _ = tx.send(Outgoing::ok(format!(
        "名称 {} 已保留，请执行: /Echat user reg password [密码] [重复密码] {}",
        name, target
    )));
}

async fn reg_password(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat user reg password [密码] [重复密码] [NAME/IP]"));
        return;
    }
    let (pwd, pwd2) = (args[0], args[1]);
    let target_arg = args.get(2).copied();

    if pwd != pwd2 {
        let _ = tx.send(Outgoing::err("两次密码不一致"));
        return;
    }
    if pwd.chars().count() < 4 {
        let _ = tx.send(Outgoing::err("密码至少 4 位"));
        return;
    }

    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => { let _ = tx.send(Outgoing::err(e)); return; }
    };

    let (name, target) = {
        let mut guard = session.lock().await;
        match std::mem::replace(&mut guard.reg_state, RegState::Idle) {
            RegState::PendingName { name, target } => (name, target),
            _ => {
                drop(guard);
                let _ = tx.send(Outgoing::err("请先执行 /Echat user reg name [名称] [NAME/IP]"));
                return;
            }
        }
    };

    if let Some(t) = target_arg {
        if t != target {
            let _ = tx.send(Outgoing::err(format!("服务器标识 {} 与 reg name 时不一致（应为 {}）", t, target)));
            return;
        }
    }

    // 二次冲突检查（防止并发注册）
    if resolve_user_name(&state.db, &name).await.is_some() {
        let _ = tx.send(Outgoing::err(format!("名称 {} 已被注册", name)));
        return;
    }

    let salt = SaltString::generate(&mut OsRng);
    let hash = match Argon2::default().hash_password(pwd.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(e) => { let _ = tx.send(Outgoing::err(format!("密码哈希失败: {}", e))); return; }
    };

    let now = chrono::Utc::now().to_rfc3339();
    match sqlx::query("INSERT INTO users (name, password_hash, created_at) VALUES (?, ?, ?)")
        .bind(&name).bind(&hash).bind(&now)
        .execute(&state.db).await
    {
        Ok(_) => {
            // Q6=A：注册即登录
            {
                let mut guard = session.lock().await;
                guard.user = Some(name.clone());
            }
            let _ = tx.send(Outgoing::ok(format!(
                "注册成功并自动登录，欢迎 {}！现在可以使用 Echat 全部功能",
                name
            )));
            let announce = format!("{} 已上线", name);
            state.broadcast("System", &announce).await;
            trigger_online_notify(state, &name).await;
        }
        Err(e) => {
            let _ = tx.send(Outgoing::err(format!("注册失败: {}", e)));
        }
    }
}

async fn login(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err("用法: /Echat user login [名称] [密码]"));
        return;
    }
    let (input_name, password) = (args[0], args[1]);

    // 检查会话是否已登录
    {
        let sess = state.sessions.get(&session_id).unwrap().clone();
        let g = sess.lock().await;
        if g.user.is_some() {
            drop(g);
            let _ = tx.send(Outgoing::err("当前会话已登录，请先 /Echat user logout"));
            return;
        }
    }

    // resolve 用户名
    let name = match resolve_user_name(&state.db, input_name).await {
        Some(n) => n,
        None => {
            let _ = tx.send(Outgoing::err(format!("用户 {} 不存在", input_name)));
            return;
        }
    };

    let is_banned: Option<(String,)> = sqlx::query_as(
        "SELECT reason FROM bans WHERE kind = 'name' AND target = ?"
    ).bind(&name).fetch_optional(&state.db).await.ok().flatten();
    
    if let Some((reason,)) = is_banned {
        let _ = tx.send(Outgoing::err(format!("你已被服务器封禁，无法登录。原因: {}", reason)));
        return;
    }

    // 检查同一账号是否已在别处登录
    if let Some((other_sid, _)) = state.find_session_by_user(&name).await {
        if other_sid != session_id {
            let _ = tx.send(Outgoing::err(format!("用户 {} 已在别处登录", name)));
            return;
        }
    }

    // 校验密码
    let row: Option<(String,)> =
        sqlx::query_as("SELECT password_hash FROM users WHERE name = ?")
            .bind(&name).fetch_optional(&state.db).await.ok().flatten();
    let hash = match row {
        Some((h,)) => h,
        None => { let _ = tx.send(Outgoing::err("用户数据异常")); return; }
    };
    let parsed = match PasswordHash::new(&hash) {
        Ok(p) => p,
        Err(_) => { let _ = tx.send(Outgoing::err("密码数据损坏")); return; }
    };
    if Argon2::default().verify_password(password.as_bytes(), &parsed).is_err() {
        let _ = tx.send(Outgoing::err("密码错误"));
        return;
    }

    {
        let sess = state.sessions.get(&session_id).unwrap().clone();
        let mut g = sess.lock().await;
        g.user = Some(name.clone());
        g.reg_state = RegState::Idle;
    }

    let _ = tx.send(Outgoing::ok(format!("登录成功，欢迎 {}", name)));
    trigger_online_notify(state, &name).await;
}

async fn logout(
    state: &Arc<AppState>,
    session_id: Uuid,
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    let sess = match state.sessions.get(&session_id) {
        Some(s) => s.clone(),
        None => { let _ = tx.send(Outgoing::err("会话不存在")); return; }
    };
    let mut guard = sess.lock().await;
    let old_name = match guard.user.take() {
        Some(n) => n,
        None => {
            drop(guard);
            let _ = tx.send(Outgoing::err("当前未登录"));
            return;
        }
    };
    guard.is_op = false;
    guard.reg_state = RegState::Idle;
    guard.show_filter = crate::session::ShowFilter::All;
    // 保留 connected（网络连接与身份无关）
    drop(guard);

    let _ = tx.send(Outgoing::ok(format!("{} 已登出", old_name)));
}

fn is_current_server(state: &Arc<AppState>, target: &str) -> bool {
    target.eq_ignore_ascii_case(&state.config.server_name)
        || target.contains(&state.config.bind.split(':').next().unwrap_or(""))
        || target == "127.0.0.1"
        || target == "localhost"
}