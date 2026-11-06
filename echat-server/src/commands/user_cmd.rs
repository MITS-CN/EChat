use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::get_session;
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
        let _ = tx.send(Outgoing::err("用法: /Echat user reg <name|password> ..."));
        return;
    }
    match args[0].to_lowercase().as_str() {
        "reg" => {
            if args.len() < 2 {
                let _ = tx.send(Outgoing::err(
                    "用法: /Echat user reg <name|password> ...",
                ));
                return;
            }
            match args[1].to_lowercase().as_str() {
                "name" => reg_name(state, session_id, &args[2..], tx).await,
                "password" => reg_password(state, session_id, &args[2..], tx).await,
                other => {
                    let _ = tx.send(Outgoing::err(format!("未知 user reg 子命令: {}", other)));
                }
            }
        }
        other => {
            let _ = tx.send(Outgoing::err(format!("未知 user 子命令: {}", other)));
        }
    }
}

async fn reg_name(
    state: &Arc<AppState>,
    session_id: Uuid,
    args: &[&str],
    tx: &mpsc::UnboundedSender<Outgoing>,
) {
    if args.len() < 2 {
        let _ = tx.send(Outgoing::err(
            "用法: /Echat user reg name [名称] [NAME/IP]",
        ));
        return;
    }
    let name = args[0];
    let target = args[1];

    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        let _ = tx.send(Outgoing::err("名称只允许字母、数字和下划线"));
        return;
    }

    if !is_current_server(state, target) {
        let _ = tx.send(Outgoing::err(format!(
            "一期仅支持在当前服务端注册，[NAME/IP] 需为 '{}' 或当前服务端地址",
            state.config.server_name
        )));
        return;
    }

    let exists: Option<(String,)> = sqlx::query_as("SELECT name FROM users WHERE name = ?")
        .bind(name)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    if exists.is_some() {
        let _ = tx.send(Outgoing::err(format!("名称 {} 已被注册", name)));
        return;
    }

    let session = match get_session(state, session_id).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };
    let mut guard = session.lock().await;
    guard.reg_state = RegState::PendingName {
        name: name.to_string(),
        target: target.to_string(),
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
        let _ = tx.send(Outgoing::err(
            "用法: /Echat user reg password [密码] [重复密码] [NAME/IP]",
        ));
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
        Err(e) => {
            let _ = tx.send(Outgoing::err(e));
            return;
        }
    };

    let (name, target) = {
        let mut guard = session.lock().await;
        match std::mem::replace(&mut guard.reg_state, RegState::Idle) {
            RegState::PendingName { name, target } => (name, target),
            _ => {
                drop(guard);
                let _ = tx.send(Outgoing::err(
                    "请先执行 /Echat user reg name [名称] [NAME/IP]",
                ));
                return;
            }
        }
    };

    if let Some(t) = target_arg {
        if t != target {
            let _ = tx.send(Outgoing::err(format!(
                "服务器标识 {} 与 reg name 时不一致（应为 {}）",
                t, target
            )));
            return;
        }
    }

    let salt = SaltString::generate(&mut OsRng);
    let hash = match Argon2::default().hash_password(pwd.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(e) => {
            let _ = tx.send(Outgoing::err(format!("密码哈希失败: {}", e)));
            return;
        }
    };

    let now = chrono::Utc::now().to_rfc3339();
    match sqlx::query(
        "INSERT INTO users (name, password_hash, created_at) VALUES (?, ?, ?)",
    )
    .bind(&name)
    .bind(&hash)
    .bind(&now)
    .execute(&state.db)
    .await
    {
        Ok(_) => {
            let mut guard = session.lock().await;
            guard.user = Some(name.clone());
            drop(guard);
            let _ = tx.send(Outgoing::ok(format!(
                "注册成功，欢迎 {}！现在可以使用 Echat 全部功能",
                name
            )));
            let announce = format!("{} 已上线", name);
            state.broadcast("System", &announce).await;
        }
        Err(e) => {
            let _ = tx.send(Outgoing::err(format!("注册失败: {}", e)));
        }
    }
}

fn is_current_server(state: &Arc<AppState>, target: &str) -> bool {
    target.eq_ignore_ascii_case(&state.config.server_name)
        || target.contains(&state.config.bind.split(':').next().unwrap_or(""))
        || target == "127.0.0.1"
        || target == "localhost"
}