use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher};
use rustyline::DefaultEditor;

use crate::state::AppState;

pub fn run(state: Arc<AppState>) {
    std::thread::Builder::new()
        .name("echat-console".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("console runtime");

            let mut rl = DefaultEditor::new().expect("rustyline");
            println!("控制台已就绪（命令以 /Echat 开头）");

            loop {
                match rl.readline("echat> ") {
                    Ok(line) => {
                        let line = line.trim().to_string();
                        if line.is_empty() {
                            continue;
                        }
                        // 含密码的命令不进历史
                        let lower = line.to_lowercase();
                        let is_secret = lower.starts_with("/echat password-op")
                            || lower.starts_with("/echat password set")
                            || lower.starts_with("/echat password reset");
                        if !is_secret {
                            let _ = rl.add_history_entry(&line);
                        }
                        rt.block_on(handle(&state, &line));
                    }
                    Err(rustyline::error::ReadlineError::Interrupted) => continue,
                    Err(rustyline::error::ReadlineError::Eof) => {
                        println!("控制台退出");
                        std::process::exit(0);
                    }
                    Err(e) => {
                        eprintln!("读取输入失败: {}", e);
                        break;
                    }
                }
            }
        })
        .expect("spawn console thread");
}

async fn handle(state: &Arc<AppState>, line: &str) {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    if tokens.is_empty() {
        return;
    }
    if !tokens[0].eq_ignore_ascii_case("/echat") {
        println!("控制台命令需以 /Echat 开头");
        return;
    }
    if tokens.len() < 2 {
        println!(
            "用法: /Echat <banlist|list|kick|ban|ban-ip|pardon|pardon-ip|op|password-op|exit|say> ..."
        );
        return;
    }
    match tokens[1].to_lowercase().as_str() {
        "banlist" => c_banlist(state).await,
        "list" => c_list(state).await,
        "kick" => c_kick(state, &tokens[2..]).await,
        "ban" => c_ban(state, &tokens[2..]).await,
        "ban-ip" => c_ban_ip(state, &tokens[2..]).await,
        "pardon" => c_pardon(state, &tokens[2..]).await,
        "pardon-ip" => c_pardon_ip(state, &tokens[2..]).await,
        "op" => c_op(state, &tokens[2..]).await,
        "password-op" => c_password_op(state, &tokens[2..]).await,
        "exit" => {
            println!("正在关闭服务器...");
            std::process::exit(0);
        }
        "say" => {
            let msg = tokens[2..].join(" ");
            state.broadcast("Console", &msg).await;
            println!("[Console] {}", msg);
        }
        other => {
            println!("未知控制台命令: {}", other);
        }
    }
}

async fn c_banlist(state: &Arc<AppState>) {
    let rows = sqlx::query_as::<_, (String, String, Option<String>, String)>(
        "SELECT kind, target, reason, banned_at FROM bans ORDER BY banned_at DESC",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    if rows.is_empty() {
        println!("封禁列表为空");
        return;
    }
    println!("封禁列表 ({} 条):", rows.len());
    for (kind, target, reason, at) in rows {
        println!(
            "  [{}] {} | {}{}",
            at,
            kind,
            target,
            reason.map(|r| format!(" | {}", r)).unwrap_or_default()
        );
    }
}

async fn c_list(state: &Arc<AppState>) {
    let snapshot: Vec<uuid::Uuid> = state.sessions.iter().map(|e| *e.key()).collect();
    let mut count = 0;
    for sid in snapshot {
        if let Some(entry) = state.sessions.get(&sid) {
            let sess = entry.value().clone();
            drop(entry);
            let g = sess.lock().await;
            let user = g.user.clone().unwrap_or_else(|| "未注册".into());
            let op = if g.is_op { " [OP]" } else { "" };
            println!(
                "  {} {}{} | 已连接: {}",
                sid,
                user,
                op,
                g.connected.len()
            );
            count += 1;
        }
    }
    println!("在线会话: {} 个", count);
}

async fn c_kick(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat kick [名称] [原因...]");
        return;
    }
    let name = args[0];
    let reason = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "被管理员踢出".into()
    };
    if let Some((sid, _)) = state.find_session_by_user(name).await {
        state.kick(sid, &reason).await;
        println!("已踢出 {}", name);
    } else {
        println!("用户 {} 不在线", name);
    }
}

async fn c_ban(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat ban [名称] [原因...]");
        return;
    }
    let name = args[0];
    let reason = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "被管理员封禁".into()
    };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO bans (kind, target, reason, banned_at) VALUES ('name', ?, ?, ?)",
    )
    .bind(name)
    .bind(&reason)
    .bind(&now)
    .execute(&state.db)
    .await;
    if let Some((sid, _)) = state.find_session_by_user(name).await {
        state.kick(sid, &reason).await;
    }
    println!("已封禁 {}", name);
}

async fn c_ban_ip(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat ban-ip [IP] [原因...]");
        return;
    }
    let ip = args[0];
    let reason = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "IP 被管理员封禁".into()
    };
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO bans (kind, target, reason, banned_at) VALUES ('ip', ?, ?, ?)",
    )
    .bind(ip)
    .bind(&reason)
    .bind(&now)
    .execute(&state.db)
    .await;
    println!("已封禁 IP {}", ip);
}

async fn c_pardon(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat pardon [名称]");
        return;
    }
    let r = sqlx::query("DELETE FROM bans WHERE kind = 'name' AND target = ?")
        .bind(args[0])
        .execute(&state.db)
        .await;
    match r {
        Ok(r) if r.rows_affected() > 0 => println!("已解封 {}", args[0]),
        _ => println!("{} 不在封禁名单中", args[0]),
    }
}

async fn c_pardon_ip(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat pardon-ip [IP]");
        return;
    }
    let r = sqlx::query("DELETE FROM bans WHERE kind = 'ip' AND target = ?")
        .bind(args[0])
        .execute(&state.db)
        .await;
    match r {
        Ok(r) if r.rows_affected() > 0 => println!("已解封 IP {}", args[0]),
        _ => println!("IP {} 不在封禁名单中", args[0]),
    }
}

async fn c_op(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat op [名称]");
        return;
    }
    let name = args[0];
    let now = chrono::Utc::now().to_rfc3339();
    let _ = sqlx::query("INSERT OR IGNORE INTO ops (name, granted_at) VALUES (?, ?)")
        .bind(name)
        .bind(&now)
        .execute(&state.db)
        .await;

    if let Some((_, sess)) = state.find_session_by_user(name).await {
        let mut g = sess.lock().await;
        g.is_op = true;
        println!("已为在线用户 {} 授予 OP 权限", name);
    } else {
        println!("{} 不在线，已记录到 OP 名单", name);
    }
}

async fn c_password_op(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: /Echat password-op [PASSWORD-OP]");
        return;
    }
    let new_pwd = args[0];
    if new_pwd.chars().count() < 4 {
        println!("OP 密码至少 4 位");
        return;
    }

    let salt = SaltString::generate(&mut OsRng);
    let hash = match Argon2::default().hash_password(new_pwd.as_bytes(), &salt) {
        Ok(h) => h.to_string(),
        Err(e) => {
            println!("哈希失败: {}", e);
            return;
        }
    };

    if let Err(e) = crate::db::set_setting(&state.db, "op_password_hash", &hash).await {
        println!("保存失败: {}", e);
        return;
    }
    *state.op_password_hash.write().await = hash;
    println!("OP 密码已更新（下一次 OP Login 生效）");
}