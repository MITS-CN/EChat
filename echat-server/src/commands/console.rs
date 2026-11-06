use std::sync::atomic::Ordering;
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
            println!("控制台已就绪（直接输入命令，也兼容 /Echat 前缀；输入 help 查看命令）");

            loop {
                match rl.readline("echat> ") {
                    Ok(line) => {
                        let line = line.trim().to_string();
                        if line.is_empty() {
                            continue;
                        }
                        // 含敏感信息的命令不进历史
                        let lower = line.to_lowercase();
                        let is_secret = lower.starts_with("/echat password")
                            || lower.starts_with("password ")
                            || lower.starts_with("/echat password-op")
                            || lower.starts_with("password-op ")
                            || lower.starts_with("/echat federation-password")
                            || lower.starts_with("federation-password ");
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

    // 兼容 "/Echat xxx" 与直接 "xxx"
    let (cmd, rest): (String, &[&str]) = if tokens[0].eq_ignore_ascii_case("/echat") {
        if tokens.len() < 2 {
            print_help();
            return;
        }
        (tokens[1].to_lowercase(), &tokens[2..])
    } else {
        (tokens[0].to_lowercase(), &tokens[1..])
    };

    match cmd.as_str() {
        "help" => print_help(),
        "status" => c_status(state).await,
        "banlist" => c_banlist(state).await,
        "list" => c_list(state).await,
        "kick" => c_kick(state, rest).await,
        "ban" => c_ban(state, rest).await,
        "ban-ip" => c_ban_ip(state, rest).await,
        "pardon" => c_pardon(state, rest).await,
        "pardon-ip" => c_pardon_ip(state, rest).await,
        "op" => c_op(state, rest).await,
        "password" => c_password(state, rest).await,
        "password-op" => c_password_op(state, rest).await,
        "password-op-reset" => c_password_op_reset(state).await,
        "federation-password" => c_federation_password(state, rest).await,
        "log" => c_log(state, rest).await,
        "exit" => {
            println!("正在关闭服务器...");
            std::process::exit(0);
        }
        "say" => {
            let msg = rest.join(" ");
            if msg.trim().is_empty() {
                println!("用法: say <消息>");
                return;
            }
            state.broadcast("Console", &msg).await;
            println!("[Console] {}", msg);
        }
        other => println!("未知命令: {}（输入 help 查看）", other),
    }
}

fn print_help() {
    println!(
        r#"Echat 控制台命令（直接输入即可，前缀 /Echat 也可）:
  help                         显示本帮助
  status                       显示服务器运行时状态
  list                         列出在线会话
  kick <名称> [原因]            踢出用户
  ban <名称> [原因]             封禁用户名
  ban-ip <IP> [原因]            封禁 IP
  banlist                      查看封禁列表
  pardon <名称>                 解封用户名
  pardon-ip <IP>                解封 IP
  op <名称>                     授予在线用户 OP 权限
  say <消息>                    广播消息
  password set [PWD]           设置接入密码（首次）
  password reset [PWD]         重设接入密码（不影响已有连接）
  password clear               清除接入密码（开放模式）
  password-op [PWD]            设置 OP 密码
  password-op-reset            清空 OP 密码 + 撤销在线 OP
  federation-password set [PWD]    设置联邦密码（三期占位）
  federation-password reset [PWD]  重设联邦密码并断开联邦连接
  federation-password clear        清除联邦密码
  log <on|off|trace|debug|info|warn|error>  切换日志级别
  exit                         关闭服务器"#
    );
}

// ==================== 状态 ====================

async fn c_status(state: &Arc<AppState>) {
    let tls = state.tls_enabled.load(Ordering::Relaxed);
    let level = match state.log_level.load(Ordering::Relaxed) {
        0 => "trace",
        1 => "debug",
        2 => "info",
        3 => "warn",
        _ => "error",
    };
    let access = if state.access_password_hash.read().await.is_some() {
        "已启用"
    } else {
        "开放"
    };
    let op = if state.op_password_hash.read().await.is_some() {
        "已设置"
    } else {
        "未设置"
    };
    let fed = if state.federation_password.read().await.is_some() {
        "已启用"
    } else {
        "未启用"
    };
    let online = state.sessions.len();

    println!("Echat Server 状态");
    println!("  监听地址    : {}", state.config.bind);
    println!("  服务器名    : {}", state.config.server_name);
    println!("  数据库      : {}", state.config.db_path);
    println!("  TLS         : {}", if tls { "已启用" } else { "未启用（HTTP）" });
    println!("  日志级别    : {}", level);
    println!("  接入密码    : {}", access);
    println!("  OP 密码     : {}", op);
    println!("  联邦密码    : {}", fed);
    println!("  在线会话    : {}", online);
}

// ==================== 封禁 ====================

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
            let fed = if g.is_federation { " [FED]" } else { "" };
            println!(
                "  {} {}{}{} | 已连接: {}",
                sid, user, op, fed, g.connected.len()
            );
            count += 1;
        }
    }
    println!("在线会话: {} 个", count);
}

async fn c_kick(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: kick [名称] [原因...]");
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
        println!("用法: ban [名称] [原因...]");
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
        println!("用法: ban-ip [IP] [原因...]");
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
        println!("用法: pardon [名称]");
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
        println!("用法: pardon-ip [IP]");
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
        println!("用法: op [名称]");
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

// ==================== 密码管理 ====================

fn hash_pwd(pwd: &str) -> Result<String, String> {
    if pwd.chars().count() < 4 {
        return Err("密码至少 4 位".into());
    }
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(pwd.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

async fn c_password(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: password <set|reset|clear> [PWD]");
        return;
    }
    match args[0].to_lowercase().as_str() {
        "set" => {
            if args.len() < 2 {
                println!("用法: password set [PWD]");
                return;
            }
            if state.access_password_hash.read().await.is_some() {
                println!("接入密码已存在，请使用 password reset [PWD] 或 password clear");
                return;
            }
            let hash = match hash_pwd(args[1]) {
                Ok(h) => h,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };
            if let Err(e) = crate::db::set_setting(&state.db, "access_password_hash", &hash).await {
                println!("保存失败: {}", e);
                return;
            }
            *state.access_password_hash.write().await = Some(hash);
            println!("接入密码已设置，客户端连接时需提供");
        }
        "reset" => {
            if args.len() < 2 {
                println!("用法: password reset [PWD]");
                return;
            }
            let hash = match hash_pwd(args[1]) {
                Ok(h) => h,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };
            if let Err(e) = crate::db::set_setting(&state.db, "access_password_hash", &hash).await {
                println!("保存失败: {}", e);
                return;
            }
            *state.access_password_hash.write().await = Some(hash);
            println!("接入密码已重设（新连接使用新密码，已有连接不受影响）");
        }
        "clear" => {
            let _ = crate::db::del_setting(&state.db, "access_password_hash").await;
            *state.access_password_hash.write().await = None;
            println!("接入密码已清除，服务器变为开放模式");
        }
        other => println!("未知 password 子命令: {}", other),
    }
}

async fn c_password_op(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: password-op [PASSWORD-OP]");
        return;
    }
    let hash = match hash_pwd(args[0]) {
        Ok(h) => h,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    if let Err(e) = crate::db::set_setting(&state.db, "op_password_hash", &hash).await {
        println!("保存失败: {}", e);
        return;
    }
    *state.op_password_hash.write().await = Some(hash);
    println!("OP 密码已更新（下一次 OP Login 生效）");
}

async fn c_password_op_reset(state: &Arc<AppState>) {
    let _ = crate::db::del_setting(&state.db, "op_password_hash").await;
    let _ = sqlx::query("DELETE FROM ops").execute(&state.db).await;
    *state.op_password_hash.write().await = None;

    let snapshot: Vec<uuid::Uuid> = state.sessions.iter().map(|e| *e.key()).collect();
    let mut revoked = 0;
    for sid in snapshot {
        if let Some(entry) = state.sessions.get(&sid) {
            let sess = entry.value().clone();
            drop(entry);
            let mut g = sess.lock().await;
            if g.is_op {
                g.is_op = false;
                revoked += 1;
            }
        }
    }
    println!(
        "OP 密码已重置（清空），ops 表已清空，撤销 {} 个在线 OP 会话。需 password-op [PWD] 重新设置",
        revoked
    );
}

// ==================== 联邦密码（三期占位） ====================

async fn c_federation_password(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: federation-password <set|reset|clear> [PWD]");
        return;
    }
    match args[0].to_lowercase().as_str() {
        "set" => {
            if args.len() < 2 {
                println!("用法: federation-password set [PWD]");
                return;
            }
            if state.federation_password.read().await.is_some() {
                println!("联邦密码已存在，请使用 federation-password reset [PWD] 或 clear");
                return;
            }
            let hash = match hash_pwd(args[1]) {
                Ok(h) => h,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };
            if let Err(e) =
                crate::db::set_setting(&state.db, "federation_password", &hash).await
            {
                println!("保存失败: {}", e);
                return;
            }
            *state.federation_password.write().await = Some(hash);
            println!("联邦密码已设置（三期功能，目前仅认证握手可用）");
        }
        "reset" => {
            if args.len() < 2 {
                println!("用法: federation-password reset [PWD]");
                return;
            }
            let hash = match hash_pwd(args[1]) {
                Ok(h) => h,
                Err(e) => {
                    println!("{}", e);
                    return;
                }
            };
            if let Err(e) =
                crate::db::set_setting(&state.db, "federation_password", &hash).await
            {
                println!("保存失败: {}", e);
                return;
            }
            *state.federation_password.write().await = Some(hash);
            state.drop_all_federation().await;
            println!("联邦密码已重设，已断开所有联邦连接");
        }
        "clear" => {
            let _ = crate::db::del_setting(&state.db, "federation_password").await;
            *state.federation_password.write().await = None;
            state.drop_all_federation().await;
            println!("联邦密码已清除，联邦功能关闭");
        }
        other => println!("未知 federation-password 子命令: {}", other),
    }
}

// ==================== 日志 ====================

async fn c_log(state: &Arc<AppState>, args: &[&str]) {
    if args.is_empty() {
        println!("用法: log <on|off|trace|debug|info|warn|error>");
        return;
    }
    let level = args[0].to_lowercase();
    match level.as_str() {
        "on" | "off" | "trace" | "debug" | "info" | "warn" | "error" => {
            (state.log_control)(&level);
            let effective = match level.as_str() {
                "on" => "info",
                "off" => "error",
                other => other,
            };
            println!("日志级别已切换为: {}", effective);
        }
        other => println!("未知日志级别: {}（可选 on/off/trace/debug/info/warn/error）", other),
    }
}