use std::sync::Arc;

use rustyline::DefaultEditor;
use tokio::runtime::Runtime;

use crate::state::AppState;

pub fn run(state: Arc<AppState>) {
    // 独立线程 + 独立 runtime，避免与主 runtime 嵌套
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
                        let _ = rl.add_history_entry(&line);
                        rt.block_on(handle(&state, &line));
                    }
                    Err(rustyline::error::ReadlineError::Interrupted) => {
                        // Ctrl+C：忽略，继续
                        continue;
                    }
                    Err(rustyline::error::ReadlineError::Eof) => {
                        // Ctrl+D：退出
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
        println!("用法: /Echat <banlist|list|kick|ban|ban-ip|pardon|pardon-ip|op|exit|say> ...");
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
        "exit" => {
            println!("正在关闭服务器...");
            std::process::exit(0);
        }
        "say" => {
            let msg = tokens[2..].join(" ");
            state.broadcast("Console", &msg).await;
            println!("[Console] {}", msg);
        }
        other => println!("未知控制台命令: {}", other),
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

    // 若在线，立即授予 OP
    if let Some((_, sess)) = state.find_session_by_user(name).await {
        let mut g = sess.lock().await;
        g.is_op = true;
        println!("已为在线用户 {} 授予 OP 权限", name);
    } else {
        println!("{} 不在线，已记录到 OP 名单", name);
    }
}