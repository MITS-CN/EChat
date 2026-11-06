mod commands;
mod config;
mod db;
mod protocol;
mod session;
mod state;

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::Layer;

struct LevelFilter(Arc<AtomicU8>);

impl<S: tracing::Subscriber> Layer<S> for LevelFilter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) -> bool {
        let current = self.0.load(Ordering::Relaxed);
        let msg_level = match *metadata.level() {
            tracing::Level::TRACE => 0u8,
            tracing::Level::DEBUG => 1,
            tracing::Level::INFO => 2,
            tracing::Level::WARN => 3,
            tracing::Level::ERROR => 4,
        };
        msg_level >= current
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;

    let log_level = Arc::new(AtomicU8::new(2));
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(LevelFilter(log_level.clone()))
        .init();

    let ll = log_level.clone();
    let log_control: state::LogControl = Box::new(move |level: &str| {
        let v = match level {
            "trace" => 0u8, "debug" => 1,
            "info" | "on" => 2, "warn" => 3,
            "error" | "off" => 4, _ => 2,
        };
        ll.store(v, Ordering::Relaxed);
    });

    let config = config::Config::from_env();
    info!("Echat Server 启动 | 监听={} | 服务器名={} | DB={}",
        config.bind, config.server_name, config.db_path);

    let db = db::init_db(&config.db_path).await?;

    // 三期 A：扫描用户名大小写冲突
    let conflicts = db::scan_case_conflicts(&db).await;
    for group in &conflicts {
        warn!("用户名大小写冲突（三期起不区分大小写）：{:?}", group);
    }

    // OP 密码
    let op_password_hash = match db::get_setting(&db, "op_password_hash").await {
        Some(h) => Some(h),
        None => {
            let default_pwd = std::env::var("ECHAT_OP_PASSWORD").unwrap_or_else(|_| "admin123".into());
            let salt = SaltString::generate(&mut OsRng);
            let hash = Argon2::default().hash_password(default_pwd.as_bytes(), &salt)
                .map_err(|e| anyhow::anyhow!("OP 密码哈希失败: {}", e))?
                .to_string();
            db::set_setting(&db, "op_password_hash", &hash).await?;
            info!("首次启动：OP 密码已从环境变量初始化，可用控制台 password-op 修改");
            Some(hash)
        }
    };

    let access_password_hash = db::get_setting(&db, "access_password_hash").await;
    if access_password_hash.is_none() {
        info!("服务器当前无接入密码（开放模式），可用控制台 password set [PWD] 设置");
    } else {
        info!("服务器已启用接入密码");
    }

    let federation_password = db::get_setting(&db, "federation_password").await;
    if federation_password.is_some() {
        info!("服务器已启用联邦密码（三期功能，目前仅认证握手可用）");
    }

    let tls_cert = std::env::var("ECHAT_TLS_CERT").ok();
    let tls_key = std::env::var("ECHAT_TLS_KEY").ok();
    let tls_enabled = Arc::new(AtomicBool::new(tls_cert.is_some() && tls_key.is_some()));

    let state = Arc::new(state::AppState::new(
        db, config.clone(),
        op_password_hash, access_password_hash, federation_password,
        tls_enabled.clone(), log_level.clone(), log_control,
    ));

    let console_state = state.clone();
    commands::console::run(console_state);

    // ===== 三期 C：启动后台清理任务 =====
    // 每 24 小时清理一次 30 天前的消息
    let clean_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
        loop {
            interval.tick().await;
            let cutoff_str = (chrono::Utc::now() - chrono::Duration::days(30)).to_rfc3339();
            let res = sqlx::query("DELETE FROM messages WHERE created_at < ?")
                .bind(&cutoff_str)
                .execute(&clean_state.db)
                .await;
            if let Ok(r) = res {
                if r.rows_affected() > 0 {
                    info!("[自动清理] 已清理 {} 条超过 30 天的旧消息", r.rows_affected());
                }
            }
        }
    });

    let app = Router::new()
        .route("/echat", get(ws_handler))
        .route("/", get(root))
        .with_state(state);

    if tls_enabled.load(Ordering::Relaxed) {
        let cert = tls_cert.unwrap();
        let key = tls_key.unwrap();
        info!("TLS 已启用 | cert={} key={}", cert, key);
        let tls_cfg = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
        let addr: std::net::SocketAddr = config.bind.parse()?;
        axum_server::bind_rustls(addr, tls_cfg)
            .serve(app.into_make_service()).await?;
    } else {
        info!("TLS 未启用（HTTP）。设置 ECHAT_TLS_CERT / ECHAT_TLS_KEY 可启用");
        let listener = TcpListener::bind(&config.bind).await?;
        info!("监听已就绪，等待连接...");
        axum::serve(listener, app).await?;
    }
    Ok(())
}

async fn root() -> &'static str {
    "Echat Server is running. WebSocket endpoint: /echat\n"
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<state::AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| session::handle_ws(socket, state))
}