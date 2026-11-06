mod commands;
mod config;
mod db;
mod protocol;
mod session;
mod state;

use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHasher};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "echat_server=info".into()),
        )
        .init();

    let config = config::Config::from_env();
    info!(
        "Echat Server 启动 | 监听={} | 服务器名={} | DB={}",
        config.bind, config.server_name, config.db_path
    );

    let db = db::init_db(&config.db_path).await?;

    // 加载 / 初始化 OP 密码 hash
    let op_password_hash = match db::get_setting(&db, "op_password_hash").await {
        Some(h) => h,
        None => {
            let default_pwd =
                std::env::var("ECHAT_OP_PASSWORD").unwrap_or_else(|_| "admin123".into());
            let salt = SaltString::generate(&mut OsRng);
            let hash = Argon2::default()
                .hash_password(default_pwd.as_bytes(), &salt)
                .map_err(|e| anyhow::anyhow!("OP 密码哈希失败: {}", e))?
                .to_string();
            db::set_setting(&db, "op_password_hash", &hash).await?;
            info!("首次启动：OP 密码已从环境变量初始化，可用控制台 /Echat password-op 修改");
            hash
        }
    };

    let state = Arc::new(state::AppState::new(db, config.clone(), op_password_hash));

    // 控制台（内部自己起线程 + 独立 runtime）
    let console_state = state.clone();
    commands::console::run(console_state);

    let app = Router::new()
        .route("/echat", get(ws_handler))
        .route("/", get(root))
        .with_state(state);

    let listener = TcpListener::bind(&config.bind).await?;
    info!("监听已就绪，等待连接...");
    axum::serve(listener, app).await?;
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