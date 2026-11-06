mod commands;
mod config;
mod db;
mod protocol;
mod session;
mod state;

use std::sync::Arc;

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
    let state = Arc::new(state::AppState::new(db, config.clone()));

    // 控制台命令 task
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