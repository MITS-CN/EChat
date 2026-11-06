#[derive(Clone, Debug)]
pub struct Config {
    pub bind: String,
    pub server_name: String,
    pub db_path: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind: std::env::var("ECHAT_BIND").unwrap_or_else(|_| "0.0.0.0:1145".into()),
            server_name: std::env::var("ECHAT_SERVER_NAME").unwrap_or_else(|_| "Echat".into()),
            db_path: std::env::var("ECHAT_DB_PATH").unwrap_or_else(|_| "echat.db".into()),
        }
    }
}