use anyhow::Result;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub rpc_http_url: String,
    pub rpc_ws_url: String,
    pub batch_size: usize,
    pub max_concurrent_requests: usize,
    pub db_max_connections: u32,
    pub port: u16,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenv::dotenv().ok();

        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:password@localhost:5432/taiko_indexer".to_string());

        let rpc_http_url = env::var("RPC_HTTP_URL")
            .unwrap_or_else(|_| "https://rpc.ankr.com/taiko".to_string());

        let rpc_ws_url = env::var("RPC_WS_URL")
            .unwrap_or_else(|_| "wss://rpc.ankr.com/taiko/ws".to_string());

        let batch_size = env::var("BATCH_SIZE")
            .unwrap_or_else(|_| "1000".to_string())
            .parse::<usize>()
            .unwrap_or(1000);

        let max_concurrent_requests = env::var("MAX_CONCURRENT_REQUESTS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<usize>()
            .unwrap_or(10);

        let db_max_connections = env::var("DB_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "20".to_string())
            .parse::<u32>()
            .unwrap_or(20);

        let port = env::var("PORT")
            .unwrap_or_else(|_| "5556".to_string())
            .parse::<u16>()
            .unwrap_or(5556);

        Ok(Self {
            database_url,
            rpc_http_url,
            rpc_ws_url,
            batch_size,
            max_concurrent_requests,
            db_max_connections,
            port,
        })
    }
}