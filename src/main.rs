use std::{env, path::PathBuf};

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use flux::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .or_else(|| env::var_os("FLUX_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let config = AppConfig::from_file(&config_path)?;

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("shutdown requested");
            signal.cancel();
        }
    });

    flux::run(config, shutdown).await
}
