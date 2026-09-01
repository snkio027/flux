use std::{env, path::PathBuf};

use anyhow::{Context, Result};
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
    let ingress = flux::run(config, shutdown.clone());
    tokio::pin!(ingress);

    tokio::select! {
        result = &mut ingress => result,
        signal = wait_for_shutdown_signal() => {
            signal?;
            info!("shutdown requested");
            shutdown.cancel();
            ingress.await
        }
    }
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("failed to listen for Ctrl-C")
            }
            received = terminate.recv() => {
                received.context("SIGTERM listener closed unexpectedly")
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl-C")
    }
}
