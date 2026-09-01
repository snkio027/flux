use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const DEFAULT_CLIENT_ID: &str = "vehicle-signal-processor";
const DEFAULT_AUTO_COMMIT_INTERVAL_MS: u64 = 5_000;
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_MAX_POLL_INTERVAL_MS: u64 = 300_000;
const DEFAULT_WORK_QUEUE_CAPACITY: usize = 1_024;
const DEFAULT_COMPLETION_QUEUE_CAPACITY: usize = 1_024;
const DEFAULT_MEMORY_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_RECORD_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
}

impl AppConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config = toml::from_str::<Self>(&source)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.kafka.validate()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub bootstrap_servers: Vec<String>,
    pub group_id: String,
    pub topics: Vec<String>,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default)]
    pub auto_offset_reset: AutoOffsetReset,
    #[serde(default = "default_auto_commit_interval_ms")]
    pub auto_commit_interval_ms: u64,
    #[serde(default = "default_session_timeout_ms")]
    pub session_timeout_ms: u64,
    #[serde(default = "default_max_poll_interval_ms")]
    pub max_poll_interval_ms: u64,
    #[serde(default = "default_work_queue_capacity")]
    pub work_queue_capacity: usize,
    #[serde(default = "default_completion_queue_capacity")]
    pub completion_queue_capacity: usize,
    #[serde(default = "default_memory_budget_bytes")]
    pub memory_budget_bytes: usize,
    #[serde(default = "default_record_overhead_bytes")]
    pub record_accounting_overhead_bytes: usize,
}

impl KafkaConfig {
    fn validate(&self) -> Result<()> {
        if self.bootstrap_servers.is_empty()
            || self
                .bootstrap_servers
                .iter()
                .any(|value| value.trim().is_empty())
        {
            bail!("kafka.bootstrap_servers must contain at least one non-empty address");
        }
        if self.group_id.trim().is_empty() {
            bail!("kafka.group_id must not be empty");
        }
        if self.client_id.trim().is_empty() {
            bail!("kafka.client_id must not be empty");
        }
        if self.topics.is_empty() || self.topics.iter().any(|value| value.trim().is_empty()) {
            bail!("kafka.topics must contain at least one non-empty topic");
        }
        if self.auto_commit_interval_ms == 0 {
            bail!("kafka.auto_commit_interval_ms must be greater than zero");
        }
        if self.session_timeout_ms == 0 {
            bail!("kafka.session_timeout_ms must be greater than zero");
        }
        if self.max_poll_interval_ms <= self.session_timeout_ms {
            bail!("kafka.max_poll_interval_ms must be greater than session_timeout_ms");
        }
        if self.work_queue_capacity == 0 {
            bail!("kafka.work_queue_capacity must be greater than zero");
        }
        if self.completion_queue_capacity == 0 {
            bail!("kafka.completion_queue_capacity must be greater than zero");
        }
        if self.memory_budget_bytes == 0 || self.memory_budget_bytes > u32::MAX as usize {
            bail!(
                "kafka.memory_budget_bytes must be between 1 and {}",
                u32::MAX
            );
        }
        if self.record_accounting_overhead_bytes > self.memory_budget_bytes {
            bail!("kafka.record_accounting_overhead_bytes exceeds memory_budget_bytes");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoOffsetReset {
    Earliest,
    #[default]
    Latest,
    Error,
}

impl AutoOffsetReset {
    pub(crate) fn as_kafka_value(self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::Error => "error",
        }
    }
}

fn default_client_id() -> String {
    DEFAULT_CLIENT_ID.to_owned()
}

fn default_auto_commit_interval_ms() -> u64 {
    DEFAULT_AUTO_COMMIT_INTERVAL_MS
}

fn default_session_timeout_ms() -> u64 {
    DEFAULT_SESSION_TIMEOUT_MS
}

fn default_max_poll_interval_ms() -> u64 {
    DEFAULT_MAX_POLL_INTERVAL_MS
}

fn default_work_queue_capacity() -> usize {
    DEFAULT_WORK_QUEUE_CAPACITY
}

fn default_completion_queue_capacity() -> usize {
    DEFAULT_COMPLETION_QUEUE_CAPACITY
}

fn default_memory_budget_bytes() -> usize {
    DEFAULT_MEMORY_BUDGET_BYTES
}

fn default_record_overhead_bytes() -> usize {
    DEFAULT_RECORD_OVERHEAD_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_removed_offset_store_flush_setting() {
        let error = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
                offset_store_flush_ms = 100
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("offset_store_flush_ms"));
    }

    #[test]
    fn supplies_v1_defaults() {
        let config = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
            "#,
        )
        .unwrap();

        config.validate().unwrap();
        assert_eq!(config.kafka.record_accounting_overhead_bytes, 128);
        assert_eq!(config.kafka.auto_commit_interval_ms, 5_000);
    }
}
