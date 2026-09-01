use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, de::Error as _};

const DEFAULT_CLIENT_ID: &str = "vehicle-signal-processor";
const DEFAULT_AUTO_COMMIT_INTERVAL_MS: u64 = 5_000;
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_MAX_POLL_INTERVAL_MS: u64 = 300_000;
const DEFAULT_WORK_QUEUE_CAPACITY: usize = 2_048;
const DEFAULT_COMPLETION_QUEUE_CAPACITY: usize = 2_048;
const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_PREFETCH_MAX_KBYTES: u32 = 16 * 1024;
const DEFAULT_PAUSE_HIGH_WATERMARK_PERCENT: u8 = 80;
const DEFAULT_RESUME_LOW_WATERMARK_PERCENT: u8 = 50;
const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 30_000;
pub(crate) const RECORD_ACCOUNTING_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub ingress: IngressConfig,
    pub shutdown: ShutdownConfig,
}

impl AppConfig {
    /// Loads and validates a TOML configuration file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, parsed, or validated.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let source = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let config = toml::from_str::<Self>(&source)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validates all cross-field ingress constraints.
    ///
    /// # Errors
    ///
    /// Returns an error describing the first invalid setting.
    pub fn validate(&self) -> Result<()> {
        self.kafka.validate()?;
        self.ingress.validate()?;
        self.shutdown.validate()
    }
}

impl<'de> Deserialize<'de> for AppConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        SerializedAppConfig::deserialize(deserializer)?
            .into_config()
            .map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub bootstrap_servers: Vec<String>,
    pub group_id: String,
    pub topics: Vec<String>,
    pub client_id: String,
    pub auto_offset_reset: AutoOffsetReset,
    pub auto_commit_interval_ms: u64,
    pub session_timeout_ms: u64,
    pub max_poll_interval_ms: u64,
    pub prefetch_max_kbytes: u32,
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
        if !(1..=2_097_151).contains(&self.prefetch_max_kbytes) {
            bail!("kafka.prefetch_max_kbytes must be between 1 and 2097151");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngressConfig {
    pub work_queue_capacity: usize,
    pub completion_queue_capacity: usize,
    pub memory_budget_bytes: usize,
    pub max_payload_bytes: usize,
    pub backpressure: BackpressureConfig,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            work_queue_capacity: DEFAULT_WORK_QUEUE_CAPACITY,
            completion_queue_capacity: DEFAULT_COMPLETION_QUEUE_CAPACITY,
            memory_budget_bytes: DEFAULT_MEMORY_BUDGET_BYTES,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            backpressure: BackpressureConfig::default(),
        }
    }
}

impl IngressConfig {
    fn validate(&self) -> Result<()> {
        if self.work_queue_capacity == 0 {
            bail!("ingress.work_queue_capacity must be greater than zero");
        }
        if self.completion_queue_capacity == 0 {
            bail!("ingress.completion_queue_capacity must be greater than zero");
        }
        if self.memory_budget_bytes == 0 || self.memory_budget_bytes > u32::MAX as usize {
            bail!(
                "ingress.memory_budget_bytes must be between 1 and {}",
                u32::MAX
            );
        }
        if self.max_payload_bytes == 0 || self.max_payload_bytes > self.memory_budget_bytes {
            bail!("ingress.max_payload_bytes must be between 1 and memory_budget_bytes");
        }
        self.backpressure.validate()
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackpressureConfig {
    pub pause_high_watermark_percent: u8,
    pub resume_low_watermark_percent: u8,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            pause_high_watermark_percent: DEFAULT_PAUSE_HIGH_WATERMARK_PERCENT,
            resume_low_watermark_percent: DEFAULT_RESUME_LOW_WATERMARK_PERCENT,
        }
    }
}

impl BackpressureConfig {
    fn validate(self) -> Result<()> {
        if self.resume_low_watermark_percent == 0
            || self.resume_low_watermark_percent >= self.pause_high_watermark_percent
            || self.pause_high_watermark_percent > 100
        {
            bail!(
                "ingress backpressure watermarks must satisfy 0 < resume_low_watermark_percent < pause_high_watermark_percent <= 100"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShutdownConfig {
    pub grace_ms: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            grace_ms: DEFAULT_SHUTDOWN_GRACE_MS,
        }
    }
}

impl ShutdownConfig {
    fn validate(self) -> Result<()> {
        if self.grace_ms == 0 {
            bail!("shutdown.grace_ms must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoOffsetReset {
    Earliest,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializedAppConfig {
    kafka: SerializedKafkaConfig,
    #[serde(default)]
    ingress: Option<IngressConfig>,
    #[serde(default)]
    shutdown: Option<ShutdownConfig>,
}

impl SerializedAppConfig {
    fn into_config(self) -> std::result::Result<AppConfig, String> {
        let (kafka, legacy_ingress, legacy_shutdown) = self.kafka.into_parts()?;

        if self.ingress.is_some() && legacy_ingress.is_some() {
            return Err(
                "ingress settings cannot be declared in both [kafka] and [ingress]".to_owned(),
            );
        }
        if self.shutdown.is_some() && legacy_shutdown.is_some() {
            return Err(
                "shutdown grace cannot be declared in both [kafka] and [shutdown]".to_owned(),
            );
        }

        Ok(AppConfig {
            kafka,
            ingress: self.ingress.or(legacy_ingress).unwrap_or_default(),
            shutdown: self.shutdown.or(legacy_shutdown).unwrap_or_default(),
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializedKafkaConfig {
    bootstrap_servers: Vec<String>,
    group_id: String,
    topics: Vec<String>,
    #[serde(default = "default_client_id")]
    client_id: String,
    auto_offset_reset: AutoOffsetReset,
    #[serde(default = "default_auto_commit_interval_ms")]
    auto_commit_interval_ms: u64,
    #[serde(default = "default_session_timeout_ms")]
    session_timeout_ms: u64,
    #[serde(default = "default_max_poll_interval_ms")]
    max_poll_interval_ms: u64,
    #[serde(default = "default_prefetch_max_kbytes")]
    prefetch_max_kbytes: u32,
    work_queue_capacity: Option<usize>,
    completion_queue_capacity: Option<usize>,
    memory_budget_bytes: Option<usize>,
    record_accounting_overhead_bytes: Option<usize>,
    max_payload_bytes: Option<usize>,
    pause_high_watermark_percent: Option<u8>,
    resume_low_watermark_percent: Option<u8>,
    shutdown_grace_ms: Option<u64>,
}

impl SerializedKafkaConfig {
    fn into_parts(
        self,
    ) -> std::result::Result<(KafkaConfig, Option<IngressConfig>, Option<ShutdownConfig>), String>
    {
        if let Some(overhead) = self.record_accounting_overhead_bytes
            && overhead != RECORD_ACCOUNTING_OVERHEAD_BYTES
        {
            return Err(format!(
                "kafka.record_accounting_overhead_bytes is fixed at {RECORD_ACCOUNTING_OVERHEAD_BYTES}"
            ));
        }

        let has_legacy_ingress = self.work_queue_capacity.is_some()
            || self.completion_queue_capacity.is_some()
            || self.memory_budget_bytes.is_some()
            || self.max_payload_bytes.is_some()
            || self.pause_high_watermark_percent.is_some()
            || self.resume_low_watermark_percent.is_some();
        let legacy_ingress = has_legacy_ingress.then(|| IngressConfig {
            work_queue_capacity: self
                .work_queue_capacity
                .unwrap_or(DEFAULT_WORK_QUEUE_CAPACITY),
            completion_queue_capacity: self
                .completion_queue_capacity
                .unwrap_or(DEFAULT_COMPLETION_QUEUE_CAPACITY),
            memory_budget_bytes: self
                .memory_budget_bytes
                .unwrap_or(DEFAULT_MEMORY_BUDGET_BYTES),
            max_payload_bytes: self.max_payload_bytes.unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES),
            backpressure: BackpressureConfig {
                pause_high_watermark_percent: self
                    .pause_high_watermark_percent
                    .unwrap_or(DEFAULT_PAUSE_HIGH_WATERMARK_PERCENT),
                resume_low_watermark_percent: self
                    .resume_low_watermark_percent
                    .unwrap_or(DEFAULT_RESUME_LOW_WATERMARK_PERCENT),
            },
        });
        let legacy_shutdown = self
            .shutdown_grace_ms
            .map(|grace_ms| ShutdownConfig { grace_ms });

        Ok((
            KafkaConfig {
                bootstrap_servers: self.bootstrap_servers,
                group_id: self.group_id,
                topics: self.topics,
                client_id: self.client_id,
                auto_offset_reset: self.auto_offset_reset,
                auto_commit_interval_ms: self.auto_commit_interval_ms,
                session_timeout_ms: self.session_timeout_ms,
                max_poll_interval_ms: self.max_poll_interval_ms,
                prefetch_max_kbytes: self.prefetch_max_kbytes,
            },
            legacy_ingress,
            legacy_shutdown,
        ))
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

fn default_prefetch_max_kbytes() -> u32 {
    DEFAULT_PREFETCH_MAX_KBYTES
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    const MINIMAL_CONFIG: &str = r#"
        [kafka]
        bootstrap_servers = ["localhost:9092"]
        group_id = "test"
        topics = ["signals"]
        auto_offset_reset = "earliest"
    "#;

    #[test]
    fn rejects_removed_offset_store_flush_setting() {
        let error = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
                auto_offset_reset = "earliest"
                offset_store_flush_ms = 100
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("offset_store_flush_ms"));
    }

    #[test]
    fn supplies_structured_runtime_defaults() {
        let config = toml::from_str::<AppConfig>(MINIMAL_CONFIG).unwrap();

        config.validate().unwrap();
        assert_eq!(config.kafka.auto_commit_interval_ms, 5_000);
        assert_eq!(config.kafka.prefetch_max_kbytes, 16 * 1024);
        assert_eq!(config.ingress.memory_budget_bytes, 64 * 1024 * 1024);
        assert_eq!(config.ingress.max_payload_bytes, 1024 * 1024);
        assert_eq!(config.ingress.backpressure.pause_high_watermark_percent, 80);
        assert_eq!(config.shutdown.grace_ms, 30_000);
    }

    #[test]
    fn accepts_legacy_runtime_fields_without_changing_values() {
        let config = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
                auto_offset_reset = "earliest"
                work_queue_capacity = 17
                memory_budget_bytes = 4096
                record_accounting_overhead_bytes = 128
                max_payload_bytes = 1024
                shutdown_grace_ms = 7000
            "#,
        )
        .unwrap();

        assert_eq!(config.ingress.work_queue_capacity, 17);
        assert_eq!(config.ingress.memory_budget_bytes, 4096);
        assert_eq!(config.ingress.max_payload_bytes, 1024);
        assert_eq!(config.shutdown.grace_ms, 7000);
    }

    #[test]
    fn rejects_non_default_legacy_record_overhead() {
        let error = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
                auto_offset_reset = "earliest"
                record_accounting_overhead_bytes = 0
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("fixed at 128"));
    }

    #[test]
    fn rejects_legacy_and_structured_runtime_fields_together() {
        let error = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
                auto_offset_reset = "earliest"
                work_queue_capacity = 17

                [ingress]
                work_queue_capacity = 18
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("both [kafka] and [ingress]"));
    }

    #[test]
    fn requires_explicit_offset_reset_policy() {
        let error = toml::from_str::<AppConfig>(
            r#"
                [kafka]
                bootstrap_servers = ["localhost:9092"]
                group_id = "test"
                topics = ["signals"]
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("auto_offset_reset"));
    }

    #[test]
    fn example_config_matches_the_structured_schema() {
        let config = toml::from_str::<AppConfig>(include_str!("../config.example.toml")).unwrap();

        config.validate().unwrap();
        assert_eq!(config.ingress.work_queue_capacity, 2_048);
        assert_eq!(config.shutdown.grace_ms, 30_000);
    }
}
