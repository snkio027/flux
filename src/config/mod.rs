mod load;
mod schema;

use anyhow::{Result, bail};

const DEFAULT_CLIENT_ID: &str = "vehicle-signal-processor";
const DEFAULT_AUTO_COMMIT_INTERVAL_MS: u64 = 5_000;
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_MAX_POLL_INTERVAL_MS: u64 = 300_000;
const DEFAULT_WORK_QUEUE_CAPACITY: usize = 2_048;
const DEFAULT_COMPLETION_QUEUE_CAPACITY: usize = 2_048;
const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const DEFAULT_OBJECT_QUEUE_CAPACITY: usize = 256;
const DEFAULT_OBJECT_WORKER_COUNT: usize = 8;
const DEFAULT_MAX_OBJECT_SIZE: u64 = 1024 * 1024 * 1024;
const DEFAULT_S3_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_S3_CONNECT_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_S3_OPERATION_ATTEMPT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_S3_OPERATION_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_S3_STREAM_IDLE_TIMEOUT_MS: u64 = 20_000;
const DEFAULT_PREFETCH_MAX_KBYTES: u32 = 16 * 1024;
const DEFAULT_PAUSE_HIGH_WATERMARK_PERCENT: u8 = 80;
const DEFAULT_RESUME_LOW_WATERMARK_PERCENT: u8 = 50;
const DEFAULT_SHUTDOWN_GRACE_MS: u64 = 30_000;
pub(crate) const RECORD_ACCOUNTING_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub kafka: KafkaConfig,
    pub ingress: IngressConfig,
    pub object_processing: ObjectProcessingConfig,
    pub s3: S3Config,
    pub shutdown: ShutdownConfig,
}

impl AppConfig {
    /// Validates all configuration and cross-field constraints.
    ///
    /// # Errors
    ///
    /// Returns an error describing the first invalid setting.
    pub fn validate(&self) -> Result<()> {
        self.kafka.validate()?;
        self.ingress.validate()?;
        self.object_processing.validate()?;
        self.s3.validate()?;
        self.shutdown.validate()
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Copy, Debug)]
pub struct ObjectProcessingConfig {
    pub queue_capacity: usize,
    pub worker_count: usize,
    /// Maximum accepted value of the wire-level `size` field, in bytes.
    pub max_object_size: u64,
}

impl Default for ObjectProcessingConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_OBJECT_QUEUE_CAPACITY,
            worker_count: DEFAULT_OBJECT_WORKER_COUNT,
            max_object_size: DEFAULT_MAX_OBJECT_SIZE,
        }
    }
}

impl ObjectProcessingConfig {
    fn validate(self) -> Result<()> {
        if self.queue_capacity == 0 {
            bail!("object_processing.queue_capacity must be greater than zero");
        }
        if self.worker_count == 0 {
            bail!("object_processing.worker_count must be greater than zero");
        }
        if self.max_object_size == 0 {
            bail!("object_processing.max_object_size must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct S3Config {
    /// Explicit region override. When absent, the AWS default provider chain is used.
    pub region: Option<String>,
    /// Optional endpoint override for S3-compatible services and local tests.
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    /// Maximum attempts including the initial request.
    pub max_attempts: u32,
    pub connect_timeout_ms: u64,
    pub operation_attempt_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    /// Maximum idle wait between chunks after `GetObject` returns its response.
    pub stream_idle_timeout_ms: u64,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            region: None,
            endpoint_url: None,
            force_path_style: false,
            max_attempts: DEFAULT_S3_MAX_ATTEMPTS,
            connect_timeout_ms: DEFAULT_S3_CONNECT_TIMEOUT_MS,
            operation_attempt_timeout_ms: DEFAULT_S3_OPERATION_ATTEMPT_TIMEOUT_MS,
            operation_timeout_ms: DEFAULT_S3_OPERATION_TIMEOUT_MS,
            stream_idle_timeout_ms: DEFAULT_S3_STREAM_IDLE_TIMEOUT_MS,
        }
    }
}

impl S3Config {
    pub(crate) fn validate(&self) -> Result<()> {
        if self
            .region
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            bail!("s3.region must not be empty when present");
        }
        if self
            .endpoint_url
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            bail!("s3.endpoint_url must not be empty when present");
        }
        if !(1..=10).contains(&self.max_attempts) {
            bail!("s3.max_attempts must be between 1 and 10");
        }
        if self.connect_timeout_ms == 0
            || self.operation_attempt_timeout_ms == 0
            || self.operation_timeout_ms == 0
            || self.stream_idle_timeout_ms == 0
        {
            bail!("all S3 timeout values must be greater than zero");
        }
        if self.connect_timeout_ms > self.operation_attempt_timeout_ms {
            bail!("s3.connect_timeout_ms must not exceed operation_attempt_timeout_ms");
        }
        if self.operation_attempt_timeout_ms > self.operation_timeout_ms {
            bail!("s3.operation_attempt_timeout_ms must not exceed operation_timeout_ms");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
