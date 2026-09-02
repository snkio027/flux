use serde::Deserialize;

use super::{
    AppConfig, AutoOffsetReset, BackpressureConfig, DEFAULT_AUTO_COMMIT_INTERVAL_MS,
    DEFAULT_CLIENT_ID, DEFAULT_COMPLETION_QUEUE_CAPACITY, DEFAULT_MAX_OBJECT_SIZE,
    DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_MAX_POLL_INTERVAL_MS, DEFAULT_MEMORY_BUDGET_BYTES,
    DEFAULT_OBJECT_QUEUE_CAPACITY, DEFAULT_OBJECT_WORKER_COUNT,
    DEFAULT_PAUSE_HIGH_WATERMARK_PERCENT, DEFAULT_PREFETCH_MAX_KBYTES,
    DEFAULT_RESUME_LOW_WATERMARK_PERCENT, DEFAULT_S3_CONNECT_TIMEOUT_MS, DEFAULT_S3_MAX_ATTEMPTS,
    DEFAULT_S3_OPERATION_ATTEMPT_TIMEOUT_MS, DEFAULT_S3_OPERATION_TIMEOUT_MS,
    DEFAULT_S3_STREAM_IDLE_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_SHUTDOWN_GRACE_MS,
    DEFAULT_WORK_QUEUE_CAPACITY, IngressConfig, KafkaConfig, ObjectProcessingConfig, S3Config,
    ShutdownConfig,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RawAppConfig {
    kafka: RawKafkaConfig,
    #[serde(default)]
    ingress: RawIngressConfig,
    #[serde(default)]
    object_processing: RawObjectProcessingConfig,
    #[serde(default)]
    s3: RawS3Config,
    #[serde(default)]
    shutdown: RawShutdownConfig,
}

impl From<RawAppConfig> for AppConfig {
    fn from(raw: RawAppConfig) -> Self {
        Self {
            kafka: raw.kafka.into(),
            ingress: raw.ingress.into(),
            object_processing: raw.object_processing.into(),
            s3: raw.s3.into(),
            shutdown: raw.shutdown.into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKafkaConfig {
    #[serde(deserialize_with = "deserialize_string_list")]
    bootstrap_servers: Vec<String>,
    group_id: String,
    #[serde(deserialize_with = "deserialize_string_list")]
    topics: Vec<String>,
    #[serde(default = "default_client_id")]
    client_id: String,
    auto_offset_reset: RawAutoOffsetReset,
    #[serde(default = "default_auto_commit_interval_ms")]
    auto_commit_interval_ms: u64,
    #[serde(default = "default_session_timeout_ms")]
    session_timeout_ms: u64,
    #[serde(default = "default_max_poll_interval_ms")]
    max_poll_interval_ms: u64,
    #[serde(default = "default_prefetch_max_kbytes")]
    prefetch_max_kbytes: u32,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringListRepresentation {
    Delimited(String),
    Sequence(Vec<String>),
}

fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let representation = StringListRepresentation::deserialize(deserializer)?;
    Ok(match representation {
        StringListRepresentation::Delimited(value) => value
            .split(',')
            .map(str::trim)
            .map(ToOwned::to_owned)
            .collect(),
        StringListRepresentation::Sequence(values) => values,
    })
}

impl From<RawKafkaConfig> for KafkaConfig {
    fn from(raw: RawKafkaConfig) -> Self {
        Self {
            bootstrap_servers: raw.bootstrap_servers,
            group_id: raw.group_id,
            topics: raw.topics,
            client_id: raw.client_id,
            auto_offset_reset: raw.auto_offset_reset.into(),
            auto_commit_interval_ms: raw.auto_commit_interval_ms,
            session_timeout_ms: raw.session_timeout_ms,
            max_poll_interval_ms: raw.max_poll_interval_ms,
            prefetch_max_kbytes: raw.prefetch_max_kbytes,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawAutoOffsetReset {
    Earliest,
    Latest,
    Error,
}

impl From<RawAutoOffsetReset> for AutoOffsetReset {
    fn from(raw: RawAutoOffsetReset) -> Self {
        match raw {
            RawAutoOffsetReset::Earliest => Self::Earliest,
            RawAutoOffsetReset::Latest => Self::Latest,
            RawAutoOffsetReset::Error => Self::Error,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawIngressConfig {
    work_queue_capacity: usize,
    completion_queue_capacity: usize,
    memory_budget_bytes: usize,
    max_payload_bytes: usize,
    backpressure: RawBackpressureConfig,
}

impl Default for RawIngressConfig {
    fn default() -> Self {
        Self {
            work_queue_capacity: DEFAULT_WORK_QUEUE_CAPACITY,
            completion_queue_capacity: DEFAULT_COMPLETION_QUEUE_CAPACITY,
            memory_budget_bytes: DEFAULT_MEMORY_BUDGET_BYTES,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            backpressure: RawBackpressureConfig::default(),
        }
    }
}

impl From<RawIngressConfig> for IngressConfig {
    fn from(raw: RawIngressConfig) -> Self {
        Self {
            work_queue_capacity: raw.work_queue_capacity,
            completion_queue_capacity: raw.completion_queue_capacity,
            memory_budget_bytes: raw.memory_budget_bytes,
            max_payload_bytes: raw.max_payload_bytes,
            backpressure: raw.backpressure.into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawBackpressureConfig {
    pause_high_watermark_percent: u8,
    resume_low_watermark_percent: u8,
}

impl Default for RawBackpressureConfig {
    fn default() -> Self {
        Self {
            pause_high_watermark_percent: DEFAULT_PAUSE_HIGH_WATERMARK_PERCENT,
            resume_low_watermark_percent: DEFAULT_RESUME_LOW_WATERMARK_PERCENT,
        }
    }
}

impl From<RawBackpressureConfig> for BackpressureConfig {
    fn from(raw: RawBackpressureConfig) -> Self {
        Self {
            pause_high_watermark_percent: raw.pause_high_watermark_percent,
            resume_low_watermark_percent: raw.resume_low_watermark_percent,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawObjectProcessingConfig {
    queue_capacity: usize,
    worker_count: usize,
    max_object_size: u64,
}

impl Default for RawObjectProcessingConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_OBJECT_QUEUE_CAPACITY,
            worker_count: DEFAULT_OBJECT_WORKER_COUNT,
            max_object_size: DEFAULT_MAX_OBJECT_SIZE,
        }
    }
}

impl From<RawObjectProcessingConfig> for ObjectProcessingConfig {
    fn from(raw: RawObjectProcessingConfig) -> Self {
        Self {
            queue_capacity: raw.queue_capacity,
            worker_count: raw.worker_count,
            max_object_size: raw.max_object_size,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawS3Config {
    region: Option<String>,
    endpoint_url: Option<String>,
    force_path_style: bool,
    max_attempts: u32,
    connect_timeout_ms: u64,
    operation_attempt_timeout_ms: u64,
    operation_timeout_ms: u64,
    stream_idle_timeout_ms: u64,
}

impl Default for RawS3Config {
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

impl From<RawS3Config> for S3Config {
    fn from(raw: RawS3Config) -> Self {
        Self {
            region: raw.region,
            endpoint_url: raw.endpoint_url,
            force_path_style: raw.force_path_style,
            max_attempts: raw.max_attempts,
            connect_timeout_ms: raw.connect_timeout_ms,
            operation_attempt_timeout_ms: raw.operation_attempt_timeout_ms,
            operation_timeout_ms: raw.operation_timeout_ms,
            stream_idle_timeout_ms: raw.stream_idle_timeout_ms,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawShutdownConfig {
    grace_ms: u64,
}

impl Default for RawShutdownConfig {
    fn default() -> Self {
        Self {
            grace_ms: DEFAULT_SHUTDOWN_GRACE_MS,
        }
    }
}

impl From<RawShutdownConfig> for ShutdownConfig {
    fn from(raw: RawShutdownConfig) -> Self {
        Self {
            grace_ms: raw.grace_ms,
        }
    }
}

fn default_client_id() -> String {
    DEFAULT_CLIENT_ID.to_owned()
}

const fn default_auto_commit_interval_ms() -> u64 {
    DEFAULT_AUTO_COMMIT_INTERVAL_MS
}

const fn default_session_timeout_ms() -> u64 {
    DEFAULT_SESSION_TIMEOUT_MS
}

const fn default_max_poll_interval_ms() -> u64 {
    DEFAULT_MAX_POLL_INTERVAL_MS
}

const fn default_prefetch_max_kbytes() -> u32 {
    DEFAULT_PREFETCH_MAX_KBYTES
}
