use anyhow::{Context, Result};
use rdkafka::ClientConfig;

use crate::config::{IngressConfig, KafkaConfig};

use super::context::{KafkaContext, ManagedConsumer};

pub(crate) fn build_consumer(
    config: &KafkaConfig,
    ingress: &IngressConfig,
    context: KafkaContext,
) -> Result<ManagedConsumer> {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", config.bootstrap_servers.join(","))
        .set("group.id", &config.group_id)
        .set("client.id", &config.client_id)
        .set(
            "auto.offset.reset",
            config.auto_offset_reset.as_kafka_value(),
        )
        .set("enable.auto.offset.store", "false")
        .set("enable.auto.commit", "true")
        .set("group.protocol", "classic")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("allow.auto.create.topics", "false")
        .set("isolation.level", "read_committed")
        .set(
            "queued.max.messages.kbytes",
            config.prefetch_max_kbytes.to_string(),
        )
        .set(
            "fetch.message.max.bytes",
            ingress.max_payload_bytes.to_string(),
        )
        .set(
            "auto.commit.interval.ms",
            config.auto_commit_interval_ms.to_string(),
        )
        .set("session.timeout.ms", config.session_timeout_ms.to_string())
        .set(
            "max.poll.interval.ms",
            config.max_poll_interval_ms.to_string(),
        )
        .set("enable.partition.eof", "false");

    client
        .create_with_context(context)
        .context("failed to create Kafka consumer")
}
