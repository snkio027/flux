pub mod config;
pub mod ingress;
pub mod sink;

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rdkafka::{ClientConfig, consumer::Consumer};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    config::AppConfig,
    ingress::{AssignmentRegistry, KafkaContext, KafkaRunner, ManagedConsumer, RebalanceEvent},
    sink::run_discard_sink,
};

/// Runs the v1 ingress until shutdown or a fail-closed processing error.
pub async fn run(config: AppConfig, shutdown: CancellationToken) -> Result<()> {
    config.validate()?;

    let (work_tx, work_rx) = mpsc::channel(config.kafka.work_queue_capacity);
    let (completion_tx, completion_rx) = mpsc::channel(config.kafka.completion_queue_capacity);
    let (rebalance_tx, rebalance_rx) = mpsc::unbounded_channel::<RebalanceEvent>();

    let registry = Arc::new(AssignmentRegistry::default());
    let context = KafkaContext::new(Arc::clone(&registry), rebalance_tx);
    let consumer: ManagedConsumer = build_consumer(&config, context)?;
    let topics = config
        .kafka
        .topics
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    consumer
        .subscribe(&topics)
        .context("failed to subscribe to Kafka topics")?;

    let sink = tokio::spawn(run_discard_sink(work_rx, completion_tx));
    let runner = KafkaRunner::new(
        consumer,
        registry,
        rebalance_rx,
        work_tx,
        completion_rx,
        config.kafka.memory_budget_bytes,
        config.kafka.record_accounting_overhead_bytes,
        shutdown,
    );

    let runner_result = runner.run().await;
    let sink_result = sink
        .await
        .map_err(|error| anyhow!("discard sink task failed: {error}"))?;

    runner_result?;
    sink_result
}

fn build_consumer(config: &AppConfig, context: KafkaContext) -> Result<ManagedConsumer> {
    let kafka = &config.kafka;
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", kafka.bootstrap_servers.join(","))
        .set("group.id", &kafka.group_id)
        .set("client.id", &kafka.client_id)
        .set(
            "auto.offset.reset",
            kafka.auto_offset_reset.as_kafka_value(),
        )
        .set("enable.auto.offset.store", "false")
        .set("enable.auto.commit", "true")
        .set(
            "auto.commit.interval.ms",
            kafka.auto_commit_interval_ms.to_string(),
        )
        .set("session.timeout.ms", kafka.session_timeout_ms.to_string())
        .set(
            "max.poll.interval.ms",
            kafka.max_poll_interval_ms.to_string(),
        )
        .set("enable.partition.eof", "false");

    client
        .create_with_context(context)
        .context("failed to create Kafka consumer")
}
