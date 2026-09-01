pub mod config;
pub mod ingress;
pub mod sink;

use std::sync::Arc;
use std::time::Duration;
use std::{future::Future, result::Result as StdResult};

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
///
/// # Errors
///
/// Returns an error when configuration, Kafka, downstream work, offset commit,
/// or graceful shutdown fails.
pub async fn run(config: AppConfig, shutdown: CancellationToken) -> Result<()> {
    run_with_sink(config, shutdown, run_discard_sink).await
}

/// Runs ingress with an injected downstream sink while preserving the same ACK contract.
///
/// This is primarily useful for correctness tests and future processor integration.
///
/// # Errors
///
/// Returns an error when configuration, Kafka, downstream work, offset commit,
/// or graceful shutdown fails.
pub async fn run_with_sink<S, SinkFuture>(
    config: AppConfig,
    shutdown: CancellationToken,
    sink_factory: S,
) -> Result<()>
where
    S: FnOnce(
            mpsc::Receiver<ingress::IngressRecord>,
            mpsc::Sender<ingress::Completion>,
        ) -> SinkFuture
        + Send
        + 'static,
    SinkFuture: Future<Output = StdResult<(), anyhow::Error>> + Send + 'static,
{
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

    let sink = tokio::spawn(sink_factory(work_rx, completion_tx));
    let runner = KafkaRunner::new(
        consumer,
        registry,
        rebalance_rx,
        work_tx,
        completion_rx,
        config.kafka.memory_budget_bytes,
        config.kafka.record_accounting_overhead_bytes,
        config.kafka.max_payload_bytes,
        config.kafka.pause_high_watermark_percent,
        config.kafka.resume_low_watermark_percent,
        Duration::from_millis(config.kafka.shutdown_grace_ms),
        shutdown,
    )?;

    let runner_result = runner.run().await;
    if runner_result.is_err() && !sink.is_finished() {
        sink.abort();
    }
    let sink_result = match sink.await {
        Ok(result) => result,
        Err(error) if error.is_cancelled() && runner_result.is_err() => Ok(()),
        Err(error) => Err(anyhow!("downstream sink task failed: {error}")),
    };

    combine_run_results(runner_result, sink_result)
}

fn combine_run_results(runner_result: Result<()>, sink_result: Result<()>) -> Result<()> {
    match (runner_result, sink_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(runner), Ok(())) => Err(runner),
        (Ok(()), Err(sink)) => Err(sink),
        (Err(runner), Err(sink)) => {
            Err(runner.context(format!("downstream also failed: {sink:#}")))
        }
    }
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
        .set("group.protocol", "classic")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("allow.auto.create.topics", "false")
        .set("isolation.level", "read_committed")
        .set(
            "queued.max.messages.kbytes",
            kafka.prefetch_max_kbytes.to_string(),
        )
        .set(
            "fetch.message.max.bytes",
            kafka.max_payload_bytes.to_string(),
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_failure_keeps_the_downstream_root_cause() {
        let result = combine_run_results(
            Err(anyhow!("runner observed a closed completion channel")),
            Err(anyhow!("downstream parser rejected the record")),
        );
        let report = match result {
            Ok(()) => String::new(),
            Err(error) => format!("{error:#}"),
        };

        assert!(report.contains("downstream parser rejected the record"));
        assert!(report.contains("runner observed a closed completion channel"));
    }
}
