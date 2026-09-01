use std::{future::Future, result::Result as StdResult, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use rdkafka::consumer::Consumer;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    ObjectMetadata,
    config::AppConfig,
    downstream::{run_discard_sink, run_object_sink},
    ingress::{
        AssignmentRegistry, Completion, IngressRecord, KafkaContext, KafkaRunner, RebalanceEvent,
        RunnerChannels, RunnerConfig, RunnerInputs, build_consumer,
    },
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

/// Runs ingress through the bounded object-processing pipeline with an injected processor.
///
/// Metadata decoding, worker distribution, and Kafka completion ownership are
/// handled by this function. The processor receives only validated object metadata.
///
/// # Errors
///
/// Returns an error when ingress, metadata decoding, object processing, or
/// completion delivery fails.
pub async fn run_with_object_processor<Processor, ProcessorFuture>(
    config: AppConfig,
    shutdown: CancellationToken,
    processor: Processor,
) -> Result<()>
where
    Processor: Fn(ObjectMetadata) -> ProcessorFuture + Send + Sync + 'static,
    ProcessorFuture: Future<Output = Result<()>> + Send + 'static,
{
    let object_processing = config.object_processing;
    run_with_sink(config, shutdown, move |records, completions| {
        run_object_sink(records, completions, object_processing, processor)
    })
    .await
}

/// Runs ingress with an injected downstream sink while preserving the same ACK contract.
///
/// This is the downstream integration boundary used by correctness tests and
/// future processors.
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
    S: FnOnce(mpsc::Receiver<IngressRecord>, mpsc::Sender<Completion>) -> SinkFuture
        + Send
        + 'static,
    SinkFuture: Future<Output = StdResult<(), anyhow::Error>> + Send + 'static,
{
    config.validate()?;

    let (work_tx, work_rx) = mpsc::channel(config.ingress.work_queue_capacity);
    let (completion_tx, completion_rx) = mpsc::channel(config.ingress.completion_queue_capacity);
    let (rebalance_tx, rebalance_rx) = mpsc::unbounded_channel::<RebalanceEvent>();

    let registry = Arc::new(AssignmentRegistry::default());
    let context = KafkaContext::new(Arc::clone(&registry), rebalance_tx);
    let consumer = build_consumer(&config.kafka, &config.ingress, context)?;
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
    let runner = KafkaRunner::new(RunnerInputs {
        consumer,
        registry,
        channels: RunnerChannels {
            rebalance_rx,
            work_tx,
            completion_rx,
        },
        config: RunnerConfig {
            memory_budget_bytes: config.ingress.memory_budget_bytes,
            max_payload_bytes: config.ingress.max_payload_bytes,
            pause_high_watermark_percent: config.ingress.backpressure.pause_high_watermark_percent,
            resume_low_watermark_percent: config.ingress.backpressure.resume_low_watermark_percent,
            shutdown_grace: Duration::from_millis(config.shutdown.grace_ms),
        },
        shutdown,
    })?;

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
