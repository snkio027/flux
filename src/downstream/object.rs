use std::{future::Future, sync::Arc};

use anyhow::{Context, Result, anyhow};
use flume::{Receiver, Sender};
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    Completion, IngressRecord, ObjectMetadata,
    config::ObjectProcessingConfig,
    metadata::{MetadataDecodeFailure, ObjectWorkItem, decode_record},
};

pub(crate) async fn run_object_sink<Processor, ProcessorFuture>(
    records: mpsc::Receiver<IngressRecord>,
    completions: mpsc::Sender<Completion>,
    config: ObjectProcessingConfig,
    processor: Processor,
) -> Result<()>
where
    Processor: Fn(ObjectMetadata) -> ProcessorFuture + Send + Sync + 'static,
    ProcessorFuture: Future<Output = Result<()>> + Send + 'static,
{
    let (work_tx, work_rx) = flume::bounded(config.queue_capacity);
    let processor = Arc::new(processor);
    let mut tasks = JoinSet::new();

    tasks.spawn(dispatch_records(
        records,
        completions.clone(),
        work_tx,
        config.max_object_size,
    ));
    for worker_index in 0..config.worker_count {
        let worker_rx = work_rx.clone();
        let worker_completions = completions.clone();
        let worker_processor = Arc::clone(&processor);
        tasks.spawn(async move {
            run_worker(worker_rx, worker_completions, worker_processor)
                .await
                .with_context(|| format!("object worker {worker_index} failed"))
        });
    }
    drop(work_rx);
    drop(completions);

    while let Some(task_result) = tasks.join_next().await {
        match task_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tasks.abort_all();
                return Err(error);
            }
            Err(error) => {
                tasks.abort_all();
                return Err(anyhow!("object pipeline task failed: {error}"));
            }
        }
    }

    Ok(())
}

async fn dispatch_records(
    mut records: mpsc::Receiver<IngressRecord>,
    completions: mpsc::Sender<Completion>,
    work_tx: Sender<ObjectWorkItem>,
    max_object_size: u64,
) -> Result<()> {
    while let Some(source) = records.recv().await {
        let source_location = source_location(&source);
        let work = match decode_record(source, max_object_size) {
            Ok(work) => work,
            Err(failure) => {
                return reject_decode_failure(&completions, failure, &source_location).await;
            }
        };

        if let Err(error) = work_tx.send_async(work).await {
            let (_metadata, source) = error.into_inner().into_parts();
            let reason = "object work queue closed before dispatch";
            completions
                .send(source.fail(reason))
                .await
                .context("failed to report closed object work queue")?;
            return Err(anyhow!("{reason} for {source_location}"));
        }
    }

    Ok(())
}

async fn reject_decode_failure(
    completions: &mpsc::Sender<Completion>,
    failure: Box<MetadataDecodeFailure>,
    source_location: &str,
) -> Result<()> {
    let (source, error) = failure.into_parts();
    let reason = error.to_string();
    if let Err(completion_error) = completions.send(source.fail(reason)).await {
        return Err(error).context(format!(
            "metadata decode failed for {source_location}; failed completion could not be delivered: {completion_error}"
        ));
    }
    Err(error).with_context(|| format!("metadata decode failed for {source_location}"))
}

async fn run_worker<Processor, ProcessorFuture>(
    work_rx: Receiver<ObjectWorkItem>,
    completions: mpsc::Sender<Completion>,
    processor: Arc<Processor>,
) -> Result<()>
where
    Processor: Fn(ObjectMetadata) -> ProcessorFuture + Send + Sync + 'static,
    ProcessorFuture: Future<Output = Result<()>> + Send + 'static,
{
    while let Ok(work) = work_rx.recv_async().await {
        let (metadata, source) = work.into_parts();
        let object_location = format!("s3://{}/{}", metadata.bucket(), metadata.key());
        match processor(metadata).await {
            Ok(()) => {
                completions.send(source.succeed()).await.with_context(|| {
                    format!("failed to report successful processing for {object_location}")
                })?;
            }
            Err(error) => {
                let reason = format!("{error:#}");
                if let Err(completion_error) = completions.send(source.fail(reason)).await {
                    return Err(error).context(format!(
                        "processing failed for {object_location}; failed completion could not be delivered: {completion_error}"
                    ));
                }
                return Err(error)
                    .with_context(|| format!("processing failed for {object_location}"));
            }
        }
    }

    Ok(())
}

fn source_location(source: &IngressRecord) -> String {
    format!(
        "{}[{}] offset {}",
        source.topic(),
        source.partition(),
        source.offset()
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use tokio::sync::Semaphore;

    use crate::{
        config::RECORD_ACCOUNTING_OVERHEAD_BYTES,
        ingress::{CompletionOutcome, DeliveryToken, PendingRecord, TopicPartition},
    };

    use super::*;

    fn config(worker_count: usize) -> ObjectProcessingConfig {
        ObjectProcessingConfig {
            queue_capacity: 1,
            worker_count,
            max_object_size: 1024,
        }
    }

    fn record(offset: i64, payload: &[u8]) -> IngressRecord {
        let accounted_bytes =
            u32::try_from(RECORD_ACCOUNTING_OVERHEAD_BYTES + payload.len()).unwrap();
        let memory_budget = Arc::new(Semaphore::new(accounted_bytes as usize));
        let memory_permit = memory_budget
            .try_acquire_many_owned(accounted_bytes)
            .unwrap();

        IngressRecord::from_pending(
            PendingRecord {
                token: DeliveryToken {
                    topic_partition: TopicPartition::new("test-topic", 0),
                    record_offset: offset,
                    assignment_epoch: 1,
                },
                key: None,
                payload: Some(payload.into()),
                headers: Vec::new(),
                timestamp_millis: None,
                accounted_bytes,
            },
            memory_permit,
        )
    }

    #[tokio::test]
    async fn bounds_processing_concurrency_and_completes_every_record() {
        let (record_tx, record_rx) = mpsc::channel(8);
        let (completion_tx, mut completion_rx) = mpsc::channel(8);
        for offset in 0..6 {
            record_tx
                .send(record(
                    offset,
                    br#"{"bucket":"signals","key":"one.gz","size":1}"#,
                ))
                .await
                .unwrap();
        }
        drop(record_tx);

        let in_flight = Arc::new(AtomicUsize::new(0));
        let maximum_in_flight = Arc::new(AtomicUsize::new(0));
        let processor = {
            let in_flight = Arc::clone(&in_flight);
            let maximum_in_flight = Arc::clone(&maximum_in_flight);
            move |_metadata: ObjectMetadata| {
                let in_flight = Arc::clone(&in_flight);
                let maximum_in_flight = Arc::clone(&maximum_in_flight);
                async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum_in_flight.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        };

        run_object_sink(record_rx, completion_tx, config(2), processor)
            .await
            .unwrap();

        let mut successes = 0;
        while let Some(completion) = completion_rx.recv().await {
            let (_token, outcome) = completion.into_parts();
            assert!(matches!(outcome, CompletionOutcome::Succeeded));
            successes += 1;
        }
        assert_eq!(successes, 6);
        assert_eq!(maximum_in_flight.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn metadata_failure_is_reported_without_calling_the_processor() {
        let (record_tx, record_rx) = mpsc::channel(1);
        let (completion_tx, mut completion_rx) = mpsc::channel(1);
        record_tx.send(record(3, b"not-json")).await.unwrap();
        drop(record_tx);
        let calls = Arc::new(AtomicUsize::new(0));
        let processor = {
            let calls = Arc::clone(&calls);
            move |_metadata: ObjectMetadata| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }
        };

        let error = run_object_sink(record_rx, completion_tx, config(1), processor)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("metadata decode failed"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let (_token, outcome) = completion_rx.recv().await.unwrap().into_parts();
        assert!(
            matches!(outcome, CompletionOutcome::Failed(reason) if reason.contains("valid JSON"))
        );
    }

    #[tokio::test]
    async fn processor_failure_is_reported_as_a_failed_completion() {
        let (record_tx, record_rx) = mpsc::channel(1);
        let (completion_tx, mut completion_rx) = mpsc::channel(1);
        record_tx
            .send(record(
                4,
                br#"{"bucket":"signals","key":"broken.gz","size":1}"#,
            ))
            .await
            .unwrap();
        drop(record_tx);

        let error = run_object_sink(record_rx, completion_tx, config(1), |_metadata| async {
            Err(anyhow!("synthetic object failure"))
        })
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("synthetic object failure"));
        let (_token, outcome) = completion_rx.recv().await.unwrap().into_parts();
        assert!(
            matches!(outcome, CompletionOutcome::Failed(reason) if reason.contains("synthetic object failure"))
        );
    }
}
