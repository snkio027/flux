use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer},
    topic_partition_list::{Offset, TopicPartitionList},
};
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore, mpsc, mpsc::OwnedPermit, mpsc::error::TrySendError,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{
    AckEffect, AssignmentRegistry, Completion, CompletionOutcome, IngressRecord, OffsetSnapshot,
    OffsetTracker, PendingRecord, RebalanceEvent, TopicPartition,
    backpressure::{BackpressurePolicy, BudgetUsage},
};

use super::context::ManagedConsumer;

enum DispatchOutcome {
    Queued,
    Stale,
    Shutdown,
}

pub(crate) struct RunnerChannels {
    pub(crate) rebalance_rx: mpsc::UnboundedReceiver<RebalanceEvent>,
    pub(crate) work_tx: mpsc::Sender<IngressRecord>,
    pub(crate) completion_rx: mpsc::Receiver<Completion>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RunnerConfig {
    pub(crate) memory_budget_bytes: usize,
    pub(crate) max_payload_bytes: usize,
    pub(crate) pause_high_watermark_percent: u8,
    pub(crate) resume_low_watermark_percent: u8,
    pub(crate) shutdown_grace: Duration,
}

pub(crate) struct RunnerInputs {
    pub(crate) consumer: ManagedConsumer,
    pub(crate) registry: Arc<AssignmentRegistry>,
    pub(crate) channels: RunnerChannels,
    pub(crate) config: RunnerConfig,
    pub(crate) shutdown: CancellationToken,
}

pub(crate) struct KafkaRunner {
    consumer: ManagedConsumer,
    registry: Arc<AssignmentRegistry>,
    rebalance_rx: mpsc::UnboundedReceiver<RebalanceEvent>,
    work_tx: Option<mpsc::Sender<IngressRecord>>,
    completion_rx: mpsc::Receiver<Completion>,
    memory_budget: Arc<Semaphore>,
    memory_budget_bytes: u32,
    max_payload_bytes: usize,
    backpressure: BackpressurePolicy,
    shutdown_grace: Duration,
    shutdown: CancellationToken,
    tracker: OffsetTracker,
    paused: bool,
}

impl KafkaRunner {
    /// Builds a runner after converting the configured byte budget to semaphore permits.
    ///
    /// # Errors
    ///
    /// Returns an error when the byte budget is outside Tokio's permit range.
    pub(crate) fn new(inputs: RunnerInputs) -> Result<Self> {
        let memory_budget_bytes = u32::try_from(inputs.config.memory_budget_bytes)
            .context("Kafka memory budget exceeds the semaphore permit range")?;
        let channels = inputs.channels;
        Ok(Self {
            consumer: inputs.consumer,
            registry: inputs.registry,
            rebalance_rx: channels.rebalance_rx,
            work_tx: Some(channels.work_tx),
            completion_rx: channels.completion_rx,
            memory_budget: Arc::new(Semaphore::new(memory_budget_bytes as usize)),
            memory_budget_bytes,
            max_payload_bytes: inputs.config.max_payload_bytes,
            backpressure: BackpressurePolicy::new(
                inputs.config.pause_high_watermark_percent,
                inputs.config.resume_low_watermark_percent,
            ),
            shutdown_grace: inputs.config.shutdown_grace,
            shutdown: inputs.shutdown,
            tracker: OffsetTracker::default(),
            paused: false,
        })
    }

    /// Consumes until cancellation or failure, then drains and commits the safe prefix.
    ///
    /// # Errors
    ///
    /// Returns an error for Kafka, downstream, backpressure, commit, or shutdown failures.
    pub(crate) async fn run(mut self) -> Result<()> {
        let consume_error = self.consume_until_stop().await.err();
        let shutdown_error = self.drain_and_commit().await.err();

        match (consume_error, shutdown_error) {
            (None, None) => Ok(()),
            (Some(error), None) | (None, Some(error)) => Err(error),
            (Some(consume), Some(shutdown)) => Err(consume.context(format!(
                "shutdown also failed after ingress stopped: {shutdown:#}"
            ))),
        }
    }

    async fn consume_until_stop(&mut self) -> Result<()> {
        loop {
            self.sync_backpressure_state()?;
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    info!("Kafka ingress stopping");
                    return Ok(());
                }
                event = self.rebalance_rx.recv() => {
                    let event = event.ok_or_else(|| anyhow!("rebalance event channel closed"))?;
                    self.handle_rebalance(event)?;
                }
                completion = self.completion_rx.recv() => {
                    let completion = completion.ok_or_else(|| {
                        anyhow!("completion channel closed while ingress was accepting records")
                    })?;
                    self.handle_completion(completion)?;
                }
                () = tokio::time::sleep(Duration::from_millis(10)), if self.paused => {}
                message = self.consumer.recv() => {
                    let message = message.context("Kafka receive failed")?;
                    if self.paused {
                        bail!(
                            "Kafka delivered {}[{}] offset {} while assignments were paused for backpressure",
                            message.topic(),
                            message.partition(),
                            message.offset(),
                        );
                    }
                    let payload_bytes = message.payload().map_or(0, <[u8]>::len);
                    if payload_bytes > self.max_payload_bytes {
                        bail!(
                            "record {}[{}] offset {} has a {} byte payload, exceeding max_payload_bytes={}",
                            message.topic(),
                            message.partition(),
                            message.offset(),
                            payload_bytes,
                            self.max_payload_bytes,
                        );
                    }
                    let topic_partition =
                        TopicPartition::new(message.topic(), message.partition());
                    let Some(assignment_epoch) = self.registry.current_epoch(&topic_partition) else {
                        debug!(%topic_partition, offset = message.offset(), "discarding record outside a current assignment");
                        continue;
                    };
                    self.tracker
                        .ensure_assigned(topic_partition, assignment_epoch);
                    let pending = PendingRecord::from_message(&message, assignment_epoch)?;
                    drop(message);

                    match self.dispatch(pending).await? {
                        DispatchOutcome::Queued | DispatchOutcome::Stale => {}
                        DispatchOutcome::Shutdown => return Ok(()),
                    }
                }
            }
        }
    }

    async fn dispatch(&mut self, pending: PendingRecord) -> Result<DispatchOutcome> {
        if pending.accounted_bytes > self.memory_budget_bytes {
            bail!(
                "record {} offset {} accounts for {} bytes, exceeding the {} byte ingress budget",
                pending.token.topic_partition,
                pending.token.record_offset,
                pending.accounted_bytes,
                self.memory_budget_bytes,
            );
        }

        let work_tx = self
            .work_tx
            .as_ref()
            .ok_or_else(|| anyhow!("work queue is already closed"))?
            .clone();
        match work_tx.clone().try_reserve_owned() {
            Ok(queue_permit) => match Arc::clone(&self.memory_budget)
                .try_acquire_many_owned(pending.accounted_bytes)
            {
                Ok(memory_permit) => {
                    let outcome = self.finish_dispatch(pending, queue_permit, memory_permit)?;
                    self.sync_backpressure_state()?;
                    return Ok(outcome);
                }
                Err(_) => drop(queue_permit),
            },
            Err(TrySendError::Closed(_)) => bail!("work queue closed unexpectedly"),
            Err(TrySendError::Full(_)) => {}
        }

        self.pause_current_assignment()?;
        let byte_count = pending.accounted_bytes;
        let memory_budget = Arc::clone(&self.memory_budget);
        let capacities = async move {
            let queue_permit = work_tx
                .reserve_owned()
                .await
                .map_err(|_| anyhow!("work queue closed unexpectedly"))?;
            let memory_permit = memory_budget
                .acquire_many_owned(byte_count)
                .await
                .map_err(|_| anyhow!("memory budget closed unexpectedly"))?;
            Ok::<_, anyhow::Error>((queue_permit, memory_permit))
        };
        tokio::pin!(capacities);

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    return Ok(DispatchOutcome::Shutdown);
                }
                acquired = &mut capacities => {
                    let (queue_permit, memory_permit) = acquired?;
                    let outcome = self.finish_dispatch(pending, queue_permit, memory_permit)?;
                    self.sync_backpressure_state()?;
                    return Ok(outcome);
                }
                event = self.rebalance_rx.recv() => {
                    let event = event.ok_or_else(|| anyhow!("rebalance event channel closed"))?;
                    self.handle_rebalance(event)?;
                    if self.registry.current_epoch(&pending.token.topic_partition)
                        != Some(pending.token.assignment_epoch)
                    {
                        self.sync_backpressure_state()?;
                        return Ok(DispatchOutcome::Stale);
                    }
                }
                completion = self.completion_rx.recv() => {
                    let completion = completion.ok_or_else(|| {
                        anyhow!("completion channel closed while waiting for ingress capacity")
                    })?;
                    self.handle_completion(completion)?;
                }
                message = self.consumer.recv() => {
                    match message {
                        Ok(message) => bail!(
                            "Kafka delivered {}[{}] offset {} while all current assignments were paused for backpressure",
                            message.topic(),
                            message.partition(),
                            message.offset(),
                        ),
                        Err(error) => return Err(error).context("Kafka polling failed during backpressure"),
                    }
                }
            }
        }
    }

    fn finish_dispatch(
        &mut self,
        pending: PendingRecord,
        queue_permit: OwnedPermit<IngressRecord>,
        memory_permit: OwnedSemaphorePermit,
    ) -> Result<DispatchOutcome> {
        if self.registry.current_epoch(&pending.token.topic_partition)
            != Some(pending.token.assignment_epoch)
        {
            return Ok(DispatchOutcome::Stale);
        }

        self.tracker.on_delivered(&pending.token)?;
        queue_permit.send(IngressRecord::from_pending(pending, memory_permit));
        Ok(DispatchOutcome::Queued)
    }

    fn handle_completion(&mut self, completion: Completion) -> Result<()> {
        let (token, outcome) = completion.into_parts();
        match outcome {
            CompletionOutcome::Succeeded => {
                let effect = self.tracker.on_success(&token)?;
                if let AckEffect::StoreNext(snapshot) = effect {
                    // `store_offset()` is the legacy singular API and stores
                    // its argument + 1. `safe_next_offset` is already a next
                    // offset, so the exact-value `store_offsets()` API is
                    // required here.
                    let offsets = topic_partition_list(std::slice::from_ref(&snapshot))?;
                    let store_result = self
                        .registry
                        .store_if_current(&snapshot, || self.consumer.store_offsets(&offsets));
                    if let Some(result) = store_result {
                        result.with_context(|| {
                            format!(
                                "failed to store safe next offset {} for {}",
                                snapshot.safe_next_offset, snapshot.topic_partition
                            )
                        })?;
                    } else {
                        debug!(
                            topic_partition = %snapshot.topic_partition,
                            epoch = snapshot.assignment_epoch,
                            "safe prefix became stale before local offset store"
                        );
                    }
                }
                Ok(())
            }
            CompletionOutcome::Failed(reason) => bail!(
                "downstream processing failed for {} offset {}: {}",
                token.topic_partition,
                token.record_offset,
                reason,
            ),
        }
    }

    fn handle_rebalance(&mut self, event: RebalanceEvent) -> Result<()> {
        match event {
            RebalanceEvent::Assigned(assignments) => {
                for assignment in assignments {
                    if self.registry.is_current(&assignment) {
                        self.tracker.ensure_assigned(
                            assignment.topic_partition,
                            assignment.assignment_epoch,
                        );
                    }
                }
                if self.paused {
                    self.pause_current_assignment()?;
                }
                Ok(())
            }
            RebalanceEvent::Revoked(assignments) => {
                for assignment in assignments {
                    self.tracker
                        .revoke(&assignment.topic_partition, assignment.assignment_epoch);
                }
                Ok(())
            }
            RebalanceEvent::Error(error) => bail!("Kafka rebalance failed: {error}"),
        }
    }

    fn pause_current_assignment(&mut self) -> Result<()> {
        let assignment = self
            .consumer
            .assignment()
            .context("failed to read Kafka assignment before pause")?;
        self.consumer
            .pause(&assignment)
            .context("failed to pause Kafka assignment")?;
        self.paused = true;
        Ok(())
    }

    fn resume_current_assignment(&mut self) -> Result<()> {
        if !self.paused {
            return Ok(());
        }
        let assignment = self
            .consumer
            .assignment()
            .context("failed to read Kafka assignment before resume")?;
        self.consumer
            .resume(&assignment)
            .context("failed to resume Kafka assignment")?;
        self.paused = false;
        Ok(())
    }

    fn sync_backpressure_state(&mut self) -> Result<()> {
        let Some(work_tx) = self.work_tx.as_ref() else {
            return Ok(());
        };
        let usage = BudgetUsage::new(
            work_tx.max_capacity() - work_tx.capacity(),
            work_tx.max_capacity(),
            self.memory_budget_bytes as usize - self.memory_budget.available_permits(),
            self.memory_budget_bytes as usize,
        );

        if self.paused && self.backpressure.should_resume(usage) {
            debug!(
                queue_used = usage.queue_used(),
                queue_total = usage.queue_total(),
                bytes_used = usage.bytes_used(),
                bytes_total = usage.bytes_total(),
                "resuming Kafka after low watermark"
            );
            self.resume_current_assignment()?;
        } else if !self.paused && self.backpressure.should_pause(usage) {
            debug!(
                queue_used = usage.queue_used(),
                queue_total = usage.queue_total(),
                bytes_used = usage.bytes_used(),
                bytes_total = usage.bytes_total(),
                "pausing Kafka at high watermark"
            );
            self.pause_current_assignment()?;
        }
        Ok(())
    }

    async fn drain_and_commit(&mut self) -> Result<()> {
        let mut first_error = self.pause_current_assignment().err();
        drop(self.work_tx.take());

        if !completed_before_deadline(
            self.shutdown_grace,
            self.drain_until_completions_close(&mut first_error),
        )
        .await
            && first_error.is_none()
        {
            first_error = Some(anyhow!(
                "Kafka shutdown drain exceeded {} ms; incomplete records will replay",
                self.shutdown_grace.as_millis()
            ));
        }

        if let Err(error) = self.commit_safe_snapshot()
            && first_error.is_none()
        {
            first_error = Some(error);
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn drain_until_completions_close(&mut self, first_error: &mut Option<anyhow::Error>) {
        loop {
            tokio::select! {
                completion = self.completion_rx.recv() => {
                    let Some(completion) = completion else { break };
                    if let Err(error) = self.handle_completion(completion)
                        && first_error.is_none()
                    {
                        *first_error = Some(error);
                    }
                }
                event = self.rebalance_rx.recv() => {
                    let Some(event) = event else { continue };
                    if let Err(error) = self.handle_rebalance(event)
                        && first_error.is_none()
                    {
                        *first_error = Some(error);
                    }
                }
                message = self.consumer.recv() => {
                    match message {
                        Ok(message) => warn!(
                            topic = message.topic(),
                            partition = message.partition(),
                            offset = message.offset(),
                            "discarding Kafka record received while shutting down"
                        ),
                        Err(error) => warn!(%error, "Kafka polling error while shutting down"),
                    }
                }
            }
        }
    }

    fn commit_safe_snapshot(&self) -> Result<()> {
        let snapshot = self
            .tracker
            .safe_snapshot()
            .into_iter()
            .filter(|offset| {
                self.registry.current_epoch(&offset.topic_partition)
                    == Some(offset.assignment_epoch)
            })
            .collect::<Vec<_>>();
        if snapshot.is_empty() {
            debug!("no safe Kafka offsets to commit during shutdown");
            return Ok(());
        }

        let offsets = topic_partition_list(&snapshot)?;
        self.consumer
            .store_offsets(&offsets)
            .context("failed to store final safe Kafka offset snapshot")?;
        self.consumer
            .commit(&offsets, CommitMode::Sync)
            .context("failed to synchronously commit final safe Kafka offset snapshot")?;
        info!(count = snapshot.len(), "committed final safe Kafka offsets");
        Ok(())
    }
}

async fn completed_before_deadline(deadline: Duration, future: impl Future<Output = ()>) -> bool {
    tokio::time::timeout(deadline, future).await.is_ok()
}

fn topic_partition_list(snapshot: &[OffsetSnapshot]) -> Result<TopicPartitionList> {
    let mut offsets = TopicPartitionList::with_capacity(snapshot.len());
    for item in snapshot {
        offsets
            .add_partition_offset(
                &item.topic_partition.topic,
                item.topic_partition.partition,
                Offset::Offset(item.safe_next_offset),
            )
            .with_context(|| {
                format!(
                    "failed to build offset snapshot for {}",
                    item.topic_partition
                )
            })?;
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[tokio::test]
    async fn shutdown_deadline_expires_for_stuck_work() {
        assert!(!completed_before_deadline(Duration::from_millis(1), pending::<()>()).await);
    }
}
