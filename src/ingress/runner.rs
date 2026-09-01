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
    AckEffect, AssignmentRegistry, Completion, IngressRecord, OffsetSnapshot, OffsetTracker,
    RebalanceEvent, TopicPartition, model::PendingRecord,
};

use super::context::ManagedConsumer;

enum DispatchOutcome {
    Queued,
    Stale,
    Shutdown,
}

#[derive(Clone, Copy, Debug)]
struct BackpressurePolicy {
    pause_high_percent: u8,
    resume_low_percent: u8,
}

#[derive(Clone, Copy, Debug)]
struct BudgetUsage {
    queue_used: usize,
    queue_total: usize,
    bytes_used: usize,
    bytes_total: usize,
}

impl BackpressurePolicy {
    fn should_pause(self, usage: BudgetUsage) -> bool {
        reached_percent(usage.queue_used, usage.queue_total, self.pause_high_percent)
            || reached_percent(usage.bytes_used, usage.bytes_total, self.pause_high_percent)
    }

    fn should_resume(self, usage: BudgetUsage) -> bool {
        at_or_below_percent(usage.queue_used, usage.queue_total, self.resume_low_percent)
            && at_or_below_percent(usage.bytes_used, usage.bytes_total, self.resume_low_percent)
    }
}

pub struct KafkaRunner {
    consumer: ManagedConsumer,
    registry: Arc<AssignmentRegistry>,
    rebalance_rx: mpsc::UnboundedReceiver<RebalanceEvent>,
    work_tx: Option<mpsc::Sender<IngressRecord>>,
    completion_rx: mpsc::Receiver<Completion>,
    memory_budget: Arc<Semaphore>,
    memory_budget_bytes: u32,
    record_accounting_overhead_bytes: usize,
    max_payload_bytes: usize,
    backpressure: BackpressurePolicy,
    shutdown_grace: Duration,
    shutdown: CancellationToken,
    tracker: OffsetTracker,
    paused: bool,
}

impl KafkaRunner {
    #[allow(clippy::too_many_arguments)]
    /// Builds a runner after converting the configured byte budget to semaphore permits.
    ///
    /// # Errors
    ///
    /// Returns an error when the byte budget is outside Tokio's permit range.
    pub fn new(
        consumer: ManagedConsumer,
        registry: Arc<AssignmentRegistry>,
        rebalance_rx: mpsc::UnboundedReceiver<RebalanceEvent>,
        work_tx: mpsc::Sender<IngressRecord>,
        completion_rx: mpsc::Receiver<Completion>,
        memory_budget_bytes: usize,
        record_accounting_overhead_bytes: usize,
        max_payload_bytes: usize,
        pause_high_watermark_percent: u8,
        resume_low_watermark_percent: u8,
        shutdown_grace: Duration,
        shutdown: CancellationToken,
    ) -> Result<Self> {
        let memory_budget_bytes = u32::try_from(memory_budget_bytes)
            .context("Kafka memory budget exceeds the semaphore permit range")?;
        Ok(Self {
            consumer,
            registry,
            rebalance_rx,
            work_tx: Some(work_tx),
            completion_rx,
            memory_budget: Arc::new(Semaphore::new(memory_budget_bytes as usize)),
            memory_budget_bytes,
            record_accounting_overhead_bytes,
            max_payload_bytes,
            backpressure: BackpressurePolicy {
                pause_high_percent: pause_high_watermark_percent,
                resume_low_percent: resume_low_watermark_percent,
            },
            shutdown_grace,
            shutdown,
            tracker: OffsetTracker::default(),
            paused: false,
        })
    }

    /// Consumes until cancellation or failure, then drains and commits the safe prefix.
    ///
    /// # Errors
    ///
    /// Returns an error for Kafka, downstream, backpressure, commit, or shutdown failures.
    pub async fn run(mut self) -> Result<()> {
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
                    let pending = PendingRecord::from_message(
                        &message,
                        assignment_epoch,
                        self.record_accounting_overhead_bytes,
                    )?;
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
        match completion {
            Completion::Succeeded(token) => {
                let effect = self.tracker.on_success(&token)?;
                if let AckEffect::StoreNext(snapshot) = effect {
                    // `store_offset()` is the legacy singular API and stores
                    // its argument + 1. `safe_next_offset` is already a next
                    // offset, so the exact-value `store_offsets()` API is
                    // required here.
                    let offsets = topic_partition_list(std::slice::from_ref(&snapshot))?;
                    let store_result = self.registry.with_current(
                        &snapshot.topic_partition,
                        snapshot.assignment_epoch,
                        || self.consumer.store_offsets(&offsets),
                    );
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
            Completion::Failed { token, reason } => bail!(
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
        let usage = BudgetUsage {
            queue_used: work_tx.max_capacity() - work_tx.capacity(),
            queue_total: work_tx.max_capacity(),
            bytes_used: self.memory_budget_bytes as usize - self.memory_budget.available_permits(),
            bytes_total: self.memory_budget_bytes as usize,
        };

        if self.paused && self.backpressure.should_resume(usage) {
            debug!(
                queue_used = usage.queue_used,
                queue_total = usage.queue_total,
                bytes_used = usage.bytes_used,
                bytes_total = usage.bytes_total,
                "resuming Kafka after low watermark"
            );
            self.resume_current_assignment()?;
        } else if !self.paused && self.backpressure.should_pause(usage) {
            debug!(
                queue_used = usage.queue_used,
                queue_total = usage.queue_total,
                bytes_used = usage.bytes_used,
                bytes_total = usage.bytes_total,
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

fn reached_percent(used: usize, total: usize, percent: u8) -> bool {
    (used as u128) * 100 >= (total as u128) * u128::from(percent)
}

fn at_or_below_percent(used: usize, total: usize, percent: u8) -> bool {
    (used as u128) * 100 <= (total as u128) * u128::from(percent)
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

    #[test]
    fn backpressure_uses_high_low_hysteresis_across_both_budgets() {
        let policy = BackpressurePolicy {
            pause_high_percent: 80,
            resume_low_percent: 50,
        };

        assert!(policy.should_pause(BudgetUsage {
            queue_used: 8,
            queue_total: 10,
            bytes_used: 1,
            bytes_total: 10,
        }));
        assert!(policy.should_pause(BudgetUsage {
            queue_used: 1,
            queue_total: 10,
            bytes_used: 8,
            bytes_total: 10,
        }));
        assert!(!policy.should_resume(BudgetUsage {
            queue_used: 5,
            queue_total: 10,
            bytes_used: 6,
            bytes_total: 10,
        }));
        assert!(policy.should_resume(BudgetUsage {
            queue_used: 5,
            queue_total: 10,
            bytes_used: 5,
            bytes_total: 10,
        }));
    }

    #[tokio::test]
    async fn shutdown_deadline_expires_for_stuck_work() {
        assert!(!completed_before_deadline(Duration::from_millis(1), pending::<()>()).await);
    }
}
