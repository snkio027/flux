use std::sync::Arc;

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

pub struct KafkaRunner {
    consumer: ManagedConsumer,
    registry: Arc<AssignmentRegistry>,
    rebalance_rx: mpsc::UnboundedReceiver<RebalanceEvent>,
    work_tx: Option<mpsc::Sender<IngressRecord>>,
    completion_rx: mpsc::Receiver<Completion>,
    memory_budget: Arc<Semaphore>,
    memory_budget_bytes: u32,
    record_accounting_overhead_bytes: usize,
    shutdown: CancellationToken,
    tracker: OffsetTracker,
    paused: bool,
}

impl KafkaRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        consumer: ManagedConsumer,
        registry: Arc<AssignmentRegistry>,
        rebalance_rx: mpsc::UnboundedReceiver<RebalanceEvent>,
        work_tx: mpsc::Sender<IngressRecord>,
        completion_rx: mpsc::Receiver<Completion>,
        memory_budget_bytes: usize,
        record_accounting_overhead_bytes: usize,
        shutdown: CancellationToken,
    ) -> Self {
        let memory_budget_bytes =
            u32::try_from(memory_budget_bytes).expect("configuration validates byte budget");
        Self {
            consumer,
            registry,
            rebalance_rx,
            work_tx: Some(work_tx),
            completion_rx,
            memory_budget: Arc::new(Semaphore::new(memory_budget_bytes as usize)),
            memory_budget_bytes,
            record_accounting_overhead_bytes,
            shutdown,
            tracker: OffsetTracker::default(),
            paused: false,
        }
    }

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
            tokio::select! {
                _ = self.shutdown.cancelled() => {
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
                message = self.consumer.recv() => {
                    let message = message.context("Kafka receive failed")?;
                    let topic_partition =
                        TopicPartition::new(message.topic(), message.partition());
                    let Some(assignment_epoch) = self.registry.current_epoch(&topic_partition) else {
                        debug!(%topic_partition, offset = message.offset(), "discarding record outside a current assignment");
                        continue;
                    };
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
                    return self.finish_dispatch(pending, queue_permit, memory_permit);
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
                _ = self.shutdown.cancelled() => {
                    self.resume_current_assignment()?;
                    return Ok(DispatchOutcome::Shutdown);
                }
                acquired = &mut capacities => {
                    let (queue_permit, memory_permit) = acquired?;
                    let outcome = self.finish_dispatch(pending, queue_permit, memory_permit)?;
                    self.resume_current_assignment()?;
                    return Ok(outcome);
                }
                event = self.rebalance_rx.recv() => {
                    let event = event.ok_or_else(|| anyhow!("rebalance event channel closed"))?;
                    self.handle_rebalance(event)?;
                    if self.registry.current_epoch(&pending.token.topic_partition)
                        != Some(pending.token.assignment_epoch)
                    {
                        self.resume_current_assignment()?;
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

    async fn drain_and_commit(&mut self) -> Result<()> {
        let mut first_error = self.pause_current_assignment().err();
        drop(self.work_tx.take());

        loop {
            tokio::select! {
                completion = self.completion_rx.recv() => {
                    let Some(completion) = completion else { break };
                    if let Err(error) = self.handle_completion(completion)
                        && first_error.is_none()
                    {
                        first_error = Some(error);
                    }
                }
                event = self.rebalance_rx.recv() => {
                    let Some(event) = event else { continue };
                    if let Err(error) = self.handle_rebalance(event)
                        && first_error.is_none()
                    {
                        first_error = Some(error);
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
