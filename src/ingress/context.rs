use std::sync::Arc;

use rdkafka::{
    client::ClientContext,
    consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    topic_partition_list::TopicPartitionList,
};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use super::{TopicPartition, assignment::Assignment};
use crate::ingress::AssignmentRegistry;

#[derive(Debug)]
pub enum RebalanceEvent {
    Assigned(Vec<Assignment>),
    Revoked(Vec<Assignment>),
    Error(Box<str>),
}

#[derive(Clone)]
pub struct KafkaContext {
    registry: Arc<AssignmentRegistry>,
    events: mpsc::UnboundedSender<RebalanceEvent>,
}

impl KafkaContext {
    pub fn new(
        registry: Arc<AssignmentRegistry>,
        events: mpsc::UnboundedSender<RebalanceEvent>,
    ) -> Self {
        Self { registry, events }
    }

    fn publish(&self, event: RebalanceEvent) {
        if self.events.send(event).is_err() {
            warn!("rebalance event receiver has closed");
        }
    }
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(partitions) = rebalance {
            let revoked = self.registry.revoke(topic_partitions(partitions));
            debug!(count = revoked.len(), "revoking Kafka assignments");
            self.publish(RebalanceEvent::Revoked(revoked));
        } else if let Rebalance::Error(error) = rebalance {
            self.publish(RebalanceEvent::Error(error.to_string().into()));
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(partitions) = rebalance {
            match self.registry.assign(topic_partitions(partitions)) {
                Ok(assigned) => {
                    debug!(count = assigned.len(), "assigned Kafka partitions");
                    self.publish(RebalanceEvent::Assigned(assigned));
                }
                Err(error) => self.publish(RebalanceEvent::Error(error.to_string().into())),
            }
        } else if let Rebalance::Error(rebalance_error) = rebalance {
            error!(error = %rebalance_error, "Kafka rebalance failed");
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(()) => debug!(?offsets, "Kafka offsets committed"),
            Err(error) => error!(%error, ?offsets, "Kafka offset commit failed"),
        }
    }
}

pub type ManagedConsumer = StreamConsumer<KafkaContext>;

fn topic_partitions(list: &TopicPartitionList) -> Vec<TopicPartition> {
    list.elements()
        .iter()
        .map(|element| TopicPartition::new(element.topic(), element.partition()))
        .collect()
}
