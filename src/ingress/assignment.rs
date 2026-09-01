use std::{collections::HashMap, sync::Mutex};

use rdkafka::error::KafkaResult;
use thiserror::Error;

use super::{OffsetSnapshot, TopicPartition};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Assignment {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) assignment_epoch: u64,
}

#[derive(Debug, Default)]
struct RegistryState {
    epochs: HashMap<TopicPartition, u64>,
    next_epoch: u64,
}

#[derive(Debug, Default)]
pub(crate) struct AssignmentRegistry {
    state: Mutex<RegistryState>,
}

#[derive(Debug, Error)]
#[error("assignment epoch space exhausted")]
pub(crate) struct EpochExhausted;

impl AssignmentRegistry {
    pub(crate) fn assign(
        &self,
        topic_partitions: impl IntoIterator<Item = TopicPartition>,
    ) -> Result<Vec<Assignment>, EpochExhausted> {
        let mut state = self.lock();
        let mut assignments = Vec::new();
        for topic_partition in topic_partitions {
            state.next_epoch = state.next_epoch.checked_add(1).ok_or(EpochExhausted)?;
            let assignment_epoch = state.next_epoch;
            state
                .epochs
                .insert(topic_partition.clone(), assignment_epoch);
            assignments.push(Assignment {
                topic_partition,
                assignment_epoch,
            });
        }
        Ok(assignments)
    }

    pub(crate) fn revoke(
        &self,
        topic_partitions: impl IntoIterator<Item = TopicPartition>,
    ) -> Vec<Assignment> {
        let mut state = self.lock();
        topic_partitions
            .into_iter()
            .filter_map(|topic_partition| {
                state
                    .epochs
                    .remove(&topic_partition)
                    .map(|assignment_epoch| Assignment {
                        topic_partition,
                        assignment_epoch,
                    })
            })
            .collect()
    }

    pub(crate) fn current_epoch(&self, topic_partition: &TopicPartition) -> Option<u64> {
        self.lock().epochs.get(topic_partition).copied()
    }

    pub(crate) fn is_current(&self, assignment: &Assignment) -> bool {
        self.current_epoch(&assignment.topic_partition) == Some(assignment.assignment_epoch)
    }

    /// Stores a local offset only while its assignment epoch is current.
    /// Rebalance callbacks use the same mutex, so revocation cannot interleave
    /// between the epoch check and the local store update.
    pub(crate) fn store_if_current(
        &self,
        snapshot: &OffsetSnapshot,
        store: impl FnOnce() -> KafkaResult<()>,
    ) -> Option<KafkaResult<()>> {
        let state = self.lock();
        if state.epochs.get(&snapshot.topic_partition).copied() == Some(snapshot.assignment_epoch) {
            Some(store())
        } else {
            None
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        // Registry mutations contain no user code and update a single map
        // entry atomically. Recovering the intact guard keeps a prior panic
        // from disabling all future rebalance fencing.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn reassigning_same_partition_produces_new_identity() {
        let registry = AssignmentRegistry::default();
        let tp = TopicPartition::new("signals", 0);
        let first = registry.assign([tp.clone()]).unwrap().remove(0);
        assert!(registry.is_current(&first));

        registry.revoke([tp.clone()]);
        let second = registry.assign([tp]).unwrap().remove(0);

        assert_ne!(first.assignment_epoch, second.assignment_epoch);
        assert!(!registry.is_current(&first));
        assert!(registry.is_current(&second));
    }
}
