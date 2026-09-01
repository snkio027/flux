use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

use super::{DeliveryToken, TopicPartition};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryState {
    Pending,
    Succeeded,
}

#[derive(Debug)]
struct PartitionProgress {
    assignment_epoch: u64,
    pending: BTreeMap<i64, DeliveryState>,
    safe_next_offset: Option<i64>,
}

impl PartitionProgress {
    fn new(assignment_epoch: u64) -> Self {
        Self {
            assignment_epoch,
            pending: BTreeMap::new(),
            safe_next_offset: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AckEffect {
    StoreNext(OffsetSnapshot),
    Stale,
    Unchanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OffsetSnapshot {
    pub topic_partition: TopicPartition,
    pub safe_next_offset: i64,
    pub assignment_epoch: u64,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TrackerError {
    #[error("duplicate in-flight delivery for {topic_partition} at offset {record_offset}")]
    DuplicateDelivery {
        topic_partition: TopicPartition,
        record_offset: i64,
    },
    #[error(
        "completion references unknown delivery for {topic_partition} at offset {record_offset}"
    )]
    UnknownDelivery {
        topic_partition: TopicPartition,
        record_offset: i64,
    },
    #[error("Kafka offset cannot advance beyond i64::MAX for {0}")]
    OffsetOverflow(TopicPartition),
}

#[derive(Debug, Default)]
pub struct OffsetTracker {
    partitions: HashMap<TopicPartition, PartitionProgress>,
}

impl OffsetTracker {
    /// Idempotently aligns business tracking to the callback-owned epoch.
    pub fn ensure_assigned(&mut self, topic_partition: TopicPartition, assignment_epoch: u64) {
        let matches_current = self
            .partitions
            .get(&topic_partition)
            .is_some_and(|state| state.assignment_epoch == assignment_epoch);
        if !matches_current {
            self.partitions
                .insert(topic_partition, PartitionProgress::new(assignment_epoch));
        }
    }

    pub fn revoke(&mut self, topic_partition: &TopicPartition, assignment_epoch: u64) {
        let should_remove = self
            .partitions
            .get(topic_partition)
            .is_some_and(|state| state.assignment_epoch == assignment_epoch);
        if should_remove {
            self.partitions.remove(topic_partition);
        }
    }

    pub fn on_delivered(&mut self, token: &DeliveryToken) -> Result<(), TrackerError> {
        self.ensure_assigned(token.topic_partition.clone(), token.assignment_epoch);
        let progress = self
            .partitions
            .get_mut(&token.topic_partition)
            .expect("assignment was ensured");

        if progress
            .safe_next_offset
            .is_some_and(|safe_next| token.record_offset < safe_next)
        {
            return Ok(());
        }
        if progress
            .pending
            .insert(token.record_offset, DeliveryState::Pending)
            .is_some()
        {
            return Err(TrackerError::DuplicateDelivery {
                topic_partition: token.topic_partition.clone(),
                record_offset: token.record_offset,
            });
        }
        Ok(())
    }

    pub fn on_success(&mut self, token: &DeliveryToken) -> Result<AckEffect, TrackerError> {
        let Some(progress) = self.partitions.get_mut(&token.topic_partition) else {
            return Ok(AckEffect::Stale);
        };
        if progress.assignment_epoch != token.assignment_epoch {
            return Ok(AckEffect::Stale);
        }
        if progress
            .safe_next_offset
            .is_some_and(|safe_next| token.record_offset < safe_next)
        {
            return Ok(AckEffect::Unchanged);
        }

        let Some(delivery) = progress.pending.get_mut(&token.record_offset) else {
            return Err(TrackerError::UnknownDelivery {
                topic_partition: token.topic_partition.clone(),
                record_offset: token.record_offset,
            });
        };
        if *delivery == DeliveryState::Succeeded {
            return Ok(AckEffect::Unchanged);
        }
        *delivery = DeliveryState::Succeeded;

        let mut advanced = false;
        while progress
            .pending
            .first_key_value()
            .is_some_and(|(_, state)| *state == DeliveryState::Succeeded)
        {
            let (record_offset, _) = progress
                .pending
                .pop_first()
                .expect("first entry was present");
            progress.safe_next_offset = Some(
                record_offset
                    .checked_add(1)
                    .ok_or_else(|| TrackerError::OffsetOverflow(token.topic_partition.clone()))?,
            );
            advanced = true;
        }

        if advanced {
            Ok(AckEffect::StoreNext(OffsetSnapshot {
                topic_partition: token.topic_partition.clone(),
                safe_next_offset: progress.safe_next_offset.expect("prefix advanced"),
                assignment_epoch: progress.assignment_epoch,
            }))
        } else {
            Ok(AckEffect::Unchanged)
        }
    }

    pub fn safe_snapshot(&self) -> Vec<OffsetSnapshot> {
        let mut snapshot = self
            .partitions
            .iter()
            .filter_map(|(topic_partition, progress)| {
                progress
                    .safe_next_offset
                    .map(|safe_next_offset| OffsetSnapshot {
                        topic_partition: topic_partition.clone(),
                        safe_next_offset,
                        assignment_epoch: progress.assignment_epoch,
                    })
            })
            .collect::<Vec<_>>();
        snapshot.sort_by(|left, right| left.topic_partition.cmp(&right.topic_partition));
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(offset: i64, epoch: u64) -> DeliveryToken {
        DeliveryToken {
            topic_partition: TopicPartition::new("signals", 2),
            record_offset: offset,
            assignment_epoch: epoch,
        }
    }

    #[test]
    fn advances_actual_delivery_prefix_without_numeric_contiguity() {
        let mut tracker = OffsetTracker::default();
        for offset in [100, 102, 105] {
            tracker.on_delivered(&token(offset, 7)).unwrap();
        }

        assert_eq!(
            tracker.on_success(&token(102, 7)).unwrap(),
            AckEffect::Unchanged
        );
        assert_eq!(
            tracker.on_success(&token(100, 7)).unwrap(),
            AckEffect::StoreNext(OffsetSnapshot {
                topic_partition: TopicPartition::new("signals", 2),
                safe_next_offset: 103,
                assignment_epoch: 7,
            })
        );
        assert_eq!(
            tracker.on_success(&token(105, 7)).unwrap(),
            AckEffect::StoreNext(OffsetSnapshot {
                topic_partition: TopicPartition::new("signals", 2),
                safe_next_offset: 106,
                assignment_epoch: 7,
            })
        );
    }

    #[test]
    fn late_ack_from_revoked_epoch_is_stale() {
        let mut tracker = OffsetTracker::default();
        tracker.on_delivered(&token(10, 1)).unwrap();
        tracker.revoke(&token(10, 1).topic_partition, 1);
        tracker.ensure_assigned(token(10, 2).topic_partition, 2);

        assert_eq!(tracker.on_success(&token(10, 1)).unwrap(), AckEffect::Stale);
        assert!(tracker.safe_snapshot().is_empty());
    }

    #[test]
    fn safe_snapshot_excludes_unfinished_gap_and_revoked_assignment() {
        let mut tracker = OffsetTracker::default();
        for offset in [20, 21, 22] {
            tracker.on_delivered(&token(offset, 3)).unwrap();
        }
        tracker.on_success(&token(20, 3)).unwrap();
        tracker.on_success(&token(22, 3)).unwrap();

        assert_eq!(tracker.safe_snapshot()[0].safe_next_offset, 21);
        tracker.revoke(&token(20, 3).topic_partition, 3);
        assert!(tracker.safe_snapshot().is_empty());
    }
}
