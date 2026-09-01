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
    last_delivered_offset: Option<i64>,
}

impl PartitionProgress {
    fn new(assignment_epoch: u64) -> Self {
        Self {
            assignment_epoch,
            pending: BTreeMap::new(),
            safe_next_offset: None,
            last_delivered_offset: None,
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
    #[error("delivery references unassigned partition {0}")]
    UnassignedDelivery(TopicPartition),
    #[error(
        "delivery epoch {delivery_epoch} does not match assignment epoch {assignment_epoch} for {topic_partition}"
    )]
    DeliveryEpochMismatch {
        topic_partition: TopicPartition,
        delivery_epoch: u64,
        assignment_epoch: u64,
    },
    #[error("duplicate in-flight delivery for {topic_partition} at offset {record_offset}")]
    DuplicateDelivery {
        topic_partition: TopicPartition,
        record_offset: i64,
    },
    #[error(
        "non-monotonic delivery for {topic_partition}: offset {record_offset} followed {last_delivered_offset}"
    )]
    NonMonotonicDelivery {
        topic_partition: TopicPartition,
        record_offset: i64,
        last_delivered_offset: i64,
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

    /// Registers a delivery under an assignment established by the runner.
    ///
    /// # Errors
    ///
    /// Returns an error for missing/mismatched assignments or duplicate in-flight offsets.
    pub fn on_delivered(&mut self, token: &DeliveryToken) -> Result<(), TrackerError> {
        let Some(progress) = self.partitions.get_mut(&token.topic_partition) else {
            return Err(TrackerError::UnassignedDelivery(
                token.topic_partition.clone(),
            ));
        };
        if progress.assignment_epoch != token.assignment_epoch {
            return Err(TrackerError::DeliveryEpochMismatch {
                topic_partition: token.topic_partition.clone(),
                delivery_epoch: token.assignment_epoch,
                assignment_epoch: progress.assignment_epoch,
            });
        }

        if progress.pending.contains_key(&token.record_offset) {
            return Err(TrackerError::DuplicateDelivery {
                topic_partition: token.topic_partition.clone(),
                record_offset: token.record_offset,
            });
        }
        if let Some(last_delivered_offset) = progress.last_delivered_offset
            && token.record_offset <= last_delivered_offset
        {
            return Err(TrackerError::NonMonotonicDelivery {
                topic_partition: token.topic_partition.clone(),
                record_offset: token.record_offset,
                last_delivered_offset,
            });
        }

        let _ = progress
            .pending
            .insert(token.record_offset, DeliveryState::Pending);
        progress.last_delivered_offset = Some(token.record_offset);
        Ok(())
    }

    /// Applies an idempotent success ACK and advances the safe prefix when possible.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown deliveries or offset overflow.
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

        let mut advanced_to = None;
        while progress
            .pending
            .first_key_value()
            .is_some_and(|(_, state)| *state == DeliveryState::Succeeded)
        {
            let Some((record_offset, _)) = progress.pending.pop_first() else {
                break;
            };
            let safe_next_offset = record_offset
                .checked_add(1)
                .ok_or_else(|| TrackerError::OffsetOverflow(token.topic_partition.clone()))?;
            progress.safe_next_offset = Some(safe_next_offset);
            advanced_to = Some(safe_next_offset);
        }

        if let Some(safe_next_offset) = advanced_to {
            Ok(AckEffect::StoreNext(OffsetSnapshot {
                topic_partition: token.topic_partition.clone(),
                safe_next_offset,
                assignment_epoch: progress.assignment_epoch,
            }))
        } else {
            Ok(AckEffect::Unchanged)
        }
    }

    #[must_use]
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
    #![allow(clippy::unwrap_used)]

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
        tracker.ensure_assigned(token(100, 7).topic_partition, 7);
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
        tracker.ensure_assigned(token(10, 1).topic_partition, 1);
        tracker.on_delivered(&token(10, 1)).unwrap();
        tracker.revoke(&token(10, 1).topic_partition, 1);
        tracker.ensure_assigned(token(10, 2).topic_partition, 2);

        assert_eq!(tracker.on_success(&token(10, 1)).unwrap(), AckEffect::Stale);
        assert!(tracker.safe_snapshot().is_empty());
    }

    #[test]
    fn safe_snapshot_excludes_unfinished_gap_and_revoked_assignment() {
        let mut tracker = OffsetTracker::default();
        tracker.ensure_assigned(token(20, 3).topic_partition, 3);
        for offset in [20, 21, 22] {
            tracker.on_delivered(&token(offset, 3)).unwrap();
        }
        tracker.on_success(&token(20, 3)).unwrap();
        tracker.on_success(&token(22, 3)).unwrap();

        assert_eq!(tracker.safe_snapshot()[0].safe_next_offset, 21);
        tracker.revoke(&token(20, 3).topic_partition, 3);
        assert!(tracker.safe_snapshot().is_empty());
    }

    #[test]
    fn completed_records_cannot_cross_an_unfinished_gap() {
        let mut tracker = OffsetTracker::default();
        tracker.ensure_assigned(token(100, 9).topic_partition, 9);
        for offset in [100, 101, 102] {
            tracker.on_delivered(&token(offset, 9)).unwrap();
        }

        assert_eq!(
            tracker.on_success(&token(100, 9)).unwrap(),
            AckEffect::StoreNext(OffsetSnapshot {
                topic_partition: TopicPartition::new("signals", 2),
                safe_next_offset: 101,
                assignment_epoch: 9,
            })
        );
        assert_eq!(
            tracker.on_success(&token(102, 9)).unwrap(),
            AckEffect::Unchanged
        );
        assert_eq!(tracker.safe_snapshot()[0].safe_next_offset, 101);
    }

    #[test]
    fn delivery_token_cannot_establish_or_replace_assignment() {
        let mut tracker = OffsetTracker::default();
        assert_eq!(
            tracker.on_delivered(&token(1, 1)).unwrap_err(),
            TrackerError::UnassignedDelivery(TopicPartition::new("signals", 2))
        );

        tracker.ensure_assigned(token(1, 2).topic_partition, 2);
        assert!(matches!(
            tracker.on_delivered(&token(1, 1)),
            Err(TrackerError::DeliveryEpochMismatch { .. })
        ));
    }

    #[test]
    fn rejects_non_monotonic_delivery_within_an_assignment() {
        let mut tracker = OffsetTracker::default();
        tracker.ensure_assigned(token(102, 11).topic_partition, 11);
        tracker.on_delivered(&token(102, 11)).unwrap();
        tracker.on_success(&token(102, 11)).unwrap();

        assert_eq!(
            tracker.on_delivered(&token(100, 11)).unwrap_err(),
            TrackerError::NonMonotonicDelivery {
                topic_partition: TopicPartition::new("signals", 2),
                record_offset: 100,
                last_delivered_offset: 102,
            }
        );
    }

    #[test]
    fn rejects_duplicate_pending_delivery() {
        let mut tracker = OffsetTracker::default();
        tracker.ensure_assigned(token(100, 12).topic_partition, 12);
        tracker.on_delivered(&token(100, 12)).unwrap();

        assert_eq!(
            tracker.on_delivered(&token(100, 12)).unwrap_err(),
            TrackerError::DuplicateDelivery {
                topic_partition: TopicPartition::new("signals", 2),
                record_offset: 100,
            }
        );
    }
}
