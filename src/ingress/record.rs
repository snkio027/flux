use std::{fmt, sync::Arc};

use anyhow::{Result, anyhow};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use tokio::sync::OwnedSemaphorePermit;

use crate::config::RECORD_ACCOUNTING_OVERHEAD_BYTES;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TopicPartition {
    pub(crate) topic: Arc<str>,
    pub(crate) partition: i32,
}

impl TopicPartition {
    pub(crate) fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}[{}]", self.topic, self.partition)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeliveryToken {
    pub(crate) topic_partition: TopicPartition,
    pub(crate) record_offset: i64,
    pub(crate) assignment_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHeader {
    key: Box<str>,
    value: Option<Box<[u8]>>,
}

impl RecordHeader {
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

/// An owned ingress record. The byte permit is intentionally retained for the
/// entire downstream lifetime of the record.
pub struct IngressRecord {
    token: DeliveryToken,
    key: Option<Box<[u8]>>,
    payload: Option<Box<[u8]>>,
    headers: Vec<RecordHeader>,
    timestamp_millis: Option<i64>,
    accounted_bytes: u32,
    _memory_permit: OwnedSemaphorePermit,
}

impl IngressRecord {
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.token.topic_partition.topic
    }

    #[must_use]
    pub fn partition(&self) -> i32 {
        self.token.topic_partition.partition
    }

    #[must_use]
    pub fn offset(&self) -> i64 {
        self.token.record_offset
    }

    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    #[must_use]
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    #[must_use]
    pub fn headers(&self) -> &[RecordHeader] {
        &self.headers
    }

    #[must_use]
    pub fn timestamp_millis(&self) -> Option<i64> {
        self.timestamp_millis
    }

    #[must_use]
    pub fn accounted_bytes(&self) -> u32 {
        self.accounted_bytes
    }

    #[must_use]
    pub fn succeed(self) -> Completion {
        Completion {
            token: self.token.clone(),
            outcome: CompletionOutcome::Succeeded,
        }
    }

    #[must_use]
    pub fn fail(self, reason: impl Into<Box<str>>) -> Completion {
        Completion {
            token: self.token.clone(),
            outcome: CompletionOutcome::Failed(reason.into()),
        }
    }

    pub(crate) fn from_pending(
        pending: PendingRecord,
        memory_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            token: pending.token,
            key: pending.key,
            payload: pending.payload,
            headers: pending.headers,
            timestamp_millis: pending.timestamp_millis,
            accounted_bytes: pending.accounted_bytes,
            _memory_permit: memory_permit,
        }
    }
}

impl fmt::Debug for IngressRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngressRecord")
            .field("token", &self.token)
            .field("key_bytes", &self.key.as_deref().map(<[u8]>::len))
            .field("payload_bytes", &self.payload.as_deref().map(<[u8]>::len))
            .field("headers", &self.headers.len())
            .field("timestamp_millis", &self.timestamp_millis)
            .field("accounted_bytes", &self.accounted_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct Completion {
    token: DeliveryToken,
    outcome: CompletionOutcome,
}

impl Completion {
    pub(crate) fn into_parts(self) -> (DeliveryToken, CompletionOutcome) {
        (self.token, self.outcome)
    }
}

#[derive(Debug)]
pub(crate) enum CompletionOutcome {
    Succeeded,
    Failed(Box<str>),
}

pub(crate) struct PendingRecord {
    pub token: DeliveryToken,
    pub key: Option<Box<[u8]>>,
    pub payload: Option<Box<[u8]>>,
    pub headers: Vec<RecordHeader>,
    pub timestamp_millis: Option<i64>,
    pub accounted_bytes: u32,
}

impl PendingRecord {
    pub(crate) fn from_message(
        message: &BorrowedMessage<'_>,
        assignment_epoch: u64,
    ) -> Result<Self> {
        let topic_partition = TopicPartition::new(message.topic(), message.partition());
        let token = DeliveryToken {
            topic_partition,
            record_offset: message.offset(),
            assignment_epoch,
        };
        let accounted_bytes = accounted_record_bytes(message)?;

        let headers = message
            .headers()
            .map(|headers| {
                (0..headers.count())
                    .map(|index| {
                        let header = headers.get(index);
                        RecordHeader {
                            key: header.key.into(),
                            value: header.value.map(Into::into),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            token,
            key: message.key().map(Into::into),
            payload: message.payload().map(Into::into),
            headers,
            timestamp_millis: message.timestamp().to_millis(),
            accounted_bytes,
        })
    }
}

fn accounted_record_bytes(message: &BorrowedMessage<'_>) -> Result<u32> {
    let mut bytes = RECORD_ACCOUNTING_OVERHEAD_BYTES;
    checked_add(&mut bytes, message.key().map_or(0, <[u8]>::len))?;
    checked_add(&mut bytes, message.payload().map_or(0, <[u8]>::len))?;

    if let Some(headers) = message.headers() {
        for index in 0..headers.count() {
            let header = headers.get(index);
            checked_add(&mut bytes, header.key.len())?;
            checked_add(&mut bytes, header.value.map_or(0, <[u8]>::len))?;
        }
    }

    u32::try_from(bytes).map_err(|_| anyhow!("record accounting exceeds {} bytes", u32::MAX))
}

fn checked_add(total: &mut usize, value: usize) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| anyhow!("record byte accounting overflow"))?;
    Ok(())
}
