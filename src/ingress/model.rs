use std::{fmt, sync::Arc};

use anyhow::{Result, anyhow};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicPartition {
    pub topic: Arc<str>,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
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
pub struct DeliveryToken {
    pub topic_partition: TopicPartition,
    pub record_offset: i64,
    pub assignment_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHeader {
    pub key: Box<str>,
    pub value: Option<Box<[u8]>>,
}

/// An owned Kafka record. The byte permit is intentionally retained for the
/// entire downstream lifetime of the record.
pub struct IngressRecord {
    pub token: DeliveryToken,
    pub key: Option<Box<[u8]>>,
    pub payload: Option<Box<[u8]>>,
    pub headers: Vec<RecordHeader>,
    pub timestamp_millis: Option<i64>,
    accounted_bytes: u32,
    _memory_permit: OwnedSemaphorePermit,
}

impl IngressRecord {
    #[must_use]
    pub fn accounted_bytes(&self) -> u32 {
        self.accounted_bytes
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
pub enum Completion {
    Succeeded(DeliveryToken),
    Failed {
        token: DeliveryToken,
        reason: Box<str>,
    },
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
    pub fn from_message(
        message: &BorrowedMessage<'_>,
        assignment_epoch: u64,
        fixed_overhead_bytes: usize,
    ) -> Result<Self> {
        let topic_partition = TopicPartition::new(message.topic(), message.partition());
        let token = DeliveryToken {
            topic_partition,
            record_offset: message.offset(),
            assignment_epoch,
        };
        let accounted_bytes = accounted_record_bytes(message, fixed_overhead_bytes)?;

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

pub(crate) fn accounted_record_bytes(
    message: &BorrowedMessage<'_>,
    fixed_overhead_bytes: usize,
) -> Result<u32> {
    let mut bytes = fixed_overhead_bytes;
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
