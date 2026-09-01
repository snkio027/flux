use crate::IngressRecord;

/// Validated object identity and size from a Kafka metadata record.
///
/// This domain type deliberately contains no Kafka or object-store SDK types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    bucket: Box<str>,
    key: Box<str>,
    size: u64,
    etag: Option<Box<str>>,
}

impl ObjectMetadata {
    pub(crate) fn new(bucket: Box<str>, key: Box<str>, size: u64, etag: Option<Box<str>>) -> Self {
        Self {
            bucket,
            key,
            size,
            etag,
        }
    }

    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Expected object size in bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Optional opaque object identity evidence supplied by the producer.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

/// A validated object plus exclusive ownership of its Kafka completion capability.
#[derive(Debug)]
pub(crate) struct ObjectWorkItem {
    metadata: ObjectMetadata,
    source: IngressRecord,
}

impl ObjectWorkItem {
    pub(crate) fn new(metadata: ObjectMetadata, source: IngressRecord) -> Self {
        Self { metadata, source }
    }

    pub(crate) fn into_parts(self) -> (ObjectMetadata, IngressRecord) {
        (self.metadata, self.source)
    }
}
