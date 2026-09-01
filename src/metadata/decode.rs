use serde::Deserialize;
use thiserror::Error;

use crate::{IngressRecord, metadata::ObjectWorkItem};

use super::ObjectMetadata;

#[derive(Debug, Error)]
pub(crate) enum MetadataError {
    #[error("record payload is missing")]
    MissingPayload,
    #[error("object metadata is not valid JSON")]
    InvalidJson(#[source] serde_json::Error),
    #[error("object metadata field `{0}` must not be empty")]
    EmptyField(&'static str),
    #[error("object metadata field `{0}` contains a null character")]
    NullCharacter(&'static str),
    #[error("object metadata field `size` must be greater than zero")]
    EmptyObject,
    #[error("object metadata size {actual} exceeds configured maximum {maximum}")]
    ObjectTooLarge { actual: u64, maximum: u64 },
}

/// A decode failure retains the source record so the caller must make an
/// explicit failed-completion decision rather than silently dropping it.
#[derive(Debug)]
pub(crate) struct MetadataDecodeFailure {
    source: IngressRecord,
    error: MetadataError,
}

impl MetadataDecodeFailure {
    pub(crate) fn into_parts(self) -> (IngressRecord, MetadataError) {
        (self.source, self.error)
    }
}

#[derive(Debug, Deserialize)]
struct WireObjectMetadata {
    bucket: Box<str>,
    key: Box<str>,
    size: u64,
    #[serde(default)]
    etag: Option<Box<str>>,
}

pub(crate) fn decode_record(
    source: IngressRecord,
    max_object_size: u64,
) -> Result<ObjectWorkItem, Box<MetadataDecodeFailure>> {
    let decoded = source
        .payload()
        .ok_or(MetadataError::MissingPayload)
        .and_then(|payload| decode_payload(payload, max_object_size));

    match decoded {
        Ok(metadata) => Ok(ObjectWorkItem::new(metadata, source)),
        Err(error) => Err(Box::new(MetadataDecodeFailure { source, error })),
    }
}

fn decode_payload(payload: &[u8], max_object_size: u64) -> Result<ObjectMetadata, MetadataError> {
    let wire = serde_json::from_slice::<WireObjectMetadata>(payload)
        .map_err(MetadataError::InvalidJson)?;

    validate_required_text("bucket", &wire.bucket, true)?;
    validate_required_text("key", &wire.key, false)?;
    if wire.size == 0 {
        return Err(MetadataError::EmptyObject);
    }
    if wire.size > max_object_size {
        return Err(MetadataError::ObjectTooLarge {
            actual: wire.size,
            maximum: max_object_size,
        });
    }
    if let Some(etag) = wire.etag.as_deref() {
        validate_required_text("etag", etag, true)?;
    }

    Ok(ObjectMetadata::new(
        wire.bucket,
        wire.key,
        wire.size,
        wire.etag,
    ))
}

fn validate_required_text(
    field: &'static str,
    value: &str,
    reject_whitespace_only: bool,
) -> Result<(), MetadataError> {
    if value.is_empty() || (reject_whitespace_only && value.trim().is_empty()) {
        return Err(MetadataError::EmptyField(field));
    }
    if value.contains('\0') {
        return Err(MetadataError::NullCharacter(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn decodes_the_v1_json_contract_and_ignores_additive_fields() {
        let metadata = decode_payload(
            br#"{
                "bucket": "vehicle-signals",
                "key": "2026/09/02/signals.dbc.gz",
                "size": 42,
                "etag": "\"abc123\"",
                "producer_trace_id": "future-compatible"
            }"#,
            1024,
        )
        .unwrap();

        assert_eq!(metadata.bucket(), "vehicle-signals");
        assert_eq!(metadata.key(), "2026/09/02/signals.dbc.gz");
        assert_eq!(metadata.size(), 42);
        assert_eq!(metadata.etag(), Some("\"abc123\""));
    }

    #[test]
    fn accepts_an_absent_or_null_etag() {
        for payload in [
            br#"{"bucket":"signals","key":"one.gz","size":1}"#.as_slice(),
            br#"{"bucket":"signals","key":"one.gz","size":1,"etag":null}"#.as_slice(),
        ] {
            assert_eq!(decode_payload(payload, 1).unwrap().etag(), None);
        }
    }

    #[test]
    fn rejects_missing_and_mistyped_required_fields() {
        for payload in [
            br#"{"key":"one.gz","size":1}"#.as_slice(),
            br#"{"bucket":"signals","size":1}"#.as_slice(),
            br#"{"bucket":"signals","key":"one.gz","size":"1"}"#.as_slice(),
            br#"{"bucket":"signals","key":"one.gz","size_bytes":1}"#.as_slice(),
        ] {
            assert!(matches!(
                decode_payload(payload, 1024),
                Err(MetadataError::InvalidJson(_))
            ));
        }
    }

    #[test]
    fn rejects_invalid_identity_and_size_values() {
        let cases = [
            (
                br#"{"bucket":"","key":"one.gz","size":1}"#.as_slice(),
                "bucket",
            ),
            (
                br#"{"bucket":"signals","key":"","size":1}"#.as_slice(),
                "key",
            ),
            (
                br#"{"bucket":"signals","key":"one.gz","size":0}"#.as_slice(),
                "greater than zero",
            ),
            (
                br#"{"bucket":"signals","key":"one.gz","size":11}"#.as_slice(),
                "configured maximum",
            ),
            (
                br#"{"bucket":"signals","key":"one.gz","size":1,"etag":""}"#.as_slice(),
                "etag",
            ),
        ];

        for (payload, expected) in cases {
            let error = decode_payload(payload, 10).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }
}
