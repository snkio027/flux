use std::{future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use aws_config::{BehaviorVersion, retry::RetryConfig, timeout::TimeoutConfig};
use aws_sdk_s3::{
    Client,
    config::{Builder as S3ClientConfigBuilder, Region},
    primitives::ByteStream,
};
use bytes::Bytes;
use tokio::time::Instant;

use crate::{ObjectMetadata, config::S3Config};

/// Summary of an object whose response headers and streamed size were verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DownloadSummary {
    size: u64,
    etag: Option<Box<str>>,
}

impl DownloadSummary {
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

/// Concrete S3 transport. The SDK client is shared and cloned cheaply across workers.
#[derive(Clone, Debug)]
pub struct S3Downloader {
    client: Client,
    stream_idle_timeout: Duration,
}

impl S3Downloader {
    /// Builds an S3 client from the AWS default credential/provider chain plus
    /// the explicit service settings in `S3Config`.
    ///
    /// # Errors
    ///
    /// Returns an error when the S3 retry/timeout settings are invalid or no
    /// AWS region can be resolved.
    pub async fn from_config(config: &S3Config) -> Result<Self> {
        config.validate()?;
        let timeout_config = TimeoutConfig::builder()
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .operation_attempt_timeout(Duration::from_millis(config.operation_attempt_timeout_ms))
            .operation_timeout(Duration::from_millis(config.operation_timeout_ms))
            .build();
        let retry_config = RetryConfig::standard().with_max_attempts(config.max_attempts);
        let mut shared_config = aws_config::defaults(BehaviorVersion::latest())
            .timeout_config(timeout_config)
            .retry_config(retry_config);
        if let Some(region) = &config.region {
            shared_config = shared_config.region(Region::new(region.clone()));
        }
        let shared_config = shared_config.load().await;
        if shared_config.region().is_none() {
            bail!("S3 region was not configured explicitly or resolved by the AWS provider chain");
        }

        let mut client_config =
            S3ClientConfigBuilder::from(&shared_config).force_path_style(config.force_path_style);
        if let Some(endpoint_url) = &config.endpoint_url {
            client_config = client_config.endpoint_url(endpoint_url);
        }

        Ok(Self {
            client: Client::from_conf(client_config.build()),
            stream_idle_timeout: Duration::from_millis(config.stream_idle_timeout_ms),
        })
    }

    /// Streams one complete object into `consume_chunk` without aggregating it
    /// in memory. Successful return guarantees response and streamed size/ETag
    /// validation; a consumer error stops the download and is propagated.
    ///
    /// # Errors
    ///
    /// Returns an error when `GetObject` fails, response identity or size does
    /// not match metadata, the body stalls or is corrupt/truncated, or the
    /// chunk consumer rejects data.
    pub async fn download_into<Consume, ConsumeFuture>(
        &self,
        metadata: &ObjectMetadata,
        consume_chunk: Consume,
    ) -> Result<DownloadSummary>
    where
        Consume: FnMut(Bytes) -> ConsumeFuture + Send,
        ConsumeFuture: Future<Output = Result<()>> + Send,
    {
        let location = format!("s3://{}/{}", metadata.bucket(), metadata.key());
        let mut request = self
            .client
            .get_object()
            .bucket(metadata.bucket())
            .key(metadata.key());
        if let Some(etag) = metadata.etag() {
            request = request.if_match(etag);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("S3 GetObject failed for {location}"))?;
        let response_etag = response.e_tag().map(Into::<Box<str>>::into);
        validate_response_headers(
            metadata.size(),
            metadata.etag(),
            response.content_length(),
            response_etag.as_deref(),
        )
        .with_context(|| format!("S3 response validation failed for {location}"))?;

        consume_body(
            response.body,
            metadata.size(),
            self.stream_idle_timeout,
            consume_chunk,
        )
        .await
        .with_context(|| format!("S3 body streaming failed for {location}"))?;

        Ok(DownloadSummary {
            size: metadata.size(),
            etag: response_etag,
        })
    }
}

fn validate_response_headers(
    expected_size: u64,
    expected_etag: Option<&str>,
    response_size: Option<i64>,
    response_etag: Option<&str>,
) -> Result<()> {
    let response_size =
        response_size.ok_or_else(|| anyhow!("S3 response omitted Content-Length"))?;
    let response_size =
        u64::try_from(response_size).context("S3 response contained a negative Content-Length")?;
    if response_size != expected_size {
        bail!("S3 Content-Length {response_size} does not match metadata size {expected_size}");
    }

    if let Some(expected_etag) = expected_etag {
        let response_etag = response_etag.ok_or_else(|| anyhow!("S3 response omitted ETag"))?;
        if response_etag != expected_etag {
            bail!("S3 ETag {response_etag:?} does not match metadata ETag {expected_etag:?}");
        }
    }
    Ok(())
}

async fn consume_body<Consume, ConsumeFuture>(
    mut body: ByteStream,
    expected_size: u64,
    stream_idle_timeout: Duration,
    mut consume_chunk: Consume,
) -> Result<()>
where
    Consume: FnMut(Bytes) -> ConsumeFuture + Send,
    ConsumeFuture: Future<Output = Result<()>> + Send,
{
    let mut streamed_size = 0_u64;
    let mut progress_deadline = Instant::now() + stream_idle_timeout;
    loop {
        let next = tokio::time::timeout_at(progress_deadline, body.next())
            .await
            .map_err(|_| anyhow!("S3 body made no progress for {stream_idle_timeout:?}"))?;
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.context("failed to read S3 response body")?;
        if chunk.is_empty() {
            continue;
        }
        progress_deadline = Instant::now() + stream_idle_timeout;
        let chunk_size = u64::try_from(chunk.len()).context("S3 chunk length exceeds u64")?;
        let next_size = streamed_size
            .checked_add(chunk_size)
            .ok_or_else(|| anyhow!("S3 streamed size overflowed u64"))?;
        if next_size > expected_size {
            bail!(
                "S3 body exceeded metadata size {expected_size} after receiving {next_size} bytes"
            );
        }
        consume_chunk(chunk)
            .await
            .context("object chunk consumer failed")?;
        streamed_size = next_size;
    }

    if streamed_size != expected_size {
        bail!("S3 body size {streamed_size} does not match metadata size {expected_size}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::{
        pin::Pin,
        task::{Context as TaskContext, Poll},
    };

    use aws_sdk_s3::{Config, config::Credentials};
    use aws_smithy_http_client::test_util::capture_request;
    use aws_smithy_types::body::SdkBody;
    use http::{Method, Response, header::IF_MATCH};
    use http_body::{Body, Frame};
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Debug)]
    struct PendingBody;

    impl Body for PendingBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn get_object_sends_if_match_and_streams_the_response() {
        let (http_client, captured_request) = capture_request(Some(
            Response::builder()
                .status(200)
                .header("content-length", "3")
                .header("etag", "\"abc\"")
                .body(SdkBody::from("abc"))
                .unwrap(),
        ));
        let client_config = Config::builder()
            .behavior_version_latest()
            .credentials_provider(Credentials::for_tests())
            .region(Region::new("us-east-1"))
            .force_path_style(true)
            .http_client(http_client)
            .build();
        let downloader = S3Downloader {
            client: Client::from_conf(client_config),
            stream_idle_timeout: Duration::from_secs(1),
        };
        let metadata = ObjectMetadata::new(
            "signals".into(),
            "folder/test.dbc.gz".into(),
            3,
            Some("\"abc\"".into()),
        );
        let collected = Arc::new(Mutex::new(Vec::new()));
        let consumer = {
            let collected = Arc::clone(&collected);
            move |chunk: Bytes| {
                let collected = Arc::clone(&collected);
                async move {
                    collected.lock().await.extend_from_slice(&chunk);
                    Ok(())
                }
            }
        };

        let summary = downloader.download_into(&metadata, consumer).await.unwrap();

        assert_eq!(summary.size(), 3);
        assert_eq!(summary.etag(), Some("\"abc\""));
        assert_eq!(&*collected.lock().await, b"abc");
        let request = captured_request.expect_request();
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.headers().get(IF_MATCH).unwrap(), "\"abc\"");
        assert!(
            request.uri().contains("/signals/folder/test.dbc.gz?"),
            "unexpected request URI: {}",
            request.uri()
        );
    }

    #[test]
    fn validates_content_length_and_optional_etag() {
        validate_response_headers(3, Some("\"abc\""), Some(3), Some("\"abc\"")).unwrap();
        validate_response_headers(3, None, Some(3), None).unwrap();

        for (result, expected) in [
            (
                validate_response_headers(3, None, None, None),
                "omitted Content-Length",
            ),
            (
                validate_response_headers(3, None, Some(-1), None),
                "negative Content-Length",
            ),
            (
                validate_response_headers(3, None, Some(2), None),
                "does not match metadata size",
            ),
            (
                validate_response_headers(3, Some("\"abc\""), Some(3), None),
                "omitted ETag",
            ),
            (
                validate_response_headers(3, Some("\"abc\""), Some(3), Some("\"def\"")),
                "does not match metadata ETag",
            ),
        ] {
            let error = result.unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn streams_the_body_into_the_consumer_and_checks_size() {
        let collected = Arc::new(Mutex::new(Vec::new()));
        let consumer = {
            let collected = Arc::clone(&collected);
            move |chunk: Bytes| {
                let collected = Arc::clone(&collected);
                async move {
                    collected.lock().await.extend_from_slice(&chunk);
                    Ok(())
                }
            }
        };

        consume_body(
            ByteStream::from_static(b"abcdef"),
            6,
            Duration::from_secs(1),
            consumer,
        )
        .await
        .unwrap();

        assert_eq!(&*collected.lock().await, b"abcdef");
    }

    #[tokio::test]
    async fn rejects_oversized_body_before_delivering_the_chunk() {
        let calls = Arc::new(AtomicUsize::new(0));
        let consumer = {
            let calls = Arc::clone(&calls);
            move |_chunk: Bytes| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }
        };

        let error = consume_body(
            ByteStream::from_static(b"abcd"),
            3,
            Duration::from_secs(1),
            consumer,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("exceeded metadata size"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_truncated_body_and_consumer_failure() {
        let truncated = consume_body(
            ByteStream::from_static(b"ab"),
            3,
            Duration::from_secs(1),
            |_chunk| async { Ok(()) },
        )
        .await
        .unwrap_err();
        assert!(
            truncated
                .to_string()
                .contains("does not match metadata size")
        );

        let consumer_error = consume_body(
            ByteStream::from_static(b"abc"),
            3,
            Duration::from_secs(1),
            |_chunk| async { Err(anyhow!("synthetic gzip boundary failure")) },
        )
        .await
        .unwrap_err();
        assert!(format!("{consumer_error:#}").contains("synthetic gzip boundary failure"));
    }

    #[tokio::test]
    async fn rejects_a_body_that_stops_making_progress() {
        let error = consume_body(
            ByteStream::from_body_1_x(PendingBody),
            1,
            Duration::from_millis(1),
            |_chunk| async { Ok(()) },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("made no progress"));
    }
}
