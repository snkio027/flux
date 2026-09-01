# S3 streaming processor

This phase adds the concrete S3 transport behind the object-processing
foundation. It does not install S3 as the default application sink and does not
implement gzip framing or DBC decoding.

## Client configuration

`S3Downloader::from_config` builds one shareable AWS SDK client. Credentials
come from the standard AWS environment, profile, container, or instance-role
provider chain; configuration files must not contain credentials.

The `[s3]` section controls:

| Setting | Purpose |
| --- | --- |
| `region` | Optional override; otherwise use the AWS provider chain |
| `endpoint_url` | Optional S3-compatible/local endpoint |
| `force_path_style` | Use path-style requests when required by an endpoint |
| `max_attempts` | Initial request plus retry attempts, bounded to 1–10 |
| `connect_timeout_ms` | Per-attempt connection establishment bound |
| `operation_attempt_timeout_ms` | Bound for one request attempt |
| `operation_timeout_ms` | Bound for the request across all retries |
| `stream_idle_timeout_ms` | Maximum wait for the next body chunk |

The operation timeout ends when `GetObject` returns its response and therefore
does not protect the subsequent `ByteStream`. The explicit stream-idle timeout
closes that gap.

SDK retries cover failures before a usable `GetObject` response is returned.
Once body chunks have reached the consumer, the downloader never retries the
stream invisibly because the consumer is not assumed to be rewindable. A body
failure instead fails the source record so Kafka replay restarts the whole
object pipeline from a clean ownership boundary.

## Download contract

`download_into` performs one full-object `GetObject`. If metadata contains an
ETag, the request sends `If-Match`, and the returned ETag is also compared
exactly. Before consuming body bytes, the response must contain a non-negative
`Content-Length` equal to metadata `size`.

Each received `Bytes` chunk is passed directly to an asynchronous consumer. The
downloader never aggregates the object. It maintains checked byte accounting,
rejects a chunk before delivery if it would exceed metadata `size`, and requires
the final streamed byte count to equal that size. A chunk-consumer error stops
the transfer and preserves its root cause.

Successful return therefore proves:

```text
GetObject succeeded
AND optional ETag matched
AND Content-Length matched metadata size
AND every chunk consumer call succeeded
AND streamed byte count matched metadata size
```

The next gzip stage will provide the chunk consumer. Only after gzip and later
business processing complete should the surrounding object worker produce
`source.succeed()`.
