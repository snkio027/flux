# flux

Kafka ingress v1 for the vehicle signal processor. It provides explicit
downstream acknowledgements, assignment-epoch fencing, continuous safe-prefix
offset tracking, two-dimensional backpressure, and fail-closed shutdown.

Copy `config.example.toml` to `config.toml`, adjust the broker, group, and topic,
then run:

```console
cargo run -- config.toml
```

The file is optional: container deployments can supply the complete canonical
schema through `FLUX__...` environment variables. Values are resolved once at
startup in this order: compiled defaults, optional TOML file, environment
overrides. A CLI path takes precedence over `FLUX_CONFIG`; explicitly selected
files must exist, while the default `./config.toml` may be absent. See
[`docs/configuration-v2.md`](docs/configuration-v2.md) for the complete contract.

Set `RUST_LOG` to adjust logging. The current downstream implementation is a
`DiscardSink` used to prove the ingress contract; replace it with the real
processor through `run_with_sink`. Downstream code consumes each
`IngressRecord` into `record.succeed()` or `record.fail(reason)`; assignment
tokens remain private to the ingress correctness kernel.

The object-processing foundation accepts UTF-8 JSON Kafka payloads with
required `bucket`, `key`, and unsigned integer `size` fields plus an optional
opaque `etag`. It validates metadata inline, retains the source record through
the complete object-work lifetime, and distributes validated work over a
bounded Flume worker queue. See
[`docs/object-processing-foundation.md`](docs/object-processing-foundation.md)
for the frozen boundary and failure semantics.

`S3Downloader` is the concrete object transport boundary. It uses the standard
AWS credential provider chain, performs streaming `GetObject`, enforces request
and stream-idle timeouts, and verifies `Content-Length`, streamed byte count,
and optional ETag without aggregating the object in memory. It is intentionally
not installed as the default sink until gzip and DBC processing can consume the
stream. See [`docs/s3-streaming-processor.md`](docs/s3-streaming-processor.md).

The complete correctness contract is in
[`docs/kafka-ingress-v1.md`](docs/kafka-ingress-v1.md).

## Quality gates

The repository pins Rust 1.98 and forbids unsafe code, implicit unwrap/expect,
and Clippy warnings. Pull requests run formatting, strict Clippy, unit tests,
the in-process mock Kafka test, and an ignored-by-default Docker-backed real
Kafka crash/restart replay test. Run the real Kafka gate locally with:

```console
cargo test --locked --test kafka_real -- --ignored --nocapture
```
