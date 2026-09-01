# flux

Kafka ingress v1 for the vehicle signal processor. It provides explicit
downstream acknowledgements, assignment-epoch fencing, continuous safe-prefix
offset tracking, two-dimensional backpressure, and fail-closed shutdown.

Copy `config.example.toml` to `config.toml`, adjust the broker, group, and topic,
then run:

```console
cargo run -- config.toml
```

`FLUX_CONFIG` can provide the config path when no command-line path is passed.
Set `RUST_LOG` to adjust logging. The current downstream implementation is a
`DiscardSink` used to prove the ingress contract; replace it with the real
processor while preserving the `Completion` protocol.

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
