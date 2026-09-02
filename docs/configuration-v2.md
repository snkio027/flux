# Configuration v2

Flux resolves an immutable `AppConfig` exactly once, before any ingress or
processing task starts. Configuration changes take effect through a process
restart or deployment rollout; there is no hot reload.

## Source precedence

Values are resolved from lowest to highest priority:

```text
compiled defaults
    ↓
optional TOML file
    ↓
FLUX__ environment overrides
    ↓
strict schema deserialization and validation
```

The configuration file path is selected separately:

```text
CLI path
    >
FLUX_CONFIG
    >
./config.toml when it exists
    >
no file
```

A CLI or `FLUX_CONFIG` path is explicit and must exist. Only the default
`./config.toml` is optional. Environment values override individual values
from the selected file; they do not replace unrelated sections.

## Environment contract

Double underscores delimit schema levels. Field underscores remain intact:

```text
FLUX__KAFKA__BOOTSTRAP_SERVERS
FLUX__KAFKA__GROUP_ID
FLUX__KAFKA__TOPICS
FLUX__KAFKA__AUTO_OFFSET_RESET
FLUX__INGRESS__WORK_QUEUE_CAPACITY
FLUX__OBJECT_PROCESSING__WORKER_COUNT
FLUX__S3__ENDPOINT_URL
FLUX__S3__FORCE_PATH_STYLE
FLUX__SHUTDOWN__GRACE_MS
```

`kafka.bootstrap_servers` and `kafka.topics` accept comma-separated values.
Elements are trimmed, and empty elements fail validation. Their lexical form is
preserved, so singleton names such as `001`, `123`, and `true` remain strings.
Other strings are not split. Boolean, integer, and enum values are converted
only when their schema fields require those types; invalid values fail startup.

The minimum env-only configuration is:

```console
FLUX__KAFKA__BOOTSTRAP_SERVERS=kafka-0:9092,kafka-1:9092
FLUX__KAFKA__GROUP_ID=flux
FLUX__KAFKA__TOPICS=vehicle-object-metadata
FLUX__KAFKA__AUTO_OFFSET_RESET=earliest
```

Kafka bootstrap servers, group ID, topics, and offset-reset policy remain
required. Unknown TOML fields and unknown `FLUX__...` keys fail startup. Legacy
runtime settings under `[kafka]` are no longer accepted; use `[ingress]` and
`[shutdown]` or their matching environment paths.

## Credential boundary

Flux configuration contains S3 behavior such as region, endpoint, path style,
retry limits, and timeouts. AWS credentials are intentionally absent from
`AppConfig`. The AWS SDK provider chain owns variables and identity sources
such as:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
AWS_ROLE_ARN
AWS_WEB_IDENTITY_TOKEN_FILE
```

Do not create `FLUX__S3__ACCESS_KEY` or `FLUX__S3__SECRET_KEY`; unknown keys are
rejected. Flux never logs the merged configuration or the complete process
environment.
