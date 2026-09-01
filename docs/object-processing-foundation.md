# Object processing foundation

This phase establishes the boundary between Kafka ingress and object work. It
does not implement S3 transport, gzip framing, or DBC decoding.

## Wire contract

The Kafka value is a UTF-8 JSON object. The v1 fields are:

| Field | JSON type | Required | Meaning |
| --- | --- | --- | --- |
| `bucket` | string | yes | S3 bucket identity |
| `key` | string | yes | Exact S3 object key |
| `size` | unsigned integer | yes | Expected object size in bytes |
| `etag` | string or null | no | Opaque object identity evidence |

Example:

```json
{
  "bucket": "vehicle-signals",
  "key": "2026/09/02/example.dbc.gz",
  "size": 123456,
  "etag": "\"abc123\""
}
```

There is no `version_id` field. Unknown fields are ignored so producers can add
backward-compatible metadata without breaking v1 consumers. Required fields
remain strict: missing or mistyped values fail decoding.

The decoder rejects a missing payload, malformed JSON, empty bucket or key,
zero size, a size above `object_processing.max_object_size`, an empty optional
ETag, and null characters in identity fields. It performs no network access.

## Ownership and completion

```text
IngressRecord
    -> decode and validate
ObjectWorkItem { ObjectMetadata, IngressRecord }
    -> bounded queue
one object worker
    -> processor success: source.succeed()
    -> any failure:      source.fail(reason)
```

`ObjectWorkItem` owns the original `IngressRecord`; it never exposes the
ingress-private delivery token. A decode failure also retains the record until
the dispatcher produces an explicit failed completion. Kafka therefore cannot
advance an offset merely because metadata or queued work was dropped.

## Queue and worker semantics

`object_processing.queue_capacity` bounds a Flume MPMC queue. The single
dispatcher decodes metadata inline and awaits queue capacity. Waiting work
continues to hold its ingress byte-budget permit, so this queue does not bypass
the existing memory bound.

Exactly `object_processing.worker_count` workers receive from the queue. Each
worker handles one object at a time. Completion order is intentionally not
ordered; the Kafka safe-prefix tracker remains responsible for advancing only
over a contiguous successful prefix.

When Kafka closes its work channel during graceful shutdown, the dispatcher
closes the object queue and workers drain accepted work. A decode or processor
failure is fail-fast: the exact record receives a failed completion, sibling
tasks stop, and uncompleted offsets remain replayable.

## Next integration

The S3 implementation will inject the concrete processor behind this worker
boundary. `aws_sdk_s3::Client` will remain inside `object_store::s3`; no generic
object-store trait is introduced. S3 GET must be streaming and must verify the
declared size and optional ETag before gzip/DBC processing can complete the
source record successfully.
