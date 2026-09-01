# Kafka Ingress v1

This module provides at-least-once Kafka ingress for the vehicle signal
processor. Its central rule is that the application may only store and commit a
partition's continuously completed delivery prefix.

## State model

```text
Kafka record -> DELIVERED -> COMPLETED -> STORED -> COMMITTED
```

- `DELIVERED`: owned by downstream work and still replayable.
- `COMPLETED`: downstream succeeded, but an earlier delivered record may still
  be incomplete.
- `STORED`: part of the safe delivery prefix and written to librdkafka's local
  offset store.
- `COMMITTED`: persisted as consumer-group state at the broker.

`enable.auto.offset.store=false` prevents receipt from implying completion.
Every safe-prefix advance calls `store_offsets()` immediately with an explicit
next offset. The singular `store_offset()` API is intentionally not used: it is
a legacy wrapper that adds one to its argument. Normal broker
persistence remains batched by `enable.auto.commit=true` and
`auto.commit.interval.ms`. Shutdown alone makes an explicit synchronous commit
from the tracker's safe snapshot.

## Invariants

- `KAFKA-001`: Receiving or delivering a record never advances its offset.
- `KAFKA-002`: Only a successful downstream completion may close a delivery.
- `KAFKA-003`: A stored or committed offset is the next offset after the safe
  record, never the record's own offset.
- `KAFKA-004`: Prefix continuity is based on records actually delivered by the
  consumer, not numeric Kafka offset continuity.
- `KAFKA-005`: Every delivery carries an immutable topic, partition, record
  offset, and assignment epoch.
- `KAFKA-006`: A completion from a revoked or replaced epoch is stale and must
  not update the offset store.
- `KAFKA-007`: Any downstream failure stops new delivery, leaves the failed
  record incomplete, and initiates fail-closed shutdown.
- `KAFKA-008`: The bounded work channel owns the message-count budget.
- `KAFKA-009`: Each queued record owns a byte-budget permit until downstream
  releases that record.
- `KAFKA-010`: Shutdown stops new delivery, closes and drains downstream work,
  drains completions, then commits only the resulting safe snapshot.
- `KAFKA-011`: Assignment epochs are created and invalidated synchronously with
  librdkafka's assignment lifecycle, not by asynchronous event ordering.
- `KAFKA-012`: Pausing partition data delivery never stops the consumer event
  loop from polling and servicing the group protocol.
- `KAFKA-013`: Every safe-prefix advance immediately updates librdkafka's local
  offset store; only broker persistence may be delayed.
- `KAFKA-014`: A record entering the work queue already owns both message-count
  capacity and byte-budget capacity.
- `KAFKA-015`: The final synchronous offset set is derived from the
  `OffsetTracker` safe snapshot, never the last received position.

## Rebalance split of responsibility

The `KafkaContext` callback only changes the small synchronized
`AssignmentRegistry` and publishes a lifecycle notification. It does not wait
for workers, drain queues, or contact the broker. The runner owns business
tracking and treats notifications as cleanup hints; when a message arrives it
can align the tracker directly from the registry, so correctness does not
depend on channel scheduling.

## Backpressure

The runner first tries to acquire a work-channel slot and the record's accounted
bytes. Accounting includes key, payload, header keys, header values, and a fixed
per-record overhead. When either capacity is unavailable, current partitions
are paused while the runner continues polling Kafka alongside completions,
rebalance events, and shutdown. Capacity is acquired before the record is sent,
then current assignments are resumed.

The memory budget must fit in `u32` because Tokio's multi-permit semaphore API
uses a `u32` permit count. A single record larger than the configured total
budget fails closed rather than waiting forever.

## Failure and shutdown policy

v1 deliberately has no retry scheduler. `Completion::Failed` is fatal. Adding a
retryable flag without a retry state machine would promise behavior that does
not exist. A future revision can introduce explicit retry, quarantine, and fatal
dispositions.

Shutdown uses this sequence:

```text
PAUSE DATA
-> CLOSE WORK QUEUE
-> DRAIN COMPLETIONS WHILE POLLING KAFKA
-> BUILD CURRENT SAFE SNAPSHOT
-> STORE SNAPSHOT
-> COMMIT SNAPSHOT SYNCHRONOUSLY
-> EXIT
```
