---
status: accepted
date: 2026-10-05
decision-makers: KM
---

# ADR-0004: Handle consumer failures with DefaultErrorHandler, exponential backoff and dead-letter topics

## Context and Problem Statement

REQ-REL-001 requires that a consumer failure is retried three times with backoff of 1 s, 2 s and 4 s and then routed to `<topic>.DLT`, without blocking the processing of later messages indefinitely. Deterministic errors (malformed JSON, payload outside the contract, unknown `eventType`) must go straight to the DLT without retries. Which mechanism implements this in Spring Kafka, and how are record values serialized?

## Decision Drivers

* REQ-REL-001: explicit numeric requirement (1 delivery + 3 retries, 1 s / 2 s / 4 s).
* A poison message must never block a partition forever.
* Minimal custom retry code.
* A deterministic path to the DLT, with no extra retry topics.
* Domain code must never see JSON ([ADR-0006](0006-lightweight-hexagonal-architecture.md)).

## Considered Options

* `DefaultErrorHandler` with exponential `BackOff` + `DeadLetterPublishingRecoverer`
* `@RetryableTopic` (cascading retry topics) + DLT
* Manual retry loop inside the listener
* No retry: log and skip

## Decision Outcome

Chosen option: **"`DefaultErrorHandler` + `DeadLetterPublishingRecoverer`"**, because it matches the requirement exactly with a proven Spring Kafka mechanism and without adding retry topics.

* Backoff: 3 retries after the first delivery, at 1 s, 2 s and 4 s. When exhausted, the record is published to `<topic>.DLT` and the consumer continues.
* `InvalidEventException` is classified as not retryable and goes directly to the DLT.
* Record values are plain `String` (`StringSerializer` / `StringDeserializer`). Each service has its own `EventEnvelopeMapper`, which builds and parses the envelope with Jackson. Invalid JSON, an out-of-contract payload or an unknown `eventType` raises `InvalidEventException`.
* `DeadLetterPublishingRecoverer` publishes to the same partition as the original record, hence the DLTs also have 3 partitions ([ADR-0012](0012-declarative-topics-three-partitions.md)).

### Consequences

* Good, because the exact backoff sequence is directly configurable.
* Good, because it is native Spring Kafka with minimal custom code.
* Good, because the DLT route is deterministic and keeps the original key.
* Bad, because the partition is blocked during the backoff window (about 7 s per failed message). Acceptable at this scope; true non-blocking retries would require `@RetryableTopic`.
* Bad, because a message that reaches the DLT leaves its order in `PENDING` indefinitely. This is a declared limitation: there is no compensation and no DLT replay in v1.

### Confirmation

Verified by [`PoisonMessageIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/PoisonMessageIT.java):
a message that keeps failing receives 4 attempts with gaps of about 1, 2 and 4 seconds, lands
unchanged on `order.created.DLT` in the same partition, and the next message on that partition is
then processed; an invalid message is parsed once and dead-lettered without retries.
[`OrderCreatedEventParserTest`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/OrderCreatedEventParserTest.java)
covers every contract violation that is treated as non-retryable.

## Pros and Cons of the Options

### DefaultErrorHandler + DLT recoverer

* Good, because it matches the numeric requirement with no custom retry code.
* Good, because the error classification is explicit.
* Bad, because it blocks the partition during backoff.

### `@RetryableTopic`

* Good, because it is non-blocking.
* Bad, because it introduces extra retry topics that are not part of the topic model, increasing operational surface without need.

### Manual retry loop

* Good, because it gives full control.
* Bad, because it reinvents what Spring Kafka already solves and needs `Thread.sleep` inside a listener thread.

### No retry

* Good, because it is the simplest.
* Bad, because transient failures lose effects, and it contradicts REQ-REL-001.

## More Information

* Related: [ADR-0003](0003-partition-key-aggregate-id.md), [ADR-0011](0011-idempotent-consumer-single-local-transaction.md), [ADR-0012](0012-declarative-topics-three-partitions.md).
* Requirements: REQ-REL-001, BR-014, REQ-FUNC-019.
