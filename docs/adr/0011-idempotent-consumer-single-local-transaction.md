---
status: accepted
date: 2026-10-05
decision-makers: KM
---

# ADR-0011: Make consumers idempotent inside a single local transaction

## Context and Problem Statement

Delivery is *at-least-once* everywhere: the outbox publisher can republish ([ADR-0001](0001-transactional-outbox-polling-publisher.md)) and a consumer can crash between the database commit and the offset commit. No component assumes exactly-once. BR-007 and REQ-FUNC-016 require that a consumer applies the same event only once, even when it is delivered twice or concurrently.

How does a consumer deduplicate events safely, and when is the Kafka offset committed?

## Decision Drivers

* BR-006 / BR-007 / REQ-FUNC-016: unique `eventId`, effect applied once per `(event_id, consumer)`.
* Correctness under concurrent delivery of the same event.
* No distributed transactions between Kafka and PostgreSQL.
* Invalid transitions and unknown aggregates must be quiet no-ops (BR-014, REQ-FUNC-019).

## Considered Options

* One local transaction: `claim` (`INSERT ... ON CONFLICT DO NOTHING`) + business effect + outbox write, with the offset committed after the commit (`AckMode.RECORD`)
* Check-then-insert (`SELECT` for existence, then `INSERT`)
* Commit the offset before processing
* Rely only on naturally idempotent state changes
* Kafka transactions / exactly-once semantics

## Decision Outcome

Chosen option: **"One local transaction with an insert-based claim"**, because the primary key resolves duplicates, including concurrent ones, with no extra locking logic.

```text
handle(event):
    @Transactional
        if not processedEvents.claim(event.eventId, CONSUMER):   # INSERT ... ON CONFLICT DO NOTHING
            log INFO "duplicate"; return                          # 0 rows = already processed
        outcome = applyBusinessEffect(event)                      # no-op if the transition is invalid
        if outcome.outgoingEvent present:                         # inventory-service only
            outbox.save(outcome.outgoingEvent)
    # the offset is committed after the transaction commits (AckMode.RECORD)
```

* `processed_events` has primary key `(event_id, consumer)`; `consumer` equals the consumer group name.
* For two concurrent deliveries of the same event, the second `INSERT` waits for the first transaction to finish and then inserts 0 rows.
* Auto-commit is disabled; the offset is acknowledged only after the database commit.
* An invalid transition, or an event for a non-existent aggregate, is recorded as processed, logged at WARN, and causes no exception, retry or DLT entry.

### Consequences

* Good, because the primary key also covers concurrent duplicates.
* Good, because committing the offset *after* the commit can only produce duplicates that are already absorbed, never lost effects.
* Good, because a rejection or outgoing event is atomic with the claim ([ADR-0010](0010-atomic-conditional-update-for-stock-reservation.md), [ADR-0013](0013-outbox-in-inventory-service.md)).
* Bad, because a crash between the commit and the offset commit causes reprocessing; the claim turns it into a no-op.
* Bad, because `processed_events` grows without bound; purging is out of scope.

### Confirmation

Verified by [`DuplicateDeliveryIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/DuplicateDeliveryIT.java)
(the same event delivered twice decrements stock once and writes one outcome),
[`ReserveInventoryAtomicityIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/application/ReserveInventoryAtomicityIT.java)
(a failure rolls back the claim together with the decrement, and the redelivery applies once),
[`CommittedRejectionIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/CommittedRejectionIT.java)
(a rejection commits with its claim) and [`ProcessedEventRepositoryIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/out/persistence/ProcessedEventRepositoryIT.java)
(a concurrent claim waits for the first transaction and is then a duplicate).

On the order side, [`ApplyInventoryOutcomeAtomicityIT`](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/application/ApplyInventoryOutcomeAtomicityIT.java)
shows that a failed transition rolls back its claim so the redelivery is applied, and
[`InvalidTransitionIT`](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/in/kafka/InvalidTransitionIT.java)
shows that duplicates, unknown orders and invalid transitions are recorded no-ops that never reach
the dead letter topic.

End to end, [`ConsumerCrashSystemIT`](../../system-tests/src/test/java/io/github/kaulinmindiola/rop/system/ConsumerCrashSystemIT.java)
kills `inventory-service` with SIGKILL while it has work in flight; after it restarts, every order is
confirmed, stock is decremented exactly once per order and each event is claimed exactly once.

## Pros and Cons of the Options

### Single local transaction with insert-based claim

* Good, because deduplication and effect commit or roll back together.
* Good, because it needs no locks beyond the primary key.
* Bad, because it adds one table and one insert per processed event.

### Check-then-insert

* Good, because it is intuitive.
* Bad, because two concurrent deliveries can both pass the check, so duplicates slip through.

### Commit the offset before processing

* Good, because it avoids reprocessing.
* Bad, because a crash after the offset but before the effect loses the event permanently.

### Natural idempotency only

* Good, because it needs no extra table.
* Bad, because a stock decrement is not idempotent, so it cannot protect the reservation.

### Kafka transactions / exactly-once

* Good, because it is a standard feature.
* Bad, because it is out of scope: Kafka must never participate in a database transaction.

## More Information

* Related: [ADR-0001](0001-transactional-outbox-polling-publisher.md), [ADR-0004](0004-consumer-retry-and-dead-letter-topics.md), [ADR-0010](0010-atomic-conditional-update-for-stock-reservation.md), [ADR-0013](0013-outbox-in-inventory-service.md).
* Requirements: BR-006, BR-007, BR-014, REQ-FUNC-016, REQ-FUNC-019.
