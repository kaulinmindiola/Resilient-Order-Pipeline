---
status: accepted
date: 2026-10-05
decision-makers: KM
---

# ADR-0001: Publish the transactional outbox with an embedded polling publisher

## Context and Problem Statement

BR-008 and REQ-FUNC-004 require that an order and its outbox event are persisted in the same PostgreSQL transaction. Kafka cannot take part in that transaction (distributed transactions are excluded), so writing to the database and then sending to Kafka directly would be a *dual write*: a crash or a Kafka outage between the two steps loses the event or publishes an event for state that was never committed.

REQ-FUNC-010 requires pending events to be published every 5 seconds (configurable) and marked as published **only after** the broker acknowledges them. REQ-FUNC-011 requires unlimited retries while Kafka is unavailable, without losing an order that is already persisted.

Which mechanism reads `outbox_events` and publishes to Kafka?

## Decision Drivers

* REQ-FUNC-010 and REQ-FUNC-011: reliable publication that tolerates Kafka outages.
* No distributed transactions between PostgreSQL and Kafka.
* REQ-COST-001: zero extra infrastructure or cost.
* Proportionality: CQRS, Event Sourcing and similar machinery are out of scope.
* One instance per service, so no coordination between replicas is needed.

## Considered Options

* Embedded polling publisher (`@Scheduled` inside the service)
* Separate relay service dedicated to publishing the outbox
* Change Data Capture with Debezium on `outbox_events`
* Publish directly to Kafka after the database commit (no outbox relay)

## Decision Outcome

Chosen option: **"Embedded polling publisher"**, because it guarantees delivery without extra infrastructure. Debezium would require Kafka Connect, a connector and replication slots in PostgreSQL.

The publisher behaves as follows:

* A `@Scheduled` task runs every `OUTBOX_POLLING_INTERVAL_MS` (default `5000`).
* It reads pending rows (`published_at IS NULL`) ordered by `occurred_at`, served by the partial index `ix_outbox_pending`.
* It sends each event to the topic stored in the row, with key = `aggregateId` (see [ADR-0003](0003-partition-key-aggregate-id.md)).
* It waits synchronously for the broker acknowledgement (`send(...).get(15 s)`) and only then sets `published_at = now()`.
* On the first failure it stops the batch, preserving order; the remaining events are retried on the next cycle, without limit.

### Consequences

* Good, because delivery is guaranteed with no new infrastructure component.
* Good, because the publisher lives next to the domain that produces the events, in the same deployable.
* Good, because "ack, then mark" means an event can never be marked published without having been accepted by Kafka.
* Bad, because publication latency is up to the polling interval; acceptable, as there is no SLA (REQ-PERF-001 is a reference only).
* Bad, because if the send succeeds but marking `published_at` fails, the event is published again in a later cycle. Duplicates are absorbed by the idempotent consumer ([ADR-0011](0011-idempotent-consumer-single-local-transaction.md)).
* Bad, because only one instance per service is supported. Scaling out would require `FOR UPDATE SKIP LOCKED` or CDC, which is out of scope. The outbox is also never purged in v1.

### Confirmation

* Test 1 (*outbox atomicity*): order and event appear in the same commit; a forced rollback leaves zero rows in both tables.
* Test 2 (*publication*): the event appears in `order.created` within the polling interval, with key = `orderId` and a complete envelope.
* Integration test for republication when marking fails after a successful send.
* Test 7 (*Kafka down*): with the broker paused, orders still return `201`; once resumed, events are published without intervention.

Verified by [`OutboxPublisherTest`](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/OutboxPublisherTest.java)
(oldest first, ack-then-mark, the first failure ends the batch, retry on the next cycle,
republication when marking fails), by [`OutboxPublisherIT`](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/OutboxPublisherIT.java)
and [`OutboxRepublicationIT`](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/OutboxRepublicationIT.java)
against a real broker, and by [`OutboxKafkaOutageIT`](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/OutboxKafkaOutageIT.java):
with the broker paused, orders are still created and their events stay pending; once it resumes,
every event is published without intervention, and any duplicate carries the same `eventId`.
End to end, with the real scheduler, [`KafkaOutageSystemIT`](../../system-tests/src/test/java/io/github/kaulinmindiola/rop/system/KafkaOutageSystemIT.java)
shows that orders created while the broker is paused stay pending across several publisher cycles
and reach their terminal status on their own once it resumes, with stock decremented exactly once.

## Pros and Cons of the Options

### Embedded polling publisher

* Good, because it is the simplest option to build, deploy and operate.
* Good, because it reuses `outbox_events` directly.
* Neutral, because the scheduler shares the process with the service, which is irrelevant at this scale.
* Bad, because it does not give the lowest possible latency.

### Separate relay service

* Good, because it separates publishing from the domain service.
* Bad, because it adds a third deployable with no clear benefit at this scope.

### Change Data Capture with Debezium

* Good, because it offers minimal latency without polling.
* Bad, because it requires Kafka Connect, replication slots and an extra connector; the operational complexity is not justified here.

### Publish directly after commit

* Good, because it is trivial to write.
* Bad, because it is the dual-write problem itself: an outage or crash after the commit silently loses the event.

## More Information

* Requirements: BR-008, REQ-FUNC-004, REQ-FUNC-010, REQ-FUNC-011.
* The same pattern is reused in `inventory-service` ([ADR-0013](0013-outbox-in-inventory-service.md)), with a deliberately duplicated implementation ([ADR-0016](0016-independent-services-no-shared-module.md)).
* Topic and partition setup: [ADR-0003](0003-partition-key-aggregate-id.md) and [ADR-0012](0012-declarative-topics-three-partitions.md).
