---
status: accepted
date: 2026-10-05
decision-makers: KM
---

# ADR-0013: Use a transactional outbox in inventory-service as well

## Context and Problem Statement

`inventory-service` consumes `OrderCreated` and must emit `InventoryReserved` or `InventoryRejected`. The stock decrement (or the rejection) and the outgoing event must be consistent. If the service published to Kafka right after its own commit, it would reintroduce the dual-write problem that [ADR-0001](0001-transactional-outbox-polling-publisher.md) removes in `order-service`.

REQ-FUNC-020 requires the reservation or rejection and its outgoing event to be written in one local transaction, with no `send()` from the use case.

## Decision Drivers

* REQ-FUNC-020 and BR-008: state change and outgoing event are atomic in both services.
* No distributed transactions.
* The consumer transaction already contains the idempotency claim ([ADR-0011](0011-idempotent-consumer-single-local-transaction.md)).
* The application layer must not know about Kafka ([ADR-0006](0006-lightweight-hexagonal-architecture.md)).

## Considered Options

* Outbox in `inventory-service`, same pattern as `order-service`
* Publish to Kafka from the listener after the commit
* Publish to Kafka inside the transaction (`KafkaTemplate` in the use case)
* Kafka transactions chained to the database transaction

## Decision Outcome

Chosen option: **"Outbox in `inventory-service`"**, because publishing after the commit reintroduces the dual write.

* `ReserveInventoryService` writes the stock change, the `processed_events` claim and the `outbox_events` row in one transaction.
* The inventory publisher mirrors the one in `order-service` and is configured the same way: polling, ack-then-mark, key = `aggregateId`.
* No `EventPublisher` port exists in either service.

### Consequences

* Good, because a reservation or rejection and its event can never diverge.
* Good, because both services follow the same, uniform reliability model.
* Bad, because there is a second publisher to maintain, which is kept in sync by equivalent tests ([ADR-0016](0016-independent-services-no-shared-module.md)).
* Bad, because inventory events also inherit the polling latency and the one-instance-per-service limit.

### Confirmation

Verified by [`ReserveInventoryIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/application/ReserveInventoryIT.java)
(the outcome event is written in the same transaction as the reservation) and by
[`OrderCreatedConsumerIT`](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/OrderCreatedConsumerIT.java)
(the outcome reaches `inventory.reserved` or `inventory.rejected` through this service's own
outbox publisher, which is byte-identical to order-service's apart from its package).

## Pros and Cons of the Options

### Outbox in `inventory-service`

* Good, because it is atomic and consistent with `order-service`.
* Bad, because it duplicates the publisher.

### Publish after commit

* Good, because it is simple.
* Bad, because a crash after the commit loses the event: the dual-write problem.

### Publish inside the transaction

* Good, because it is simple to write.
* Bad, because a rollback after the send leaves a phantom event in Kafka, and the application layer would know about Kafka.

### Kafka transactions chained to the database

* Good, because it is a documented mechanism.
* Bad, because it is out of scope and does not give atomicity across both systems.

## More Information

* Related: [ADR-0001](0001-transactional-outbox-polling-publisher.md), [ADR-0010](0010-atomic-conditional-update-for-stock-reservation.md), [ADR-0011](0011-idempotent-consumer-single-local-transaction.md), [ADR-0016](0016-independent-services-no-shared-module.md).
* Requirements: REQ-FUNC-020, BR-008.
