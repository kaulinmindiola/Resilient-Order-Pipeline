# Resilient Order Pipeline

[![CI](https://github.com/kaulinmindiola/resilient-order-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/<tu-usuario>/resilient-order-pipeline/actions/workflows/ci.yml)

A reference backend for reliable, event-driven order processing: eventual consistency and fault tolerance across two services, built on Kafka and PostgreSQL.

## Status

**Under development. `v0.1.0` has not been released.**

The pipeline is implemented end to end, and every behaviour listed under
[Evidence](#evidence) is covered by an automated test that runs in CI. A quick start will be
added once the setup has been verified from a clean clone.

## The problem

Creating an order is a synchronous operation, but what comes after it (reserving stock, confirming or rejecting the order) happens asynchronously in another service. These steps cannot be wrapped in a single ACID transaction. The naive approach of "save to the database, then publish to Kafka" is a dual write: if the process crashes between the two steps, the order exists but the event is lost, and the order stays in `PENDING` forever.

This project explores how to keep the business flow coherent despite that gap, without distributed transactions.

## Evidence

Every claim of this README is backed by a test. Tests marked ★ are the core evidence of the project.
System tests run against the real `docker-compose.yml`, with the images built from source.

| # | What is demonstrated | Test | Requirement / decision |
|---|---|---|---|
| 1 | An order and its outbox event are committed or rolled back together | [`OrderOutboxAtomicityIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/application/OrderOutboxAtomicityIT.java) | REQ-FUNC-004, BR-008 |
| 2 | Outbox events reach Kafka keyed by order id, and are marked only after the acknowledgement | [`OutboxPublisherIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/OutboxPublisherIT.java) | REQ-FUNC-010, ADR-0001 |
| 3 | `OrderCreated` leads to `InventoryReserved` or `InventoryRejected` | [`OrderCreatedConsumerIT`](inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/OrderCreatedConsumerIT.java) | REQ-FUNC-012, REQ-FUNC-013 |
| 4 | Inventory outcomes drive the order to `CONFIRMED` or `REJECTED` | [`InventoryOutcomeConsumerIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/in/kafka/InventoryOutcomeConsumerIT.java) | REQ-FUNC-014, REQ-FUNC-015 |
| 5 | The full flow, end to end, through the public API | [`HappyPathSystemIT`](system-tests/src/test/java/io/github/kaulinmindiola/rop/system/HappyPathSystemIT.java) | REQ-FUNC-001..015 |
| 6 | Only a valid, unexpired token signed with our key gets through | [`AuthenticationIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/in/rest/AuthenticationIT.java), [`TokenEndpointIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/in/rest/TokenEndpointIT.java) | REQ-FUNC-007..009, ADR-0005 |
| 7 ★ | With Kafka down, orders are accepted and none is lost; they complete on their own once it returns | [`KafkaOutageSystemIT`](system-tests/src/test/java/io/github/kaulinmindiola/rop/system/KafkaOutageSystemIT.java), [`OutboxKafkaOutageIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/OutboxKafkaOutageIT.java) | REQ-FUNC-011, ADR-0001 |
| 8 ★ | The same event delivered twice has a single effect | [`DuplicateDeliveryIT`](inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/DuplicateDeliveryIT.java) | REQ-FUNC-016, ADR-0011 |
| 9 | A consumer killed with work in flight neither loses nor duplicates any effect | [`ConsumerCrashSystemIT`](system-tests/src/test/java/io/github/kaulinmindiola/rop/system/ConsumerCrashSystemIT.java) | ADR-0011 |
| 10 ★ | A failing message is retried with backoff, then dead-lettered; invalid messages are never retried | [`PoisonMessageIT`](inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/PoisonMessageIT.java) | REQ-REL-001, ADR-0004 |
| 11 ★ | Concurrent reservations never oversell and never deadlock | [`ConcurrentReservationIT`](inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/out/persistence/ConcurrentReservationIT.java) | BR-015, ADR-0010 |
| 12 | An invalid state transition is a recorded no-op, not an error | [`InvalidTransitionIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/in/kafka/InvalidTransitionIT.java) | BR-014, REQ-FUNC-019 |
| 13 | Events of one order land in one partition and are consumed in order | [`PartitionOrderingIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/PartitionOrderingIT.java) | ADR-0003, ADR-0012 |
| 14 | A stock rejection is committed as a result, decrements nothing and is never dead-lettered | [`CommittedRejectionIT`](inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/in/kafka/CommittedRejectionIT.java) | BR-015, ADR-0010 |
| 15 | Each service's database role cannot touch the other service's schema | [`SchemaIsolationIT`](order-service/src/test/java/io/github/kaulinmindiola/rop/order/SchemaIsolationIT.java) | ADR-0002, ADR-0015 |
| 16 | An order can be followed across services in JSON logs, and no secret is ever logged | [`StructuredLogsSystemIT`](system-tests/src/test/java/io/github/kaulinmindiola/rop/system/StructuredLogsSystemIT.java) | REQ-OBS-001, REQ-SEC-002 |
| — | The domain depends on no infrastructure | [`ArchitectureTest` (order)](order-service/src/test/java/io/github/kaulinmindiola/rop/order/ArchitectureTest.java), [`ArchitectureTest` (inventory)](inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/ArchitectureTest.java) | REQ-MAINT-001, ADR-0006 |

## Scope

A single release, `v0.1.0`, with no planned follow-up phases.

**This is not an e-commerce application.** The business domain is deliberately minimal; it exists only to give the event-driven patterns something real to operate on.

Out of scope: UI, payments, user management, OAuth2 or external identity providers, Kubernetes or cloud deployment, Saga frameworks, and end-to-end exactly-once delivery.

## Architecture at a glance

Two independent Spring Boot services, `order-service` and `inventory-service`, share a single PostgreSQL instance but each owns its own schema and database role, so neither can query the other's tables. They communicate exclusively through Kafka (KRaft mode, no ZooKeeper). Each service has its own outbox. Delivery is at-least-once, and correctness comes from consumer idempotency rather than from broker guarantees. PostgreSQL is the source of truth for all business state; Kafka is transport only.

The environment is designed to run as four containers (`order-service`, `inventory-service`, `postgres`, `kafka`) with a single `docker compose up --build`.

### Target flow

```mermaid
sequenceDiagram
    participant C as API client
    participant O as order-service
    participant K as Kafka
    participant I as inventory-service
    C->>O: POST /api/v1/orders
    O->>O: TX: order (PENDING) + outbox event
    O-->>C: 201 PENDING
    O->>K: order.created (polling publisher)
    K->>I: OrderCreated
    I->>I: TX: claim + reserve stock + outbox event
    I->>K: inventory.reserved | inventory.rejected
    K->>O: InventoryReserved | InventoryRejected
    O->>O: TX: claim + CONFIRMED | REJECTED
```

An order starts as `PENDING` and ends as `CONFIRMED` or `REJECTED`, both terminal.

## Key design decisions

Full records, including alternatives and consequences, live in [`docs/adr`](docs/adr/README.md).

| Decision | ADR |
|---|---|
| Outbox published by an embedded polling publisher | [0001](docs/adr/0001-transactional-outbox-polling-publisher.md) |
| One schema and one role per service in a single PostgreSQL instance | [0002](docs/adr/0002-schema-and-role-per-service.md) |
| Message key = aggregate ID, for per-order ordering | [0003](docs/adr/0003-partition-key-aggregate-id.md) |
| Retries with exponential backoff and dead-letter topics | [0004](docs/adr/0004-consumer-retry-and-dead-letter-topics.md) |
| Self-issued JWT validated by a Resource Server | [0005](docs/adr/0005-self-issued-jwt-with-resource-server.md) |
| Lightweight hexagonal architecture | [0006](docs/adr/0006-lightweight-hexagonal-architecture.md) |
| Stock reservation with a conditional `UPDATE` and savepoint | [0010](docs/adr/0010-atomic-conditional-update-for-stock-reservation.md) |
| Idempotent consumers in one local transaction | [0011](docs/adr/0011-idempotent-consumer-single-local-transaction.md) |
| Outbox in both services | [0013](docs/adr/0013-outbox-in-inventory-service.md) |
| Independent services, no shared module | [0016](docs/adr/0016-independent-services-no-shared-module.md) |

## Planned evidence

The core claims of this project are meant to be proven by tests, not asserted. This table will be completed with links and results as each test is implemented.

| Claim | Test | Status |
|---|---|---|
| Kafka down does not lose a persisted order | Kafka outage: orders return `201`, events publish after recovery | Planned |
| A duplicate event produces its effect once | Same `eventId` delivered twice, one stock decrement | Planned |
| A poison message ends in the DLT without blocking | 3 retries with backoff, then DLT; next messages processed | Planned |
| No overselling under concurrency | N concurrent reservations, stock for one, exactly one succeeds | Planned |

## Known limitations (by design)

- An order whose event reaches the DLT stays `PENDING`; there is no compensation or DLT replay.
- Reserved stock is never released (no cancellation or expiry).
- Publication latency is up to the polling interval (5 s by default), and the outbox is not purged.
- One instance per service; no horizontal scaling.
- `POST /auth/token` is a custom credential exchange, **not OAuth2**. HTTP only, no rate limiting.
- Single-node Kafka (replication factor 1) and a single shared PostgreSQL instance.

## Tech stack

- **Language:** Java 21
- **Framework:** Spring Boot 4.1.1 (Spring Framework 7)
- **Build:** Maven (multi-module) with Maven Wrapper
- **Database:** PostgreSQL 16, with Flyway migrations
- **Persistence:** JPA for the `Order` aggregate, `JdbcClient` for outbox, stock and technical tables
- **Messaging:** Apache Kafka 4.2.2 (KRaft) with Spring Kafka
- **Security:** Spring Security 7 Resource Server with self-issued JWT (HS256)
- **Logging:** Spring Boot structured logging (Logstash JSON format) with MDC
- **Testing:** JUnit Jupiter, AssertJ, Awaitility, ArchUnit and Testcontainers 2
- **Code style:** Spotless (google-java-format)
- **Environment:** Docker Compose
- **CI:** GitHub Actions and Dependabot

## Documentation

- [`docs/adr`](docs/adr/README.md): architecture decision records (ADR-0001 to ADR-0018)
- `docs/srs.md`: requirements specification *(planned)*
- `docs/sdd.md`: design description *(planned)*
- `docs/events.md`: event contracts *(planned)*

## Building from source

System tests start the real Docker Compose stack, so stop any running stack first:

```powershell
docker compose down
.\mvnw.cmd -Psystem-tests -pl system-tests verify      # Windows (PowerShell)
```

```bash
docker compose down
./mvnw -Psystem-tests -pl system-tests verify          # Linux / macOS
```

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).
