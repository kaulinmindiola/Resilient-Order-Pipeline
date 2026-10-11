# Software Design Description
## For Event-Driven Order Platform

Version 1
Prepared by KM
September 10, 2026

## Table of Contents
<!-- TOC -->
* [1. Introduction](#1-introduction)
    * [1.1 Document Purpose](#11-document-purpose)
    * [1.2 Subject Scope](#12-subject-scope)
    * [1.3 Definitions, Acronyms, and Abbreviations](#13-definitions-acronyms-and-abbreviations)
    * [1.4 References](#14-references)
    * [1.5 Document Overview](#15-document-overview)
* [2. Design Overview](#2-design-overview)
    * [2.1 Stakeholder Concerns](#21-stakeholder-concerns)
    * [2.2 Selected Viewpoints](#22-selected-viewpoints)
* [3. Design Views](#3-design-views)
* [4. Decisions](#4-decisions)
* [5. Appendixes](#5-appendixes)
<!-- TOC -->

## Revision History

| Name | Date | Reason For Changes | Version |
|------|------|--------------------|---------|
| Kaulin Mindiola | 2026-09-10 | Initial baseline, derived from SRS v1 and the 10 accepted decisions | 0.1 |
| Kaulin Mindiola | 2026-10-04 | final version | 1 |

## 1. Introduction

### 1.1 Document Purpose

This SDD describes **how** the Event-Driven Order Platform v1 is designed to satisfy the requirements specified in `docs/srs.md`, translating the architectural decisions recorded individually in the `docs/adr/` directory (ADR-0001 through ADR-0018, all accepted) into a concrete structure of components, interfaces, data, and behavior. Its primary audience is the author â€” acting as implementer â€” and any technical portfolio reviewer evaluating the rigor of the design. The document contains enough detail to begin implementation without descending to the level of source code.

### 1.2 Subject Scope

**Event-Driven Order Platform**, version v1, single release. The design fully covers the system defined in the SRS: two proprietary services (`order-service`, `inventory-service`) that collaborate asynchronously via Apache Kafka, with PostgreSQL as the shared source of truth (separate schemas per domain, ADR-0002), the Transactional Outbox pattern (ADR-0001), and idempotent consumers. It includes the design of: REST and event contracts, data model, JWT authentication, failure handling (Kafka down, retries, DLT), and deployment topology via Docker Compose. It explicitly excludes everything marked as Out-of-Scope in the SRS (UI, user management, external IdP, Kubernetes, CQRS/Event Sourcing, `notification-service`).

### 1.3 Definitions, Acronyms, and Abbreviations

| Term | Definition |
|------|------------|
| Adapter | An infrastructure component that implements a domain Port (e.g., a JPA repository implementing `OrderRepository`). |
| Aggregate | See SRS 1.3. |
| Bounded Context | An explicit boundary within which a domain model (entities, rules) is valid and consistent; in this system, `order-service` and `inventory-service` are separate bounded contexts. |
| Consumer Group | A logical grouping of instances of a Kafka consumer that share the partitions of a topic. |
| DLT | See SRS 1.3 (Dead Letter Topic). |
| Idempotent Consumer | See SRS 1.3. |
| JWT | See SRS 1.3. |
| KRaft | An operating mode of Apache Kafka that eliminates the Zookeeper dependency, using Kafka's own consensus protocol for metadata (see ADR-0007). |
| MDC | Mapped Diagnostic Context â€” an SLF4J/Logback mechanism for propagating structured fields (e.g., `orderId`, `eventId`) across the logs of a single thread of execution. |
| Outbox Pattern | See SRS 1.3. |
| Partition Key | The value used by the Kafka producer to determine the destination partition of a message; in this system it is always `aggregateId` (ADR-0003). |
| Port | An interface defined by the domain that abstracts an external dependency (persistence, messaging), implemented by an Adapter (see ADR-0006). |

### 1.4 References

| Title | Owner | Version | Location | Type |
|---|---|---|---|---|
| SRS â€” Event-Driven Order Platform | Kaulin Mindiola | 1 | `docs/srs.md` | Normative |
| ADR â€” Event-Driven Order Platform | Kaulin Mindiola | ADR-0001..018 | `docs/adr/` | Normative (decisions implemented by this design) |
| RFC 7807 â€” Problem Details for HTTP APIs | IETF | RFC 7807 | https://www.rfc-editor.org/rfc/rfc7807 | Normative (error format) |

### 1.5 Document Overview

Section 2 (Design Overview) identifies stakeholder concerns and selects, with justification, the viewpoints from the `sdd-template.md` catalog that actually add value to this system. Section 3 (Design Views) contains the concrete views â€” diagrams and descriptions â€” organized by viewpoint, each referencing the SRS REQ-* requirements it implements. Section 4 (Decisions) summarizes the 10 accepted decisions, deferring to `MADR.md` as the detailed source without duplicating its content. Section 5 (Appendixes) contains non-normative supporting material: an environment variable reference and a traceability matrix.

## 2. Design Overview

This system is an event-driven reference backend, with two services collaborating asynchronously under eventual consistency. The design approach prioritizes: (a) demonstrating the patterns required by the SRS (Outbox, Idempotent Consumer) in a verifiable way; (b) keeping the domain isolated from messaging technology (ADR-0006); and (c) avoiding complexity not justified by the portfolio scope (no CQRS, no Event Sourcing, no external observability infrastructure).

### 2.1 Stakeholder Concerns

| Stakeholder | Concern | Addressed by (viewpoint) |
|---|---|---|
| Developer/Implementer (the author) | Understand the structure well enough to implement it directly | Composition, Logical, Dependency, Interaction |
| Technical portfolio reviewer | Evaluate engineering rigor and the coherence between decisions and design | Patterns, Section 4 (Decisions), ADR references |
| API Client | Have stable input/output contracts | Interface |
| Operator (the author, in local mode) | Be able to start and observe the system without friction | Deployment |
| All of the above | Trust that eventual consistency and fault tolerance work as documented | Information, State Dynamics, Concurrency, Algorithm |

### 2.2 Selected Viewpoints

The 15 viewpoints in the `sdd-template.md` catalog were evaluated. Twelve were selected because they address a real, verifiable concern in this system; the remaining 3 are marked `N/A` with their justification, to avoid producing views with no added value.

#### 2.2.1 Context

**Selected.** The system has a single external actor (API Client) and no third-party systems â€” it is worth fixing that boundary explicitly. See DV-001.

#### 2.2.2 Composition

**Selected.** There are 4 physical deployable pieces (order-service, inventory-service, PostgreSQL, Kafka) whose relationship is not trivial to infer from the SRS alone. See DV-002.

#### 2.2.3 Logical

**Selected.** ADR-0006 requires an explicit domain/infrastructure separation via ports; this view makes those ports tangible. See DV-003, DV-004.

#### 2.2.4 Physical

**N/A.** There is no complex hardware or network topology â€” everything runs in local Docker containers on a single machine; cloud infrastructure is explicitly out of scope (SRS, Out-of-Scope).

#### 2.2.5 Structure

**N/A.** No entity in this system has an internal composition (fine-grained ports/connectors) that is not already covered by Composition and Logical.

#### 2.2.6 Dependency

**Selected.** REQ-MAINT-001 is literally a dependency-direction requirement (the domain must not import Kafka); this view makes it explicit and verifiable. See DV-005.

#### 2.2.7 Information

**Selected.** The data model (orders, order_items, outbox_events, processed_events, stock) is central to BR-009/BR-010. See DV-006.

#### 2.2.8 Interface

**Selected.** REQ-MAINT-002 requires documented event contracts; in addition, the REST contracts must be fully specified for the API Client. See DV-007, DV-008.

#### 2.2.9 Interaction

**Selected.** It is the heart of an event-driven system: both the happy path and the failure paths (Kafka down, duplicates, DLT) must be shown. See DV-009 through DV-012 and DV-019.

#### 2.2.10 Algorithm

**Selected.** Two algorithms are critical to the correctness of the system and non-trivial: idempotent deduplication and atomic stock reservation. See DV-013, DV-014.

#### 2.2.11 State Dynamics

**Selected.** The order lifecycle (PENDING/CONFIRMED/REJECTED) is an explicit business requirement (BR-003 through BR-005, BR-011). See DV-015.

#### 2.2.12 Concurrency

**Selected.** ADR-0001 (single publisher instance) and ADR-0010 (atomic UPDATE) are concurrency guarantees that deserve to be made explicit, not merely implicit in the code. See DV-016.

#### 2.2.13 Patterns

**Selected.** One of the project's explicit goals (Master Context, Goals) is to demonstrate specific patterns; this view enumerates and references them. See DV-017.

#### 2.2.14 Deployment

**Selected.** REQ-INST-001 requires a reproducible environment with a single command; this view documents the container topology. See DV-018.

#### 2.2.15 Resources

**N/A.** There are no shared-resource constraints relevant to this scope (no production SLA, single instance of each service, no contention documented in the SRS). If connection pooling to be tuned by load were introduced in the future, this decision would be revisited.

## 3. Design Views

### Diseño del Consumer de `inventory-service`

- **Parser de entrada y separación de mappers:** El servicio implementa un parser dedicado para la entrada (`OrderCreatedEventParser`), totalmente desacoplado del mapper de eventos salientes de outbox. Cualquier estructura inválida o violación de contrato en el mensaje recibido se traduce a `InvalidEventException`, clasificada como no reintentable.
- **Resolutor explícito del DLT:** Se utiliza un `DeadLetterPublishingRecoverer` configurado para enrutar los mensajes venenosos (poison messages) o aquellos que agoten la política de reintentos hacia el tópico de destino `<topic>.DLT`, manteniendo estrictamente el mismo número de partición del registro original.
- **Política de consumo (`AckMode.RECORD` y `auto-offset-reset: earliest`):**
    - `auto.offset.reset = earliest`: Asegura que si el consumer group no posee un offset previo registrado, la lectura inicie desde el principio de las particiones disponibles.
    - `AckMode.RECORD`: El reconocimiento (acknowledgement) del offset se realiza de forma individual por cada registro procesado de manera exitosa tras completarse la transacción local de base de datos.
- **Control explícito del ciclo de vida en Tests de Integración (IT):** Los listeners se inician y detienen explícitamente mediante `KafkaListenerEndpointRegistry` en los métodos `@BeforeEach` y `@AfterEach` de las clases base de pruebas (como `AbstractListenerIT`). Esto garantiza que solo un consumer del grupo permanezca activo a la vez, evitando conflictos de concurrencia cuando Spring mantiene múltiples contextos de pruebas en caché.

### 3.1 Context

```markdown
- ID: DV-001
- Title: System Context Diagram
- Viewpoint: Context
- Representation:
```

```mermaid
graph TB
    Client["API Client<br/>(script, Postman, test suite)"]
    subgraph Platform["Event-Driven Order Platform"]
        OS[order-service]
        IS[inventory-service]
        PG[(PostgreSQL)]
        KF[[Kafka]]
    end
    Client -- "REST + JWT Bearer" --> OS
    OS -- "OrderCreated" --> KF
    KF -- "OrderCreated" --> IS
    IS -- "InventoryReserved / InventoryRejected" --> KF
    KF -- "InventoryReserved / InventoryRejected" --> OS
    OS --- PG
    IS --- PG
```

```markdown
- More Information: Implements UC-01 through UC-08 of the SRS. There are no third-party external systems (self-issued authentication, no IdP). Source: SRS 2.1, 2.4.
```

### 3.2 Composition

```markdown
- ID: DV-002
- Title: Container Diagram (C4)
- Viewpoint: Composition
- Representation:
```

```mermaid
graph TB
    Client((API Client))
    subgraph "order-service (Spring Boot 4 , :8080)"
        OSApp[REST API + Outbox Publisher + JWT Issuer]
    end
    subgraph "inventory-service (Spring Boot 4)"
        ISApp[Kafka Consumer + Reservation Logic]
    end
    PG[("PostgreSQL 16<br/>schemas: order_service, inventory_service")]
    KF[["Kafka (KRaft, :9092)<br/>topics: order.created, inventory.reserved, inventory.rejected + .DLT"]]

    Client -->|HTTPS/HTTP + JWT| OSApp
    OSApp -->|JDBC| PG
    ISApp -->|JDBC| PG
    OSApp -->|produce/consume| KF
    ISApp -->|produce/consume| KF
```

```markdown
- More Information: Each container implements REQ-INST-001 (Docker Compose). Full topology detail in DV-018. Related to ADR-0002 (separate schemas) and ADR-0007 (Kafka KRaft).
```

### 3.3 Logical

```markdown
- ID: DV-003
- Title: Domain Ports â€” order-service
- Viewpoint: Logical
- Representation:
```

```mermaid
classDiagram
    class Order {
        +OrderId id
        +OrderStatus status
        +List~OrderItem~ items
        +Money total
        +confirm()
        +reject()
    }
    class OrderItem {
        +ProductId productId
        +int quantity
        +Money unitPriceSnapshot
    }
    class OrderStatus {
        <<enumeration>>
        PENDING
        CONFIRMED
        REJECTED
    }
    class OrderRepository {
        <<interface>>
        +findById(OrderId) Order
        +save(Order) void
    }
    class OutboxEventRepository {
        <<interface>>
        +save(OutboxEvent) void
        +findPending() List~OutboxEvent~
        +markPublished(eventId) void
    }
    class OutboxEvent {
        +UUID eventId
        +String eventType
        +String aggregateId
        +String payload
        +Instant occurredAt
        +Instant publishedAt
    }
    Order "1" --> "many" OrderItem
    Order --> OrderStatus
    OrderRepository ..> Order
    OutboxEventRepository ..> OutboxEvent
```

```markdown
- More Information: Implements REQ-FUNC-003/004/005. `OutboxEventRepository` is consumed both by the application service (to persist within the same transaction, ADR-0001) and by the `OutboxPublisher` adapter (to read pending events). No type in this diagram depends on Kafka or JPA (ADR-0006). Unlike inventory-service (DV-004), the order-service domain does **not** define an `EventPublisher` port: it never publishes directly to Kafka, it only writes to `OutboxEventRepository` â€” the actual publishing happens entirely in the `OutboxPublisher` adapter (see DV-019).
```

```markdown
- ID: DV-004
- Title: Domain Ports â€” inventory-service
- Viewpoint: Logical
- Representation:
```

```mermaid
classDiagram
    class Stock {
        +ProductId productId
        +int quantity
    }
    class StockRepository {
        <<interface>>
        +tryReserve(ProductId, int) boolean
    }
    class ProcessedEventRepository {
        <<interface>>
        +hasProcessed(eventId, consumer) boolean
        +markProcessed(eventId, consumer) void
    }
    class EventPublisher {
        <<interface>>
        +publishReserved(orderId) void
        +publishRejected(orderId, reason) void
    }
    StockRepository ..> Stock
```

```markdown
- More Information: `tryReserve` encapsulates the atomic conditional UPDATE of ADR-0010 (DV-014). `ProcessedEventRepository` implements the `processed_events` table of REQ-FUNC-016 (DV-013). `EventPublisher` is invoked by `InventoryReservationService` (application layer) after resolving the reservation, and implemented by a `KafkaEventPublisherAdapter` (see DV-005, DV-009, DV-011).
```

### 3.4 Dependency

```markdown
- ID: DV-005
- Title: Allowed Import Directions (Package Dependency Graph)
- Viewpoint: Dependency
- Representation:
```

```mermaid
graph LR
    subgraph domain["domain (pure)"]
        D1["Order / Stock / OrderStatus"]
        D2["Ports: OrderRepository, StockRepository,<br/>OutboxEventRepository, ProcessedEventRepository,<br/>EventPublisher"]
    end
    subgraph application["application"]
        A1["OrderService / InventoryReservationService"]
    end
    subgraph adapters["adapters (infrastructure)"]
        AJ["JPA Adapters"]
        AK["Kafka Adapters<br/>(Producer, Listener, OutboxPublisher,<br/>OutboxSchedulingConfiguration)"]
        AR["REST Controllers"]
        AS["Security/JWT Adapter"]
    end
    application --> domain
    adapters --> domain
    adapters --> application
```

```markdown
- More Information: Implements REQ-MAINT-001 / ADR-0006. Rule verifiable by inspection: no type in the `domain` package imports `org.apache.kafka.*`, `org.springframework.kafka.*`, or JPA annotations (`jakarta.persistence.*`). Arrows only point toward `domain`, never the other way around. The `EventPublisher` port exists only in the inventory-service domain (DV-004); order-service does not need it, since it never publishes directly to Kafka â€” it only writes to `OutboxEventRepository` (DV-003).
```

### 3.5 Information

```markdown
- ID: DV-006
- Title: Data Model (ERD)
- Viewpoint: Information
- Representation:
```

```mermaid
erDiagram
    ORDERS ||--o{ ORDER_ITEMS : contains
    ORDERS ||--o{ OUTBOX_EVENTS : "emits (aggregate_id)"
    ORDERS {
        uuid id PK
        string status
        decimal total
        timestamp created_at
    }
    ORDER_ITEMS {
        uuid order_id FK
        string product_id
        int quantity
        decimal unit_price_snapshot
    }
    OUTBOX_EVENTS {
        uuid event_id PK
        string event_type
        string aggregate_id
        text payload
        timestamp occurred_at
        timestamp published_at
    }
    PROCESSED_EVENTS {
        uuid event_id PK
        string consumer PK
        timestamp processed_at
    }
    STOCK {
        string product_id PK
        int quantity
    }
```

```markdown
- More Information: `ORDERS`, `ORDER_ITEMS`, `OUTBOX_EVENTS`, and one instance of `PROCESSED_EVENTS` (consumer = "order-service") live in the `order_service` schema. `STOCK` and another instance of `PROCESSED_EVENTS` (consumer = "inventory-service") live in the `inventory_service` schema (ADR-0002) â€” there is no single shared physical table; the pattern is repeated per service. Source: SRS Appendix B, BR-009.
```

### 3.6 Interface

```markdown
- ID: DV-007
- Title: REST API Contract â€” order-service
- Viewpoint: Interface
- Representation:
```

| Endpoint | Auth | Request | 2xx Response | Errors |
|---|---|---|---|---|
| `POST /auth/token` | No | `{ "clientId": string, "clientSecret": string }` | `200 { "accessToken": string, "tokenType": "Bearer", "expiresIn": 1800 }` | `401` ProblemDetail |
| `POST /api/v1/orders` | Bearer JWT | `{ "items": [{ "productId": string, "quantity": int }] }` | `201 { "orderId": uuid, "status": "PENDING" }` | `400` ProblemDetail + `errors[]`, `401` |
| `GET /api/v1/orders/{id}` | Bearer JWT | â€” | `200 { "orderId": uuid, "status": string, "items": [...], "total": decimal }` | `404`, `401` |

```markdown
- More Information: Implements REQ-FUNC-001/002/005/006/007/008/009. RFC 7807 error format per SRS 3.1.3. Auth per ADR-0005.
```

```markdown
- ID: DV-008
- Title: Kafka Event Contracts
- Viewpoint: Interface
- Representation:
```

**Topic `order.created`** (key = `aggregateId`, ADR-0003)
```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "occurredAt": "2026-09-10T12:00:00Z",
  "aggregateId": "orderId-uuid",
  "payload": {
    "orderId": "uuid",
    "items": [ { "productId": "string", "quantity": 0 } ]
  }
}
```

**Topic `inventory.reserved`** (key = `aggregateId`)
```json
{
  "eventId": "uuid",
  "eventType": "InventoryReserved",
  "occurredAt": "2026-09-10T12:00:05Z",
  "aggregateId": "orderId-uuid",
  "payload": { "orderId": "uuid" }
}
```

**Topic `inventory.rejected`** (key = `aggregateId`)
```json
{
  "eventId": "uuid",
  "eventType": "InventoryRejected",
  "occurredAt": "2026-09-10T12:00:05Z",
  "aggregateId": "orderId-uuid",
  "payload": { "orderId": "uuid", "reason": "INSUFFICIENT_STOCK" }
}
```

Dead Letter Topics: `order.created.DLT`, `inventory.reserved.DLT`, `inventory.rejected.DLT` (ADR-0004).

```markdown
- More Information: Implements REQ-MAINT-002. Common envelope per SRS 3.1.3. No Schema Registry (Out-of-Scope) â€” any payload change must be kept backward-compatible manually.
```

### 3.7 Interaction

```markdown
- ID: DV-009
- Title: Happy Path â€” Order Creation to Confirmation
- Viewpoint: Interaction
- Representation:
```

```mermaid
sequenceDiagram
    participant C as API Client
    participant OS as order-service
    participant DB as PostgreSQL (order_service)
    participant KF as Kafka
    participant IS as inventory-service
    participant DBI as PostgreSQL (inventory_service)

    C->>OS: POST /api/v1/orders
    OS->>DB: INSERT order (PENDING) + INSERT outbox_event (OrderCreated) [1 tx]
    OS-->>C: 201 {orderId, status: PENDING}
    Note over OS: Outbox Publisher polls every 5s (ADR-0001)
    OS->>KF: publish OrderCreated (key=orderId)
    KF->>IS: deliver OrderCreated
    IS->>DBI: tryReserve(stock) [conditional UPDATE, ADR-0010]
    IS->>KF: publish InventoryReserved (key=orderId)
    KF->>OS: deliver InventoryReserved
    OS->>DB: markProcessed + UPDATE order SET status=CONFIRMED [1 tx]
    C->>OS: GET /api/v1/orders/{id}
    OS-->>C: 200 {status: CONFIRMED}
```

```markdown
- More Information: Implements UC-01, UC-02, UC-04, UC-05. References ADR-0001, ADR-0003, ADR-0010.
```

```markdown
- ID: DV-010
- Title: Failure Path â€” Kafka Unavailable at Publish Time
- Viewpoint: Interaction
- Representation:
```

```mermaid
sequenceDiagram
    participant OS as order-service
    participant DB as PostgreSQL
    participant KF as Kafka

    Note over OS,KF: Kafka unavailable
    OS->>DB: INSERT order + outbox_event [tx OK, order persisted]
    loop every 5s (ADR-0001)
        OS->>KF: publish attempt
        KF--xOS: failure/timeout
        Note over OS: event remains "pending", order is NOT lost
    end
    Note over KF: Kafka recovers
    OS->>KF: publish OrderCreated (next cycle)
    KF-->>OS: ack
    OS->>DB: markPublished(eventId)
```

```markdown
- More Information: Implements REQ-FUNC-011. Verifies the SRS NFR Reliability.
```

```markdown
- ID: DV-011
- Title: Failure Path â€” Duplicate Event Delivery (Idempotency)
- Viewpoint: Interaction
- Representation:
```

```mermaid
sequenceDiagram
    participant KF as Kafka
    participant IS as inventory-service
    participant DBI as PostgreSQL

    KF->>IS: deliver OrderCreated (eventId=E1) [1st delivery]
    IS->>DBI: hasProcessed(E1, "inventory-service")? NO
    IS->>DBI: tryReserve(stock) + markProcessed(E1) [1 tx]
    IS->>KF: publish InventoryReserved
    KF->>IS: redeliver OrderCreated (eventId=E1) [duplicate]
    IS->>DBI: hasProcessed(E1, "inventory-service")? YES
    Note over IS: skip business effect, recorded no-op (BR-007)
```

```markdown
- More Information: Implements REQ-FUNC-016, UC-07. Algorithmic detail in DV-013.
```

```markdown
- ID: DV-012
- Title: Failure Path â€” Consumer Retry Exhaustion to DLT
- Viewpoint: Interaction
- Representation:
```

```mermaid
sequenceDiagram
    participant KF as Kafka (order.created)
    participant IS as inventory-service
    participant DLT as order.created.DLT

    KF->>IS: deliver OrderCreated
    IS--xIS: exception (attempt 1)
    Note over IS: backoff 1s
    KF->>IS: redeliver (attempt 2)
    IS--xIS: exception (attempt 2)
    Note over IS: backoff 2s
    KF->>IS: redeliver (attempt 3)
    IS--xIS: exception (attempt 3)
    Note over IS: backoff 4s, limit reached
    IS->>DLT: publish original message
    Note over IS: continues processing subsequent messages
```

```markdown
- More Information: Implements REQ-REL-001. References ADR-0004.
```

```markdown
- ID: DV-019
- Title: Outbox Publisher — Structure, Test Behavior, Producer Configuration and Delivery Semantics
- Viewpoint: Interaction
- Representation: Narrative description and tables (complements DV-009, DV-010, DV-016)
```

**1. Scheduling separated from the work.** The publisher is split into two classes with distinct responsibilities:

| Class | Responsibility | Knows about scheduling? |
|---|---|---|
| `OutboxPublisher` | Does the work of one cycle: reads pending events through `OutboxEventRepository.findPending()`, sends each one to Kafka (key = `aggregateId`, ADR-0003), and calls `markPublished(eventId)` once the broker acknowledges it. Exposes a plain method that performs a single polling cycle. | No |
| `OutboxSchedulingConfiguration` | Enables Spring scheduling and triggers `OutboxPublisher` every `OUTBOX_POLLING_INTERVAL_MS` (default 5000, ADR-0001). It contains no publishing logic. | Yes |

Because `OutboxPublisher` has no scheduling annotations, a test can invoke a single cycle deterministically instead of waiting for a timer. The trigger can also be switched off without touching the publishing logic.

**2. Scheduling disabled in integration tests.** `OutboxSchedulingConfiguration` is not loaded in the integration test (IT) profile (it is guarded by a configuration property, e.g. `@ConditionalOnProperty`). The reason is the Spring test context cache: when different IT classes use different configurations, Spring keeps several application contexts alive at the same time within one test run. If each cached context started its own scheduler, there would be several `OutboxPublisher` instances polling the same `outbox_events` table concurrently. That breaks the single-publisher assumption of ADR-0001 (DV-016), which is deliberately not protected by `SELECT ... FOR UPDATE SKIP LOCKED`, and would produce duplicate publications and non-deterministic tests. With the trigger disabled, ITs call `OutboxPublisher` explicitly, so exactly one publisher acts at a time.

**3. Producer configuration.** The producer is tuned so that a Kafka outage makes a cycle fail quickly and leave the event pending (DV-010), instead of blocking the scheduler thread.

| Property | Value | Purpose |
|---|---|---|
| `acks` | `all` | The send is acknowledged only after the in-sync replicas have the record; `markPublished` is never called on an unconfirmed write. |
| `enable.idempotence` | `true` | Removes duplicates caused by the producer's own internal retries within one producer session. |
| `max.block.ms` | `3000` | Upper bound on waiting for metadata or buffer space when Kafka is unreachable. |
| `request.timeout.ms` | `2000` | Time to wait for a single broker response before the request is retried or failed. |
| `delivery.timeout.ms` | `5000` | Total time budget for one send, including retries. Must be at least `request.timeout.ms` plus `linger.ms`. |
| Wait on the send future | `5s` | `OutboxPublisher` waits synchronously for the acknowledgement up to this limit; on timeout or error it stops the cycle and leaves the event pending. |

The key and value serializers are string-based, and `bootstrap.servers` comes from `KAFKA_BOOTSTRAP_SERVERS` (Appendix A). The timeouts are kept in the same order of magnitude as the polling interval so that a failing cycle does not overlap the next one.

**4. Delivery semantics: at-least-once.** The publisher guarantees that every committed event is published **at least once**, never zero times, but it does not guarantee exactly once. The `markPublished` update in PostgreSQL and the send to Kafka cannot share a transaction (no 2PC, ADR-0001), so a duplicate can appear from two sources:

| # | Source of duplicates | What happens | Result |
|---|---|---|---|
| 1 | **Acknowledgement lost or timed out** | The broker persisted the record, but the acknowledgement does not reach the publisher before the timeout. The publisher treats the send as failed and leaves the event pending. | The next cycle sends the same event again. |
| 2 | **Failure after the acknowledgement, before `markPublished` commits** | Kafka confirmed the record, but the service crashes or the database update fails before the event is marked as published. | After recovery, the event is still pending and is sent again. |

In both cases the republished message carries the **same `eventId`**. Duplicates are absorbed downstream by the Idempotent Consumer (`processed_events`, DV-011 and DV-013), so the business effect is applied only once. Idempotence on the producer (`enable.idempotence`) covers only the producer's internal retries and does not remove these two sources, which is why consumer-side deduplication remains mandatory.

```markdown
- More Information: Implements REQ-FUNC-010 / REQ-FUNC-011. References ADR-0001, ADR-0003, ADR-0006. Source 1 and source 2 correspond to the failure window shown in DV-010, and the consumer side of the guarantee to DV-011.
```

### 3.8 Algorithm

```markdown
- ID: DV-013
- Title: Idempotent Event Processing Algorithm
- Viewpoint: Algorithm
- Representation:
```

```text
function handle(event):
    if processedEventRepository.hasProcessed(event.eventId, CONSUMER_NAME):
        log.info("duplicate event skipped", event.eventId)
        return                              # no-op, BR-007

    applyBusinessEffect(event)              # e.g., tryReserve or state transition
    processedEventRepository.markProcessed(event.eventId, CONSUMER_NAME)
    # both steps in the SAME local consumer transaction
```

```markdown
- More Information: Implements REQ-FUNC-016, BR-006/BR-007. The transactional coupling between `applyBusinessEffect` and `markProcessed` is critical: if they ran in separate transactions, a failure between them could leave the effect applied but unmarked (incorrect future reprocessing) or marked but not applied (lost effect).
```

```markdown
- ID: DV-014
- Title: Atomic Stock Reservation Algorithm
- Viewpoint: Algorithm
- Representation:
```

```text
function tryReserve(productId, quantity):
    rowsAffected = execute(
        "UPDATE stock SET quantity = quantity - :qty
         WHERE product_id = :id AND quantity >= :qty",
        qty=quantity, id=productId
    )
    return rowsAffected == 1        # true = reserved, false = insufficient stock

function reserveForOrder(order):
    for item in order.items:
        if not tryReserve(item.productId, item.quantity):
            rollbackTransaction()
            return REJECTED         # atomic rejection of the whole order, no partial reservations
    return RESERVED
```

```markdown
- More Information: Implements REQ-FUNC-012/013, ADR-0010. Atomicity is guaranteed by PostgreSQL in a single statement, with no explicit application-level locks.
```

### 3.9 State Dynamics

```markdown
- ID: DV-015
- Title: Order State Machine
- Viewpoint: State Dynamics
- Representation:
```

```mermaid
stateDiagram-v2
    [*] --> PENDING: OrderCreated persisted (BR-003)
    PENDING --> CONFIRMED: InventoryReserved consumed (BR-004)
    PENDING --> REJECTED: InventoryRejected consumed (BR-005)
    CONFIRMED --> [*]
    REJECTED --> [*]
```

```markdown
- More Information: BR-011 allows the order to remain temporarily in PENDING while the corresponding event has not yet been consumed (eventual consistency, SRS NFR Consistency).
```

### 3.10 Concurrency

```markdown
- ID: DV-016
- Title: Outbox Publisher Singleton & Stock Update Race Avoidance
- Viewpoint: Concurrency
- Representation: Narrative description (no additional diagram; complements DV-009/DV-014)
```

order-service runs as a single instance (an assumption of ADR-0001): the Outbox Publisher's `@Scheduled` task never runs concurrently with itself, so `SELECT ... FOR UPDATE SKIP LOCKED` is not required in this version. inventory-service, on the other hand, may receive concurrent messages from different partitions; correctness under concurrent `OrderCreated` events for the same `productId` does not depend on an application-level lock, but on the fact that the conditional `UPDATE` of DV-014 is atomic at the PostgreSQL engine level â€” two concurrent transactions on the same row are serialized automatically by the engine. Additionally, the `DefaultErrorHandler` (ADR-0004) blocks the advancement of a partition during the backoff window of a failed message, guaranteeing that there are never two processing attempts of the same message running in parallel. The single-publisher assumption is also why scheduling is disabled in integration tests (see DV-019, section 2).

```markdown
- More Information: References ADR-0001, ADR-0004, ADR-0010. Publisher design details in DV-019.
```

### 3.11 Patterns

```markdown
- ID: DV-017
- Title: Applied Architectural & Design Patterns
- Viewpoint: Patterns
- Representation:
```

| Pattern | Where it is applied | Decision |
|---|---|---|
| Transactional Outbox | order-service (order + event persistence) | ADR-0001 |
| Idempotent Consumer | order-service and inventory-service (`processed_events`) | REQ-FUNC-016, DV-013 |
| Ports & Adapters (Lightweight Hexagonal) | Both services | ADR-0006 |
| Dead Letter Channel | Kafka consumers of both services | ADR-0004 |
| Conditional atomic update (concurrency without explicit locks) | inventory-service | ADR-0010 |

```markdown
- More Information: These patterns are the project's central engineering evidence (Master Context, Goals and Key Engineering Evidence).
```

### 3.12 Deployment

```markdown
- ID: DV-018
- Title: Docker Compose Deployment Topology
- Viewpoint: Deployment
- Representation:
```

```mermaid
graph TB
    subgraph "Docker Compose network"
        OS["order-service<br/>:8080"]
        IS["inventory-service"]
        PG[("postgres:16<br/>:5432")]
        KF[["kafka (KRaft)<br/>:9092"]]
    end
    Client((API Client)) -->|"HTTP :8080"| OS
    OS --> PG
    IS --> PG
    OS <--> KF
    IS <--> KF
```

```markdown
- More Information: KRaft mode, no Zookeeper container (ADR-0007). Environment variables externalized per REQ-INST-001 â€” see Appendix A. A single `docker-compose up` leaves everything operational (REQ-INST-001).
```

## 4. Decisions

The following 10 decisions were detailed and accepted in `MADR.md`. This section summarizes each one; **the normative source is `MADR.md`**, and its full content is not duplicated here.

```markdown
- ID: SDD-DEC-001
- Title: Outbox publishing strategy
- Context: Kafka does not participate in order-service's PostgreSQL transaction (no 2PC); pending events must be published reliably.
- Options: Embedded Polling Publisher / Separate relay service / CDC (Debezium)
- Outcome: Embedded Polling Publisher â€” see ADR-0001 in MADR.md
- More Information: MADR.md â†’ ADR-0001
```

```markdown
- ID: SDD-DEC-002
- Title: Data ownership in a shared PostgreSQL instance
- Context: Single mandatory instance, with schema separation per domain.
- Options: Schema-level isolation / Shared schema / Separate instances
- Outcome: Schema-level isolation with a dedicated DB role â€” see ADR-0002 in MADR.md
- More Information: MADR.md â†’ ADR-0002
```

```markdown
- ID: SDD-DEC-003
- Title: Kafka event key/partition strategy
- Context: Preserve per-aggregate ordering in the event of a future increase in partitions.
- Options: Key=aggregateId / No key / Key=eventType
- Outcome: Key=aggregateId â€” see ADR-0003 in MADR.md
- More Information: MADR.md â†’ ADR-0003
```

```markdown
- ID: SDD-DEC-004
- Title: Consumer retry and Dead Letter Topic strategy
- Context: REQ-REL-001 requires 3 retries with 1s/2s/4s backoff and then a DLT.
- Options: DefaultErrorHandler+DLT recoverer / Manual retry / @RetryableTopic
- Outcome: DefaultErrorHandler + DeadLetterPublishingRecoverer â€” see ADR-0004 in MADR.md
- More Information: MADR.md â†’ ADR-0004
```

```markdown
- ID: SDD-DEC-005
- Title: JWT authentication and client credentials strategy
- Context: Self-issued JWT, no external IdP, no user management.
- Options: Spring Security Resource Server / Custom filter / Spring Authorization Server
- Outcome: Spring Security Resource Server with a local JWT decoder â€” see ADR-0005 in MADR.md
- More Information: MADR.md â†’ ADR-0005
```

```markdown
- ID: SDD-DEC-006
- Title: Domain/infrastructure separation (layering)
- Context: REQ-MAINT-001 requires a domain with no dependency on Kafka.
- Options: Lightweight Hexagonal / Simple layers / Full Clean Architecture
- Outcome: Lightweight Hexagonal (Ports & Adapters) â€” see ADR-0006 in MADR.md
- More Information: MADR.md â†’ ADR-0006
```

```markdown
- ID: SDD-DEC-007
- Title: Kafka deployment mode
- Context: REQ-INST-001 requires single-command startup with minimal complexity.
- Options: KRaft / Classic Kafka+Zookeeper
- Outcome: KRaft (no Zookeeper) â€” see ADR-0007 in MADR.md
- More Information: MADR.md â†’ ADR-0007
```

```markdown
- ID: SDD-DEC-008
- Title: Structured logging and correlation strategy
- Context: REQ-OBS-001 requires orderId/eventId in all relevant logs.
- Options: MDC + JSON logging / Manual plain text / External observability stack
- Outcome: MDC + JSON logging (Logback encoder) â€” see ADR-0008 in MADR.md
- More Information: MADR.md â†’ ADR-0008
```

```markdown
- ID: SDD-DEC-009
- Title: DB schema versioning and data seeding
- Context: Full reproducibility required by REQ-INST-001, including seeded client credentials.
- Options: Flyway / Liquibase / ddl-auto + CommandLineRunner
- Outcome: Flyway with a seed migration â€” see ADR-0009 in MADR.md
- More Information: MADR.md â†’ ADR-0009
```

```markdown
- ID: SDD-DEC-010
- Title: Stock concurrency control in inventory-service
- Context: Avoid overselling under concurrent orders for the same product.
- Options: Atomic conditional UPDATE / Optimistic locking / Pessimistic locking
- Outcome: Atomic conditional UPDATE â€” see ADR-0010 in MADR.md
- More Information: MADR.md â†’ ADR-0010
```

## 5. Security

### Architectural Security Decisions

- **Zero Clock Skew Tolerance:** JWT validation is configured with zero seconds of clock skew tolerance. Since the token is self-issued and validated within the same internal infrastructure, there is no network delay or external clock synchronization drift to account for, making any tolerance window an unnecessary risk.
- **Dummy Hash Calculation:** To prevent client enumeration via timing attacks, the authentication process executes a dummy hash computation when an unknown client or user is queried. This ensures that the response times for both valid and invalid clients remain indistinguishable.
- **Deny-by-Default Policy:** The resource server enforces a strict deny-by-default access control policy (`anyRequest().authenticated()`). Only a single public endpoint (the token issuance point) is explicitly whitelisted, safely minimizing the exposed attack surface.

## 6. Appendixes

### Appendix A â€” Environment Variables Reference

Every setting comes from environment variables (REQ-INST-001) and none is hardcoded in the repository
(REQ-SEC-001). Docker Compose reads the variables you set from `.env` (copied from `.env.example`) and
refuses to start if a required one is missing.

**Variables you set**

| Variable | Used by | Required | Purpose |
|---|---|---|---|
| `POSTGRES_PASSWORD` | postgres | yes | Superuser password, used only by the init script that creates the service roles (ADR-0015) |
| `ORDER_DB_PASSWORD` | postgres, order-service | yes | Password of the `order_svc` role (ADR-0002) |
| `INVENTORY_DB_PASSWORD` | postgres, inventory-service | yes | Password of the `inventory_svc` role (ADR-0002) |
| `SEED_CLIENT_ID` | order-service | yes | API client seeded at startup by a Java Flyway migration registered as a Spring bean (ADR-0009) |
| `SEED_CLIENT_SECRET` | order-service | yes | Its secret; only the BCrypt hash is stored (REQ-SEC-001) |
| `JWT_SIGNING_SECRET` | order-service | yes | HS256 signing key, at least 32 bytes, validated at startup (REQ-SEC-001, ADR-0005) |
| `OUTBOX_POLLING_INTERVAL_MS` | order-service, inventory-service | no (5000) | Interval of the outbox publisher (REQ-FUNC-010) |

**Variables set by `docker-compose.yml`** (no configuration needed)

| Variable | Used by | Value |
|---|---|---|
| `DB_URL` | both services | `jdbc:postgresql://postgres:5432/rop`; identical for both, the schema comes from each role's `search_path` |
| `DB_SCHEMA` | both services | `order_service` / `inventory_service`; default schema of Flyway (ADR-0002) |
| `DB_USER` | both services | `order_svc` / `inventory_svc` |
| `DB_PASSWORD` | both services | Taken from `ORDER_DB_PASSWORD` / `INVENTORY_DB_PASSWORD` |
| `KAFKA_BOOTSTRAP_SERVERS` | both services | `kafka:9092` (internal listener) |

### Appendix B â€” Traceability Matrix (SRS â†’ Design View â†’ ADR)

| SRS Requirement | Design View(s) | ADR(s) |
|---|---|---|
| REQ-FUNC-001 / 002 | DV-007 | â€” |
| REQ-FUNC-003 / 004 / 005 | DV-003, DV-006, DV-009 | ADR-0001, ADR-0002 |
| REQ-FUNC-006 | DV-007 | â€” |
| REQ-FUNC-007 / 008 / 009 | DV-007 | ADR-0005 |
| REQ-FUNC-010 / 011 | DV-001, DV-002, DV-009, DV-010, DV-016, DV-019 | ADR-0001 |
| REQ-FUNC-012 / 013 | DV-004, DV-006, DV-014 | ADR-0010 |
| REQ-FUNC-014 / 015 | DV-009, DV-015 | â€” |
| REQ-FUNC-016 | DV-004, DV-011, DV-013 | ADR-0002 |
| REQ-PERF-001 | N/A â€” implementation benchmark, no dedicated design view | â€” |
| REQ-SEC-001 / 002 | DV-007, Appendix A | ADR-0005 |
| REQ-REL-001 | DV-012 | ADR-0004 |
| REQ-OBS-001 | DV-009 to DV-012 (cross-cutting) | ADR-0008 |
| REQ-COMP-001 | â€” (outside design scope, see SRS 3.4) | â€” |
| REQ-INST-001 | DV-018, Appendix A | ADR-0007, ADR-0009 |
| REQ-BUILD-001 | â€” (outside design scope, see SRS 3.5.2) | â€” |
| REQ-MAINT-001 | DV-005 | ADR-0006 |
| REQ-MAINT-002 | DV-008 | â€” |
| REQ-PORT-001 | DV-018 | ADR-0007 |
| REQ-COST-001 | DV-002, DV-018 (cross-cutting) | ADR-0001, ADR-0007 |
| REQ-CM-001 | â€” (outside design scope, see SRS 3.5.10) | â€” |
