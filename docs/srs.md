# Software Requirements Specification — Resilient Order Pipeline

Version 1.1 · Kaulin Mindiola · Personal portfolio project

## Table of Contents

* [1. Introduction](#1-introduction)
* [2. Product Overview](#2-product-overview)
* [3. Requirements](#3-requirements)
* [4. Verification](#4-verification)
* [5. Appendixes](#5-appendixes)

## Revision History

| Name | Date | Reason for changes | Version |
|------|------|--------------------|---------|
| KM | 2026-09-10 | Initial baseline, derived from the requirements discovery process | 0.1 |
| KM | 2026-10-04 | Baseline before implementation | 1.0 |
| KM | 2026-10-12 | Verification results for release v0.1.0; name, references and wording aligned with the implementation | 1.1 |

## 1. Introduction

This document specifies the requirements of Resilient Order Pipeline: what the system must do and which
properties it must satisfy. Section 2 provides context and scope; Section 3 contains the verifiable
requirements; Section 4 records how each one was verified for release v0.1.0; Section 5 contains
supporting material.

### 1.1 Document Purpose

This SRS specifies the functional and quality requirements of Resilient Order Pipeline, a reference
backend that demonstrates asynchronous communication between services with Apache Kafka, eventual
consistency, the Transactional Outbox pattern and idempotent consumers. Its audience is the author and
any technical reviewer evaluating the engineering rigor of the project. It defines **what** the system
must do; **how** it does it is described in the [Software Design Description](sdd.md) and the
[architecture decision records](adr/).

### 1.2 Product Scope

**Resilient Order Pipeline**, release v0.1.0, single release. It processes order creation and
asynchronously coordinates the associated inventory reservation, keeping `order-service` and
`inventory-service` eventually consistent through events published to Kafka with the Transactional
Outbox pattern.

It includes:
- Order creation and retrieval.
- API client authentication with self-issued JWTs.
- Emission and consumption of domain events (`OrderCreated`, `InventoryReserved`, `InventoryRejected`).
- Idempotent event processing and tolerance to Kafka and consumer failures.

It explicitly excludes: a user interface, catalog, cart, checkout or payment management, end-user
management, integration with an external identity provider (OAuth2), and any production SLA. The project
is not an e-commerce platform; it demonstrates event-driven architecture patterns within a bounded,
zero-cost scope.

### 1.3 Definitions, Acronyms, and Abbreviations

| Term | Definition |
|------|------------|
| API | Application Programming Interface. |
| SRS | Software Requirements Specification. |
| UI | User Interface. |
| Outbox Pattern | Persisting a domain event in the same database transaction as the state change, so that it can later be published asynchronously and reliably. |
| Eventual Consistency | In the absence of new updates, all components eventually converge to the same state, with no guarantee of immediacy. |
| Idempotent Consumer | An event consumer that produces the same business effect regardless of how many times it receives the same event. |
| DLT | Dead Letter Topic: the Kafka topic that receives messages whose processing failed. |
| JWT | JSON Web Token: a signed token used to authenticate API calls. |
| Client Credential | A seeded (not self-registrable) `clientId` / `clientSecret` pair used to obtain a JWT. |
| Aggregate | A domain object identified by an id (the order) whose state changes are communicated through events. |

### 1.4 References

| Title | Location | Type |
|---|---|---|
| RFC 9457 — Problem Details for HTTP APIs | https://www.rfc-editor.org/rfc/rfc9457 | Normative (error format, see 3.1.3) |
| Software Design Description | [`sdd.md`](sdd.md) | Informative |
| Architecture Decision Records | [`adr/`](adr/) | Informative |
| Event contracts | [`events.md`](events.md) | Normative (event format) |

### 1.5 Document Overview

Section 2 provides context: perspective, functions, constraints, users, assumptions and dependencies.
Section 3 contains the verifiable requirements. Section 4 maps each requirement to its verification and
result. Section 5 contains the architecture overview, the data model reference and the business rules.
Requirement identifiers are unique and immutable.

## 2. Product Overview

### 2.1 Product Perspective

A greenfield, standalone project. It consists of two components (`order-service`,
`inventory-service`) and two open-source infrastructure dependencies, self-hosted with Docker Compose
(PostgreSQL, Apache Kafka). There are no contractual SLAs or support model: it is an individual
portfolio project.

### 2.2 Product Functions

- Create an order, validating its items and quantities against a product catalog.
- Retrieve an order by id.
- Issue an access token (JWT) from client credentials.
- Reliably publish domain events through the Transactional Outbox.
- Reserve or reject inventory, all or nothing, based on stock availability.
- Confirm or reject the order based on the outcome of the reservation.
- Process events idempotently, tolerating duplicate deliveries.
- Recover from Kafka unavailability and consumer crashes without losing or duplicating effects.

### 2.3 Product Constraints

- Java 21 with Spring Boot 4; Maven build.
- PostgreSQL as a single instance, with one schema per service (no database per service).
- Asynchronous communication between the services, only through Apache Kafka.
- Transactional Outbox for every event derived from a state change.
- Idempotent event consumers.
- The whole environment starts with Docker Compose and a single command.
- Integration tests run against real PostgreSQL and Kafka with Testcontainers.
- No distributed transactions (2PC) between PostgreSQL and Kafka; no event sourcing.
- No paid service, tool or tier (see 3.5.7).
- Self-issued JWT authentication, with no external identity provider.
- No production SLA (uptime, guaranteed throughput).

### 2.4 User Characteristics

A single actor, the **API client**: a script, tool or test suite that uses the platform exclusively
through REST, authenticated with a Bearer token. There are no human end users, no graphical interface,
and no roles or permission levels.

### 2.5 Assumptions and Dependencies

**Assumptions**

| ID | Assumption | Impact if false |
|---|---|---|
| ASM-001 | Single-developer project, with no operations team. | Low: affects only the process level (3.5.10). |
| ASM-002 | This specification corresponds to a single release (v0.1.0). | Medium: further releases would version this SRS. |
| ASM-004 | Kafka and PostgreSQL are available in the execution environment (Docker Compose). | High: the full flow cannot run without them. The outbox tolerates *temporary* Kafka unavailability, not its total absence. |

**Dependencies:** Apache Kafka (single broker), PostgreSQL (single instance), Spring Security with
Nimbus JOSE + JWT, Testcontainers, Docker with Docker Compose, and GitHub Actions (free tier). All are
free and open source (2.3, 3.5.7).

### 2.6 Apportioning of Requirements

All requirements of Section 3 belong to release v0.1.0. An inventory management API, order
cancellation, stock release and a notification service are outside this release (see the README's
known limitations).

## 3. Requirements

### 3.1 External Interfaces

#### 3.1.1 User Interfaces

**N/A.** There is no graphical interface.

#### 3.1.2 Hardware Interfaces

**N/A.** The system is purely software.

#### 3.1.3 Software Interfaces

**REST API — `order-service`**
- `POST /auth/token`: exchanges `clientId` / `clientSecret` for a JWT. No prior authentication.
- `POST /api/v1/orders`: creates an order. Requires `Authorization: Bearer <JWT>`.
- `GET /api/v1/orders/{id}`: retrieves an order. Requires `Authorization: Bearer <JWT>`.
- `Content-Type: application/json` on every endpoint.
- Error format: **RFC 9457 Problem Details**, extended with an `errors[]` array for field-level
  violations. Status codes: `400` malformed request, `401` missing or invalid credentials, `404`
  unknown order, `422` product outside the catalog.

**Kafka**
- Topics `order.created`, `inventory.reserved`, `inventory.rejected`, each with a `<topic>.DLT`.
- No Schema Registry; JSON values with a common envelope
  `{ eventId, eventType, occurredAt, aggregateId, payload }`. Full contracts in [`events.md`](events.md).

**PostgreSQL**
- A single instance shared by both services, with one schema and one database role per service.

### 3.2 Functional

The following requirements implement business rules BR-001 to BR-018 (Appendix C).

#### 3.2.1 Order Creation

```markdown
- ID: REQ-FUNC-001
- Title: Reject malformed order creation requests
- Statement: The system shall reject an order creation request that does not include at least one item, responding with HTTP 400 and a structured error response.
- Rationale: Enforces BR-001 at the API boundary.
- Acceptance Criteria: A POST with an empty or missing `items` array returns HTTP 400; no order or outbox event is persisted.
- Verification Method: Test
```

```markdown
- ID: REQ-FUNC-002
- Title: Reject items with non-positive quantity
- Statement: The system shall reject an order creation request containing any item with quantity <= 0, responding with HTTP 400.
- Rationale: Enforces BR-002.
- Acceptance Criteria: A request with an item of quantity 0 or negative returns HTTP 400, listing the invalid field in `errors[]`; nothing is persisted.
- Verification Method: Test
```

```markdown
- ID: REQ-FUNC-003
- Title: Persist new order in PENDING state
- Statement: Given a valid request, the system shall persist a new order with status PENDING, its items (productId, quantity, unit price snapshot taken from the catalog) and a computed total.
- Rationale: BR-003, BR-018.
- Acceptance Criteria: Querying the order returns status PENDING, the submitted items and total = Σ(quantity × unit price snapshot). A product outside the catalog returns HTTP 422.
- Verification Method: Test
```

```markdown
- ID: REQ-FUNC-004
- Title: Atomically persist order and outbox event
- Statement: The system shall persist an OrderCreated outbox event together with the new order in the same PostgreSQL transaction.
- Rationale: BR-008: the event is never lost relative to the order.
- Acceptance Criteria: If the transaction fails, neither the order nor the event is persisted; if it succeeds, both exist in the same commit.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-005
- Title: Respond with the created order
- Statement: Upon successful creation, the system shall respond 201 Created with the order identifier, its status and a Location header.
- Rationale: Enables the client to track the order.
- Acceptance Criteria: Status 201; body contains orderId and status = PENDING; Location points to the order.
- Verification Method: Test
```

#### 3.2.2 Order Query

```markdown
- ID: REQ-FUNC-006
- Title: Retrieve order by id
- Statement: The system shall provide GET /api/v1/orders/{id} returning the order's current persisted state (PENDING, CONFIRMED or REJECTED), its items and its total.
- Rationale: Allows observing the eventual state.
- Acceptance Criteria: An existing id returns 200 with the state persisted in PostgreSQL (BR-009); a non-existent id returns 404.
- Verification Method: Test
```

#### 3.2.3 Authentication

```markdown
- ID: REQ-FUNC-007
- Title: Issue JWT for valid client credentials
- Statement: The system shall provide POST /auth/token that, given valid client credentials, returns a signed JWT.
- Rationale: Authenticated access without an external identity provider.
- Acceptance Criteria: Valid credentials return 200 with a token; an unknown client and a wrong secret both return the same 401.
- Verification Method: Test
- More Information: Client credentials are seeded, not self-registrable.
```

```markdown
- ID: REQ-FUNC-008
- Title: Reject unauthenticated access to protected endpoints
- Statement: The system shall reject any request to /api/v1/** lacking a valid, non-expired JWT, responding with 401.
- Rationale: BR-012.
- Acceptance Criteria: A missing header, a malformed token, a token signed with another key or an expired token receives 401 and is not processed.
- Verification Method: Test
```

```markdown
- ID: REQ-FUNC-009
- Title: Enforce JWT expiration
- Statement: The system shall issue JWTs with a 30-minute expiration and reject expired tokens. No refresh token is issued; the client requests a new token when it expires.
- Rationale: BR-013.
- Acceptance Criteria: exp = iat + 30 min; a token used after its expiration is rejected with 401.
- Verification Method: Test
```

#### 3.2.4 Outbox Publishing

```markdown
- ID: REQ-FUNC-010
- Title: Publish pending outbox events to Kafka
- Statement: The system shall poll for unpublished outbox events every 5 seconds (configurable with OUTBOX_POLLING_INTERVAL_MS) and publish each to its Kafka topic.
- Rationale: Decouples the HTTP transaction from Kafka availability.
- Acceptance Criteria: An event is published within the polling interval and marked as published only after the broker acknowledged it.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-011
- Title: Retry publishing on Kafka unavailability
- Statement: If Kafka is unavailable, the system shall leave the event pending and retry on later cycles, with no attempt limit, until it is published.
- Rationale: A temporary Kafka failure must not lose a persisted order.
- Acceptance Criteria: With Kafka unavailable, orders are still accepted and their events stay pending; once Kafka recovers, they are published with no manual intervention.
- Verification Method: Test (failure test)
```

#### 3.2.5 Inventory Reservation

```markdown
- ID: REQ-FUNC-012
- Title: Reserve inventory on sufficient stock
- Statement: Upon consuming OrderCreated, inventory-service shall, if stock suffices for all items, reserve it and emit InventoryReserved.
- Rationale: BR-015.
- Acceptance Criteria: Given sufficient stock, consuming OrderCreated results in InventoryReserved for the same orderId, and concurrent reservations never oversell.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-013
- Title: Reject reservation on insufficient stock
- Statement: Upon consuming OrderCreated, inventory-service shall, if any item cannot be reserved, emit InventoryRejected without reserving any stock.
- Rationale: BR-015.
- Acceptance Criteria: InventoryRejected is emitted with reason INSUFFICIENT_STOCK or UNKNOWN_PRODUCT; no stock is decremented.
- Verification Method: Test (integration)
```

#### 3.2.6 Order State Transitions

```markdown
- ID: REQ-FUNC-014
- Title: Confirm order on InventoryReserved
- Statement: Upon consuming InventoryReserved, order-service shall move the order from PENDING to CONFIRMED.
- Rationale: BR-004.
- Acceptance Criteria: GET returns CONFIRMED.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-015
- Title: Reject order on InventoryRejected
- Statement: Upon consuming InventoryRejected, order-service shall move the order from PENDING to REJECTED.
- Rationale: BR-005.
- Acceptance Criteria: GET returns REJECTED.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-016
- Title: Deduplicate event processing per consumer
- Statement: Each consumer shall record every processed event in processed_events (unique per event_id and consumer) and skip the business effect of an event already recorded.
- Rationale: BR-006, BR-007.
- Acceptance Criteria: Delivering the same event twice, or concurrently, produces the business effect exactly once.
- Verification Method: Test
```

```markdown
- ID: REQ-FUNC-017
- Title: Deterministic seeding
- Statement: Flyway shall seed the product catalog, the client credentials and the stock so that the system is operable after docker compose up without manual steps.
- Rationale: REQ-INST-001.
- Acceptance Criteria: On a clean clone, docker compose up --build leaves catalog, credentials and stock available.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-018
- Title: Declarative topics
- Statement: Each service shall declare the Kafka topics it produces or consumes, and their dead letter topics, as code at startup, with broker auto-creation disabled.
- Rationale: REQ-INST-001.
- Acceptance Criteria: The 6 topics exist with 3 partitions after startup, with no auto-creation and no scripts.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-019
- Title: Invalid transitions and unknown aggregates are no-ops
- Statement: When an event does not apply to the current state of the order, or refers to a non-existent order, the consumer shall record it as processed and log it, without raising an exception.
- Rationale: BR-014.
- Acceptance Criteria: The event is recorded as processed and logged at WARN, with no exception, retry or DLT entry.
- Verification Method: Test (integration)
```

```markdown
- ID: REQ-FUNC-020
- Title: Inventory events published through the outbox
- Statement: inventory-service shall commit the reservation or rejection and its outgoing event in a single local transaction, and shall not publish to Kafka from the use case.
- Rationale: BR-008.
- Acceptance Criteria: The reservation or rejection and its outgoing event are committed together.
- Verification Method: Test (integration)
```

### 3.3 Quality of Service

#### 3.3.1 Performance

```markdown
- ID: REQ-PERF-001
- Title: Order creation latency (reference, not an SLA)
- Statement: Under local conditions (Docker Compose, no external load), order creation should complete with a p95 latency under 300 ms.
- Rationale: Demonstration reference only; there is no production SLA (2.3).
- Acceptance Criteria: Measured locally and documented as a reference figure, not a contractual target.
- Verification Method: Demonstration
```

#### 3.3.2 Security

```markdown
- ID: REQ-SEC-001
- Title: JWT signing and secret management
- Statement: The system shall sign JWTs with HS256 using a secret of at least 256 bits provided through an environment variable, never stored in source control.
- Rationale: Baseline secret hygiene at zero cost.
- Acceptance Criteria: The secret is externalized and validated at startup; the repository contains no secret value.
- Verification Method: Test and inspection
```

```markdown
- ID: REQ-SEC-002
- Title: No sensitive data in logs
- Statement: The system shall not log JWTs, client secrets, passwords or Authorization header values at any log level.
- Rationale: Baseline confidentiality control.
- Acceptance Criteria: The logs of the running services contain none of these values.
- Verification Method: Test
```

**Note:** HTTPS is advisable for any real deployment but is not a requirement of this release, which
has no cloud infrastructure in scope.

#### 3.3.3 Reliability

```markdown
- ID: REQ-REL-001
- Title: Consumer retry and dead-letter handling
- Statement: On a consumer processing failure, the system shall retry 3 times with exponential backoff (1 s, 2 s, 4 s) and then route the message to <topic>.DLT. Messages that violate their contract shall go to the DLT without retries.
- Rationale: Tolerates transient failures; poison messages never stall processing indefinitely.
- Acceptance Criteria: A message that keeps failing receives 4 attempts, reaches the DLT in its original partition, and the next messages are processed; an invalid message reaches the DLT after one attempt.
- Verification Method: Test (failure test)
```

#### 3.3.4 Availability

**N/A for this release.** Single-instance deployment with no uptime objective.

#### 3.3.5 Observability

```markdown
- ID: REQ-OBS-001
- Title: Correlate logs by orderId and eventId
- Statement: Every log entry related to an order or an event shall include orderId and, when processing an event, eventId as structured fields.
- Rationale: Tracing an order across services.
- Acceptance Criteria: In the JSON logs of both services, every step of an order's journey carries its orderId, and event handling also carries its eventId.
- Verification Method: Test
```

### 3.4 Compliance

```markdown
- ID: REQ-COMP-001
- Title: Open-source licensing for all dependencies
- Statement: The system shall use only third-party libraries and tools that are free of charge and licensed under permissive (MIT, Apache 2.0, BSD) or weak-copyleft (EPL, LGPL, MPL, CDDL) terms, or dual-licensed with one such option.
- Rationale: Zero-cost constraint and public distribution of the code.
- Acceptance Criteria: A license audit of all dependencies shows no commercial licence and no strong copyleft (GPL, AGPL) as the only option.
- Verification Method: Inspection
```

No personal data and no payments are processed, so no further regulatory obligations apply.

### 3.5 Design and Implementation

#### 3.5.1 Installation

```markdown
- ID: REQ-INST-001
- Title: Full environment startup with Docker Compose
- Statement: The system shall start order-service, inventory-service, PostgreSQL and Kafka with a single docker compose up --build, with every environment-specific value externalized through environment variables.
- Rationale: Reproducibility for any reviewer.
- Acceptance Criteria: A clean clone, with Docker installed, becomes fully operational after copying .env.example to .env and running docker compose up --build, with no other manual step.
- Verification Method: Demonstration
```

Uninstall: `docker compose down -v` removes containers and volumes.

#### 3.5.2 Build and Delivery

```markdown
- ID: REQ-BUILD-001
- Title: Continuous integration on push
- Statement: The system shall run an automated build and test pipeline (GitHub Actions) on every push and pull request.
- Rationale: Quality evidence at no cost.
- Acceptance Criteria: Every push triggers the unit, integration and system tests; a failing test blocks a green status and the merge into main.
- Verification Method: Demonstration
```

#### 3.5.3 Distribution

Single-instance deployment: one `docker-compose.yml`, no replicas, no orchestrator.

#### 3.5.4 Maintainability

```markdown
- ID: REQ-MAINT-001
- Title: Domain independent of infrastructure
- Statement: The domain layer shall not depend on Kafka, Spring, JPA or JSON libraries; all such interaction shall live in adapters.
- Rationale: The business rules can be understood and tested without infrastructure.
- Acceptance Criteria: An automated architecture test fails the build on any forbidden dependency.
- Verification Method: Test
```

```markdown
- ID: REQ-MAINT-002
- Title: Documented event contracts
- Statement: Each event type shall have its fields, types and an example payload documented in the repository.
- Rationale: A shared contract between independently deployed services.
- Acceptance Criteria: A documentation file lists, for each event type, its full field set and an example.
- Verification Method: Inspection
```

#### 3.5.5 Reusability

**N/A.** No component is designed for reuse outside this project.

#### 3.5.6 Portability

```markdown
- ID: REQ-PORT-001
- Title: Cross-platform execution through containers
- Statement: The system shall run on any host with Docker Engine and Docker Compose (Linux, macOS, Windows).
- Rationale: Docker is the only platform requirement.
- Acceptance Criteria: The system builds, starts and passes its tests on at least one Linux and one non-Linux host.
- Verification Method: Demonstration
```

#### 3.5.7 Cost

```markdown
- ID: REQ-COST-001
- Title: Zero operational cost
- Statement: The system shall not require any paid service, tier or licence to build, run or demonstrate it.
- Rationale: Explicit project constraint (2.3).
- Acceptance Criteria: No credit card, subscription or paid key is needed to reproduce the full demo.
- Verification Method: Inspection
```

#### 3.5.8 Deadline

No external deadline. Release v0.1.0 was published on 2026-10-12.

#### 3.5.9 Proof of Concept

**N/A.** The project itself validates the patterns; there was no separate proof of concept.

#### 3.5.10 Change Management

```markdown
- ID: REQ-CM-001
- Title: Traceable change history
- Statement: The system shall track changes with Git, with readable commits and a tagged release (v0.1.0).
- Rationale: A lightweight process that keeps the focus on technical evidence.
- Acceptance Criteria: The history is organised in one pull request per phase and the release is tagged v0.1.0.
- Verification Method: Inspection
```

### 3.6 AI/ML

**N/A.**

## 4. Verification

Status for release v0.1.0. Tests run in CI on every push; system tests run against the real Docker
Compose stack. Test numbers refer to the [Evidence](../README.md#evidence) table of the README.

| Requirement | Method | Evidence | Status |
|---|---|---|---|
| REQ-FUNC-001 / 002 | Test | `OrderTest`, `OrderApiIT`, `RestErrorHandlingIT` | Passed |
| REQ-FUNC-003 / 005 | Test | `CreateOrderServiceTest`, `OrderApiIT`, `HappyPathSystemIT` (test 5) | Passed |
| REQ-FUNC-004 | Test | `OrderOutboxAtomicityIT` (test 1) | Passed |
| REQ-FUNC-006 | Test | `OrderApiIT`, `RestErrorHandlingIT` | Passed |
| REQ-FUNC-007 / 008 / 009 | Test | `TokenServiceTest`, `TokenEndpointIT`, `AuthenticationIT` (test 6) | Passed |
| REQ-FUNC-010 | Test | `OutboxPublisherTest`, `OutboxPublisherIT` (test 2) | Passed |
| REQ-FUNC-011 | Test | `OutboxKafkaOutageIT`, `KafkaOutageSystemIT` (test 7) | Passed |
| REQ-FUNC-012 / 013 | Test | `StockReservationIT`, `OrderCreatedConsumerIT` (test 3), `ConcurrentReservationIT` (test 11), `CommittedRejectionIT` (test 14) | Passed |
| REQ-FUNC-014 / 015 | Test | `InventoryOutcomeConsumerIT` (test 4) | Passed |
| REQ-FUNC-016 | Test | `ProcessedEventRepositoryIT`, `DuplicateDeliveryIT` (test 8), `ConsumerCrashSystemIT` (test 9) | Passed |
| REQ-FUNC-017 | Test | `SeedDataIT`, `StockSchemaIT` | Passed |
| REQ-FUNC-018 | Test | `KafkaTopicsIT` (both services) | Passed |
| REQ-FUNC-019 | Test | `ApplyInventoryOutcomeIT`, `InvalidTransitionIT` (test 12) | Passed |
| REQ-FUNC-020 | Test | `ReserveInventoryIT`, `ReserveInventoryAtomicityIT` | Passed |
| REQ-PERF-001 | Demonstration | 200 sequential order creations after warm-up, local Docker Desktop, client-side timing | Measured: p50 11 ms, p95 15 ms (target: p95 < 300 ms) |
| REQ-SEC-001 | Test, inspection | `JwtPropertiesTest`; configuration audit | Passed |
| REQ-SEC-002 | Test | `StructuredLogsSystemIT` (test 16) | Passed |
| REQ-REL-001 | Test | `OrderCreatedEventParserTest`, `PoisonMessageIT` (test 10) | Passed |
| REQ-OBS-001 | Test | `StructuredLogsSystemIT` (test 16) | Passed |
| REQ-COMP-001 | Inspection | License audit of all dependencies and runtime images | Passed |
| REQ-INST-001 | Demonstration | Clean-clone verification; system tests | Passed |
| REQ-BUILD-001 | Demonstration | CI jobs `build` and `system-tests`, required to merge into main | Passed |
| REQ-MAINT-001 | Test | `ArchitectureTest` (both services) | Passed |
| REQ-MAINT-002 | Inspection | [`events.md`](events.md), checked against real Kafka records | Passed |
| REQ-PORT-001 | Demonstration | Windows 11 (local) and Ubuntu (CI) | Passed |
| REQ-COST-001 | Inspection | Open-source stack, free CI tier | Passed |
| REQ-CM-001 | Inspection | One pull request per phase; tag v0.1.0 | Passed |

## 5. Appendixes

### Appendix A — Architecture overview

Two services exchange events through Kafka; each owns a schema in a single PostgreSQL instance and
publishes through its own outbox. Diagrams, flows and failure paths are in the
[Software Design Description](sdd.md).

### Appendix B — Data model

The tables, columns, constraints and ownership per schema are specified in the
[Software Design Description, DV-006](sdd.md#35-information).

### Appendix C — Business Rules

| ID | Rule |
|---|---|
| BR-001 | An order contains at least one item. |
| BR-002 | The quantity of each item is greater than zero. |
| BR-003 | Every order starts in `PENDING`. |
| BR-004 | `PENDING → CONFIRMED` only through `InventoryReserved`. |
| BR-005 | `PENDING → REJECTED` only through `InventoryRejected`. |
| BR-006 | Every event has a unique identifier. |
| BR-007 | A consumer does not apply the same event twice. |
| BR-008 | A state change and its outgoing event are persisted atomically, in both services. |
| BR-009 | PostgreSQL is the source of truth for order and stock state. |
| BR-010 | Kafka is not the source of truth for any business state. |
| BR-011 | An order may temporarily remain `PENDING`. |
| BR-012 | A request to a protected endpoint without a valid JWT is rejected with `401`. |
| BR-013 | The JWT expires after 30 minutes; a new one must then be requested. |
| BR-014 | An invalid state transition is a recorded no-op, never an exception. |
| BR-015 | A stock reservation is all or nothing for the whole order. |
| BR-016 | Decremented stock is not released in v0.1.0. |
| BR-017 | `CONFIRMED` and `REJECTED` are terminal states. |
| BR-018 | The unit price snapshot is fixed when the order is created and never recalculated. |
