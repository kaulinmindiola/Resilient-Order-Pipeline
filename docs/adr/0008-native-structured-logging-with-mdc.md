---
status: accepted
date: 2026-10-05
decision-makers: KM
---

# ADR-0008: Use Spring Boot's native structured logging with MDC correlation

## Context and Problem Statement

REQ-OBS-001 requires every log entry related to an order or event to include `orderId` and `eventId` as structured JSON fields. Distributed tracing and metrics are out of scope, so logging is the only correlation mechanism. REQ-SEC-002 forbids logging JWTs, `clientSecret` and the `Authorization` header. Which logging mechanism is used?

## Decision Drivers

* REQ-OBS-001: structured, verifiable fields.
* REQ-SEC-002: no secrets in logs.
* No extra dependencies and no hand-written logging XML.
* Zero cost: no centralized logging stack.

## Considered Options

* Spring Boot native structured logging (Logstash JSON format) + MDC
* `logstash-logback-encoder` with a custom Logback configuration
* Plain-text logs with fields concatenated by hand
* An external observability stack (ELK, Loki)

## Decision Outcome

Chosen option: **"Native structured logging + MDC"**, because it produces correlatable JSON with no additional dependencies and no configuration XML.

* Console output uses `logging.structured.format.console=logstash`.
* Kafka listeners put `eventId` and `orderId` in the MDC; the REST layer puts `orderId` when known (on `GET` and after creation).
* The MDC is cleared when each message or request finishes, so context never leaks between pooled threads.
* Duplicate events log at INFO and no-op transitions at WARN, both carrying the same fields.

### Consequences

* Good, because correlation is consistent and independent of per-statement discipline.
* Good, because no dependency or XML is added.
* Bad, because JSON logs are harder to read in a local console.
* Bad, because there is no log aggregator; correlation is done by filtering JSON fields.

### Confirmation

Verified by [`StructuredLogsSystemIT`](../../system-tests/src/test/java/io/github/kaulinmindiola/rop/system/StructuredLogsSystemIT.java)
on the real container output: every step of an order's journey is a JSON line carrying `orderId`,
event handling also carries `eventId`, and no access token, client secret, signing key or password
appears in any log.

## Pros and Cons of the Options

### Native structured logging + MDC

* Good, because it needs no extra dependency.
* Good, because the output is automatable.
* Bad, because it is less readable in a plain console.

### `logstash-logback-encoder`

* Good, because it is highly configurable.
* Bad, because it adds a dependency and XML for something Spring Boot now does natively.

### Plain text

* Good, because it is readable without tools.
* Bad, because it depends on discipline in every log statement and is easy to get wrong.

### External observability stack

* Bad, because it is disproportionate infrastructure for this scope, and tracing and metrics are out of scope.

## More Information

* Related: [ADR-0001](0001-transactional-outbox-polling-publisher.md) (the publisher logs `eventId`), [ADR-0004](0004-consumer-retry-and-dead-letter-topics.md) (retries and DLT log the same fields), [ADR-0005](0005-self-issued-jwt-with-resource-server.md).
* Requirements: REQ-OBS-001, REQ-SEC-002.
