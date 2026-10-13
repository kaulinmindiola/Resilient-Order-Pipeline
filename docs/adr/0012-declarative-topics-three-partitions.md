---
status: accepted
date: 2026-10-05
decision-makers: KM
---

# ADR-0012: Declare topics as code with 3 partitions and replication factor 1

## Context and Problem Statement

Per-key ordering ([ADR-0003](0003-partition-key-aggregate-id.md)) only means something when a topic has more than one partition. REQ-FUNC-018 requires the topics to exist with 3 partitions after startup, with no `auto.create` and no manual scripts. How are topics created and sized?

## Decision Drivers

* REQ-FUNC-018: declarative, deterministic topic creation.
* Make ADR-0003 demonstrable with a test (needs more than one partition).
* REQ-INST-001: no manual steps.
* `DeadLetterPublishingRecoverer` publishes to the same partition number as the failed record.

## Considered Options

* `NewTopic` beans in each service, `auto.create.topics.enable=false`
* Broker auto-creation of topics
* An initialization script (`kafka-topics.sh`) in Compose
* A single partition per topic

## Decision Outcome

Chosen option: **"`NewTopic` beans with 3 partitions and replication factor 1"**, because topics become part of the code, startup is deterministic and ordering by key can be tested.

| Topic | Producer | Consumer group |
|---|---|---|
| `order.created` | `order-service` | `inventory-service` |
| `inventory.reserved` | `inventory-service` | `order-service` |
| `inventory.rejected` | `inventory-service` | `order-service` |
| `order.created.DLT`, `inventory.reserved.DLT`, `inventory.rejected.DLT` | consumer error handler | manual inspection |

* All six topics use 3 partitions and replication factor 1; `auto.create.topics.enable=false` on the broker.
* The `.DLT` topics need 3 partitions because the recoverer publishes to the same partition as the original record.
* Each service declares, with an identical definition, the topics it produces or consumes and the `.DLT` topics it consumes, so neither service depends on the other's startup.

### Consequences

* Good, because topic topology is versioned and reviewable.
* Good, because ADR-0003 can be proven with a real multi-partition test.
* Bad, because partition counts are fixed; changing them later is a deliberate operation.
* Bad, because replication factor 1 gives no durability against broker loss ([ADR-0007](0007-kafka-kraft-single-node.md)).
* Bad, because definitions are duplicated across services, an accepted cost under [ADR-0016](0016-independent-services-no-shared-module.md).

### Confirmation

Verified in both services by `KafkaTopicsIT`
([order-service](../../order-service/src/test/java/io/github/kaulinmindiola/rop/order/adapter/out/kafka/KafkaTopicsIT.java),
[inventory-service](../../inventory-service/src/test/java/io/github/kaulinmindiola/rop/inventory/adapter/out/kafka/KafkaTopicsIT.java)):
with broker-side auto-creation disabled, every topic each service declares exists at startup with 3
partitions and replication factor 1. Together they cover the six topics.

## Pros and Cons of the Options

### `NewTopic` beans

* Good, because it is declarative and testable.
* Bad, because definitions are repeated in both services.

### Broker auto-creation

* Good, because it needs no configuration.
* Bad, because partition counts and names depend on defaults and typos create topics silently.

### Compose init script

* Good, because it keeps topics out of application code.
* Bad, because it is an extra manual-style step outside the services and weakens determinism.

### Single partition

* Good, because ordering is trivial.
* Bad, because per-key ordering would be impossible to demonstrate.

## More Information

* Related: [ADR-0003](0003-partition-key-aggregate-id.md), [ADR-0004](0004-consumer-retry-and-dead-letter-topics.md), [ADR-0007](0007-kafka-kraft-single-node.md).
* Requirements: REQ-FUNC-018, REQ-INST-001.
