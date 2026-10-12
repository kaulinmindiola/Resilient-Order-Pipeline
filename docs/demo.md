# Demo — five minutes with Resilient Order Pipeline

A guided tour of what makes this project different: not the happy path, but what happens when things
break. Every step below is also covered by an automated test (see the README's
[Evidence](../README.md#evidence) table).

Commands run from the repository root and work in PowerShell and in bash. API calls use
[`requests.http`](requests.http) (IntelliJ's HTTP client) or the commands of the README's
[Quick start](../README.md#quick-start).

## 1. Start from scratch

```bash
docker compose down -v
docker compose up --build -d
docker compose logs order-service | grep "Started OrderServiceApplication"   # PowerShell: | Select-String "Started"
```

Four containers: two services, PostgreSQL and Kafka. The databases are created and seeded by Flyway.

## 2. The happy path and every outcome

Run `requests.http` from the top, wait about 15 seconds, then run the "Check outcomes" section.

| Order         | Outcome     | Why                                      |
|---            |---          |---                                       |
| `SKU-001`     | `CONFIRMED` | Stock available                          |
| `SKU-002 (1)` | `CONFIRMED` | Takes the last unit                      |
| `SKU-002 (2)` | `REJECTED`  | No stock left                            |
| `SKU-003`     | `REJECTED`  | Insufficient stock                       |
| `SKU-004`     | `REJECTED`  | Unknown product in inventory             |
| `SKU-999`     | `422`       | Not in the catalog: refused upfront      |

Follow one order across both services through its id:

```bash
docker compose logs order-service inventory-service | grep "<orderId>"
```

Every line is JSON and carries `orderId`; event handling also carries `eventId`.

## 3. Kafka goes down: no order is lost

```bash
docker compose pause kafka
```

Create an `SKU-001` order: the API still answers `201`, and the order stays `PENDING`, because its
event waits in the outbox table, in the same database transaction as the order. The publisher keeps
retrying.

```bash
docker compose unpause kafka
```

Within a few seconds the order is `CONFIRMED`, with no manual action.

## 4. A poison message does not block the pipeline

```bash
echo "poison:this is not json" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic order.created \
  --reader-property parse.key=true --reader-property key.separator=:
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic order.created.DLT --from-beginning --max-messages 1 --timeout-ms 10000 \
  --formatter-property print.key=true
```

The invalid message lands in `order.created.DLT` immediately, without retries, and orders created
afterward are still processed. A transient failure would instead be retried three times (1, 2 and 4
seconds) before being dead-lettered.

## 5. The consumer crashes: nothing is lost or duplicated

Create a few orders and, right away, kill `inventory-service` without a graceful shutdown:

```bash
docker compose kill inventory-service
docker compose up -d inventory-service
```

Every order still reaches its terminal status, and stock is decremented exactly once per order:
unfinished transactions were rolled back by PostgreSQL, uncommitted offsets were redelivered by Kafka,
and redeliveries were recognized by the consumer's claim on the event id.

## 6. Clean up

```bash
docker compose down -v
```

## What to look at next

- [Architecture decision records](adr/README.md): why each of these behaviors is designed the way it is.
- [Software design description](sdd.md): sequence diagrams for the happy path and the Kafka outage.
- The tests behind each step: [Evidence](../README.md#evidence).
