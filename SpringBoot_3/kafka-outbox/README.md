# kafka-outbox

Transactional Outbox + Idempotent Consumer + DLQ demo with Spring Boot 3 & Apache Kafka.

## Local infrastructure

```bash
docker compose up -d
```

| Service     | URL                       | Credentials              |
|-------------|---------------------------|--------------------------|
| Kafbat UI   | http://localhost:8085      | —                        |
| pgAdmin     | http://localhost:5050      | admin@admin.com / admin  |
| PostgreSQL  | localhost:5432             | outbox / outbox / outbox |
| Kafka       | localhost:9092             | —                        |

### pgAdmin — first-time setup
1. Open http://localhost:5050 and log in.
2. Right-click **Servers → Register → Server**.
3. **General** tab: Name = `outbox`
4. **Connection** tab: Host = `postgres`, Port = `5432`, DB = `outbox`, User = `outbox`, Password = `outbox`.

## Build

```bash
# Unit tests only
mvn clean verify -DskipITs

# Including integration tests (Testcontainers — requires Docker)
mvn clean verify
```

## Run

```bash
mvn -pl producer spring-boot:run
mvn -pl consumer spring-boot:run
```

## Create an order (curl)

```bash
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"itemName":"widget","quantity":3}' | jq
```

## Useful commands

```bash
# Watch outbox table
psql -U outbox -h localhost outbox -c \
  "SELECT id, status, retry_count, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 20;"

# Consume main topic
docker exec -it $(docker ps -qf name=kafka) \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.events --from-beginning

# Consume DLQ
docker exec -it $(docker ps -qf name=kafka) \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.events.DLQ --from-beginning
```

## Architecture

```
POST /orders
      │
      ▼
 OrderService
 ┌──────────────────────────────────────┐
 │ DB Transaction                       │
 │  INSERT orders                       │
 │  INSERT outbox_events (status=NEW)   │
 └──────────────────────────────────────┘
      │  (committed)
      ▼
 OutboxEventRelay (@Scheduled every 1s)
      │  kafka.send(orders.events)
      │  UPDATE outbox_events status=SENT
      ▼
 Kafka topic: orders.events
      │
      ▼
 OrderEventConsumer
      ├─ idempotency check (processed_messages table)
      ├─ handle event
      └─ on failure × 3 → orders.events.DLQ
```
