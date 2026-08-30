# Order Processing — Microservices

[![CI](https://github.com/rabinavidan/Order-Processing-Microservices/actions/workflows/ci.yml/badge.svg)](https://github.com/rabinavidan/Order-Processing-Microservices/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rabinavidan/Order-Processing-Microservices/branch/master/graph/badge.svg)](https://codecov.io/gh/rabinavidan/Order-Processing-Microservices)

A mini microservices project demonstrating **FastAPI**, **Apache Kafka**, **Docker**, and **pytest** using an order processing domain.

---

## Architecture

```
[HTTP Client]
      │
      │ POST /orders
      ▼
[Order Service] ── DB write + outbox ──▶ [Postgres: order_db]
      │                                        │ OutboxRelay
      ▼                                        ▼
  GET /health                     Kafka topic: orders ────▶ [Notification Service] ──▶ [Postgres: notification_db]
                                           │
                                           ▼
                                  [Inventory Service] ── DB write + outbox ──▶ [Postgres: inventory_db]
                                  checks & reserves stock       │ OutboxRelay
                                                                 ▼
                                                    Kafka topic: inventory.reserved
                                                                 │
                                                                 ▼
                                                    [Payment Service] ── DB write + outbox ──▶ [Postgres: payment_db]
                                                    processes payment       │ OutboxRelay
                                                                             ▼
                                                                Kafka topic: payments.processed
```

Every state change (an order created, stock reserved, a payment recorded) is written to that service's own Postgres database and published to Kafka via the [Outbox pattern](#persistence) — the two can never disagree, even across a crash or a Kafka outage. See [Persistence](#persistence) and [Resilience](#resilience) below.

| Service              | Role                                                      | Tech                          |
|----------------------|-----------------------------------------------------------|--------------------------------|
| Order Service        | REST API — validates, persists, and publishes orders      | FastAPI, Uvicorn, SQLAlchemy   |
| Notification Service | Consumes `orders`, logs notifications                     | kafka-python, SQLAlchemy       |
| Inventory Service    | Consumes `orders`, reserves stock, publishes result        | kafka-python, SQLAlchemy       |
| Payment Service      | Consumes `inventory.reserved`, processes payment           | kafka-python, SQLAlchemy       |
| Kafka (KRaft)        | Message broker                                             | Confluent 7.6.1               |
| Postgres             | Database-per-service persistence (one instance, 4 DBs)     | Postgres 16                   |

### Kafka Topics

| Topic                     | Producer               | Consumer(s)                             |
|----------------------------|-------------------------|-------------------------------------------|
| `orders`                  | Order Service          | Notification Service, Inventory Service |
| `inventory.reserved`      | Inventory Service      | Payment Service                         |
| `payments.processed`      | Payment Service        | —                                        |
| `orders.dlq`               | Inventory Service, Notification Service | — (quarantine, see [Resilience](#resilience)) |
| `inventory.reserved.dlq`   | Payment Service         | — (quarantine, see [Resilience](#resilience)) |

### Seeded stock (Inventory Service)

Persisted in the `stock` table (`inventory_db`), seeded by the initial Alembic migration:

| Product    | Initial stock |
|------------|---------------|
| `laptop`   | 10            |
| `phone`    | 5             |
| `keyboard` | 20            |

---

## Project Structure

```
.
├── docker-compose.yml
├── architecture.html                # Visual system design diagram
├── docs/adr/                        # Architecture Decision Records
│   ├── 0001-postgres-database-per-service.md
│   └── 0002-in-process-outbox-relay.md
├── infra/postgres/
│   └── init-databases.sql          # Creates the 4 per-service databases on first boot
├── contracts/                      # Kafka topic JSON Schemas (producer/consumer contract)
│   ├── orders.schema.json
│   ├── inventory_reserved.schema.json
│   ├── payments_processed.schema.json
│   └── dlq_envelope.schema.json
├── e2e/                             # Cross-service end-to-end test (real Kafka + Postgres via testcontainers)
│   ├── conftest.py
│   ├── test_order_pipeline.py
│   └── requirements.txt
├── services/
│   ├── order_service/
│   │   ├── main.py                 # FastAPI app (POST /orders, GET /health) + outbox relay thread
│   │   ├── db.py                   # SQLAlchemy models: OrderRecord, OutboxEvent
│   │   ├── outbox_relay.py         # Publishes outbox rows to Kafka (Outbox pattern)
│   │   ├── producer.py             # KafkaProducer wrapper (used by the relay)
│   │   ├── models.py               # Pydantic Order request model
│   │   ├── alembic/                # DB migrations
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_api.py         # Endpoint tests (TestClient) + idempotent POST
│   │       ├── test_producer.py    # Producer unit tests
│   │       ├── test_contract.py    # `orders` topic contract tests
│   │       ├── test_outbox_relay.py
│   │       └── test_migrations.py
│   ├── notification_service/
│   │   ├── main.py                 # Entry point + signal handling for graceful shutdown
│   │   ├── consumer.py             # Idempotent (DB-backed), retrying KafkaConsumer wrapper
│   │   ├── db.py                   # SQLAlchemy model: ProcessedMessage (idempotency ledger)
│   │   ├── dlq_producer.py         # Publishes poison messages to orders.dlq
│   │   ├── alembic/                # DB migrations
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_consumer.py    # Consumer + idempotency/retry/DLQ/shutdown tests
│   │       ├── test_contract.py    # `orders` + DLQ envelope contract tests
│   │       └── test_migrations.py
│   ├── inventory_service/
│   │   ├── main.py                 # Entry point + signal handling for graceful shutdown
│   │   ├── consumer.py             # Consumes orders, reserves stock (idempotent, retrying, DB-backed)
│   │   ├── db.py                   # SQLAlchemy models: Stock, ProcessedMessage, OutboxEvent
│   │   ├── outbox_relay.py         # Publishes outbox rows to Kafka (Outbox pattern)
│   │   ├── producer.py             # Publishes to inventory.reserved (used by the relay)
│   │   ├── dlq_producer.py         # Publishes poison messages to orders.dlq
│   │   ├── alembic/                # DB migrations (also seeds initial stock)
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_consumer.py    # Reservation logic + idempotency/retry/DLQ/shutdown tests
│   │       ├── test_producer.py    # Producer unit tests
│   │       ├── test_contract.py    # `orders` / `inventory.reserved` / DLQ contract tests
│   │       ├── test_outbox_relay.py
│   │       └── test_migrations.py
│   └── payment_service/
│       ├── main.py                 # Entry point + signal handling for graceful shutdown
│       ├── consumer.py             # Consumes inventory.reserved, processes payment (idempotent, retrying, DB-backed)
│       ├── db.py                   # SQLAlchemy models: Payment (business record + idempotency ledger), OutboxEvent
│       ├── outbox_relay.py         # Publishes outbox rows to Kafka (Outbox pattern)
│       ├── producer.py             # Publishes to payments.processed (used by the relay)
│       ├── dlq_producer.py         # Publishes poison messages to inventory.reserved.dlq
│       ├── alembic/                # DB migrations
│       ├── requirements.txt
│       ├── Dockerfile
│       └── tests/
│           ├── test_consumer.py    # Payment logic + idempotency/retry/DLQ/shutdown tests
│           ├── test_producer.py    # Producer unit tests
│           ├── test_contract.py    # `inventory.reserved` / `payments.processed` / DLQ contract tests
│           ├── test_outbox_relay.py
│           └── test_migrations.py
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) running
- Python 3.11+ (for running tests locally)

---

## Running the Full Stack

```bash
docker-compose up --build
```

Services started:
- Kafka broker on `localhost:9092`
- Postgres on `localhost:5432` (4 databases: `order_db`, `inventory_db`, `payment_db`, `notification_db` — see [Persistence](#persistence))
- Order Service API on `http://localhost:8000`
- Notification Service (background consumer)
- Inventory Service (background consumer)
- Payment Service (background consumer)

Each service runs its Alembic migrations automatically on startup — no manual migration step needed.

### Send a test order

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "1", "product": "laptop", "quantity": 2}'
```

Expected response:
```json
{"message": "Order received", "order_id": "1"}
```

Watch the docker-compose logs — you will see:
1. **Notification Service** logs the incoming order
2. **Inventory Service** logs the stock reservation
3. **Payment Service** logs the payment result

### Health check

```bash
curl http://localhost:8000/health
```

### Stop

```bash
docker-compose down
```

Orders, stock levels, and payment records all persist in Postgres (`postgres_data` is a named Docker volume) — stop and restart the stack (`docker-compose down` then `docker-compose up`) and everything you created is still there. Only `docker-compose down -v` discards the volume and truly resets the stack to its seeded state.

---

## Persistence

Before this, `order_service` had no persisted state at all and the other 3 services kept theirs in plain in-process `dict`/`set` objects — a restart silently reset everything. Now every service owns a Postgres database and writes through SQLAlchemy 2.0, with Alembic migrations (`alembic upgrade head`, run automatically at startup — see each service's `main.py`) as the only way its schema changes.

### Database-per-service

One Postgres container in `docker-compose.yml`, but 4 separate databases inside it (created by [`infra/postgres/init-databases.sql`](infra/postgres/init-databases.sql) on first boot) — no service can reach into another's tables:

| Service               | Database             | Tables                                              |
|------------------------|------------------------|--------------------------------------------------------|
| Order Service          | `order_db`            | `orders`, `outbox_events`                              |
| Inventory Service      | `inventory_db`        | `stock` (seeded by migration), `processed_messages`, `outbox_events` |
| Payment Service        | `payment_db`          | `payments` (doubles as the idempotency ledger), `outbox_events` |
| Notification Service   | `notification_db`     | `processed_messages`                                    |

See [ADR 0001](docs/adr/0001-postgres-database-per-service.md) for why Postgres over SQLite.

### The Outbox pattern

`order_service`, `inventory_service`, and `payment_service` each need to change durable state *and* publish a Kafka event about it, atomically — otherwise a crash between the two leaves them disagreeing. Each writes its state change and an `OutboxEvent` row in the **same DB transaction** (`_process()` / `create_order()`), so the two can never drift apart. A background `OutboxRelay` (one per service, a daemon thread started in `main.py`) separately polls its own `outbox_events` table and publishes unpublished rows to Kafka, marking them published — a broker outage at processing time delays the publish, but never loses or half-applies the update. See [ADR 0002](docs/adr/0002-in-process-outbox-relay.md) for the relay's design and trade-offs.

### Durable idempotency

The M2 in-memory dedup set is now the `processed_messages` table (or, for `payment_service`, the `payments` table itself) — a DB row, not a process-local `set`, so a redelivery that lands after a restart is still recognized and skipped. See [Resilience](#resilience) below for the full idempotency + retry + DLQ story this plugs into.

---

## Resilience

The 3 consumer services (`notification_service`, `inventory_service`, `payment_service`) share the same failure-handling design, so a redelivered or malformed message never double-processes an order or crashes a consumer.

### At-least-once delivery, made safe

Each consumer sets `enable_auto_commit=False` and commits its offset **only after** a message has been fully handled — processed successfully, or routed to the DLQ. If the process crashes between receiving a message and committing, that message is redelivered on restart (standard Kafka at-least-once semantics). Naively this could double-reserve stock or double-charge a payment; the idempotency layer below is what makes redelivery safe instead of dangerous.

Each consumer group is also explicitly named and stable across restarts:

| Service               | `group_id`             | Topic consumed        |
|------------------------|--------------------------|--------------------------|
| Notification Service  | `notification-group`    | `orders`                |
| Inventory Service     | `inventory-group`       | `orders`                |
| Payment Service       | `payment-group`         | `inventory.reserved`    |

### Idempotent processing

Every consumer checks a durable ledger (a Postgres table — see [Persistence](#persistence)) for `order_id`s it has already processed successfully. A redelivered message for an `order_id` already recorded there is logged and skipped before it reaches business logic (`_process`) — so a duplicate `orders` message never decrements inventory stock twice, and a duplicate `inventory.reserved` message never charges payment twice. Because this ledger lives in the database rather than process memory, dedup holds across a restart too, not just within one process's lifetime — each service's `tests/test_consumer.py` has a dedicated test proving this (`test_idempotency_survives_a_restart`).

### Retry with backoff, then dead-letter

Each consumer wraps `_process()` in a retry loop (3 attempts by default, exponential backoff starting at 0.5s) before giving up. A message that still fails after all retries — or one that can't even be JSON-decoded — is published as-is to a dead-letter topic instead of crashing the consumer or blocking the partition forever:

- `orders.dlq` — poison messages from `notification_service` and `inventory_service`
- `inventory.reserved.dlq` — poison messages from `payment_service`

Each DLQ topic is shared across the consumer groups that read its source topic; the envelope (see [`contracts/dlq_envelope.schema.json`](contracts/dlq_envelope.schema.json)) carries `consumer_group`, `error`, `original_value`, and `failed_at` so a failed message can be triaged and, once fixed, replayed.

### Graceful shutdown

Each service's `main.py` registers `SIGTERM`/`SIGINT` handlers that call the consumer's `stop()` method. The poll loop (`poll(timeout_ms=1000)` rather than the blocking `for message in consumer:` idiom) checks a running flag between messages, so a shutdown signal is picked up within ~1s instead of only between broker round-trips, and never interrupts a message mid-processing — avoiding both lost messages and unnecessary duplicate processing on the next restart.

---

## Testing Strategy

Tests are layered like a pyramid:

| Layer        | Where                                    | What it proves                                                                 | Speed / deps            |
|--------------|-------------------------------------------|---------------------------------------------------------------------------------|--------------------------|
| **Unit**     | `services/*/tests/test_{producer,consumer,api}.py` | Each service's business logic in isolation, Kafka fully mocked                  | Milliseconds, no Docker |
| **Contract** | `services/*/tests/test_contract.py`       | Every producer's output — and every consumer's input handling — matches the shared JSON Schema for its topic in [`contracts/`](contracts) | Milliseconds, no Docker |
| **End-to-end** | [`e2e/test_order_pipeline.py`](e2e/test_order_pipeline.py) | The full saga over a **real** Kafka broker and a **real** Postgres instance (testcontainers): `orders` → `inventory.reserved` → `payments.processed`, plus the independent `notification_service` delivery — each step's DB state (stock decremented, payment recorded) is asserted directly, not just the Kafka messages | ~20-30s, needs Docker |

The contract tests exist specifically to catch the failure mode unit tests miss: a producer and a downstream consumer silently drifting apart (e.g. a renamed field) while each service's own mocked tests keep passing. See [`contracts/README.md`](contracts/README.md) for details.

Every service with a database also has `tests/test_migrations.py`, which runs that service's real Alembic migration against a throwaway SQLite file and asserts the expected tables (and, for `inventory_service`, the seeded stock rows) exist — this is what proves the migration files themselves are correct, independent of which database engine is behind them. Services with an outbox also have `tests/test_outbox_relay.py`, testing publish/mark-published, skipping already-published rows, and leaving a row unpublished (to retry on the next poll) when the producer fails.

## Continuous Integration

Every push and pull request runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml), which:

- Runs `pytest` (unit + contract tests) for each of the 4 services in a matrix job (Python 3.11)
- Enforces a **70% coverage gate** per service (`--cov-fail-under=70`) — a service with a test suite too thin to reach 70% fails the build
- Collects coverage with `pytest-cov` and uploads an HTML/XML coverage report as a CI artifact per service
- Publishes coverage to Codecov (aggregated across all 4 services)
- Runs the [end-to-end pipeline test](e2e/test_order_pipeline.py) in its own job, spinning up a real Kafka broker and a real Postgres instance via testcontainers — which also proves every service's Alembic migrations apply cleanly against Postgres, not just the SQLite used by the per-service `test_migrations.py`

## Running Tests Locally

Unit, contract, and migration tests use mocked Kafka and an in-memory SQLite database — no Docker required. Add `--cov=. --cov-report=term-missing` to any `pytest` command below to see coverage locally.

**Order Service**
```bash
cd services/order_service
pip install -r requirements.txt
pytest
```

**Notification Service**
```bash
cd services/notification_service
pip install -r requirements.txt
pytest
```

**Inventory Service**
```bash
cd services/inventory_service
pip install -r requirements.txt
pytest
```

**Payment Service**
```bash
cd services/payment_service
pip install -r requirements.txt
pytest
```

**End-to-end pipeline test** (needs Docker running locally — spins up its own throwaway Kafka and Postgres via testcontainers, nothing else needs to be running)
```bash
cd e2e
pip install -r requirements.txt
pytest -v
```

### Test coverage

| File                                        | Tests                                                                 |
|---------------------------------------------|-----------------------------------------------------------------------|
| `order_service/test_api.py`                 | POST /orders → 201, invalid payload → 422, wrong type → 422, missing field → 422, GET /health → 200, GET /orders → 405, response body, order + outbox event persisted, repeated POST is idempotent |
| `order_service/test_producer.py`            | `send()` calls Kafka send + flush, `close()` calls Kafka close       |
| `order_service/test_contract.py`            | producer output validates against `orders` schema; schema rejects missing/wrong-typed fields; outbox event written by the API also validates |
| `order_service/test_outbox_relay.py`        | publishes + marks pending rows, skips already-published, leaves a row unpublished on producer failure and retries it later, processes multiple rows in order, `stop()` |
| `order_service/test_migrations.py`          | migration creates `orders` + `outbox_events`, is idempotent to re-run |
| `notification_service/test_consumer.py`     | logs order fields, handles missing fields, multiple messages, consumer topic/group config, `close()`, duplicate order_id skipped, idempotency survives a restart, malformed JSON → DLQ, retry-then-succeed, retries exhausted → DLQ, graceful stop |
| `notification_service/test_contract.py`     | consumer accepts any `orders`-schema-valid message; DLQ output validates against DLQ envelope schema |
| `notification_service/test_migrations.py`   | migration creates `processed_messages`, is idempotent to re-run     |
| `inventory_service/test_consumer.py`        | reserved/insufficient/boundary/unknown product/field preservation (via outbox events + Stock table), duplicate order_id skipped, idempotency survives a restart, malformed JSON → DLQ, retry-then-succeed, retries exhausted → DLQ, graceful stop |
| `inventory_service/test_producer.py`        | `send()` calls Kafka send + flush, `close()` calls Kafka close       |
| `inventory_service/test_contract.py`        | consumer accepts `orders`-schema messages; producer output validates against `inventory.reserved` schema; DLQ output validates against DLQ envelope schema |
| `inventory_service/test_outbox_relay.py`    | publishes + marks pending rows, skips already-published, leaves a row unpublished on producer failure and retries it later, processes multiple rows in order, `stop()` |
| `inventory_service/test_migrations.py`      | migration creates `stock`/`processed_messages`/`outbox_events` and seeds default stock, is idempotent to re-run |
| `payment_service/test_consumer.py`          | paid when reserved, skips when insufficient/missing (via outbox events + Payment table), duplicate order_id skipped, idempotency survives a restart, malformed JSON → DLQ, retry-then-succeed, retries exhausted → DLQ, graceful stop |
| `payment_service/test_producer.py`          | `send()` calls Kafka send + flush, `close()` calls Kafka close       |
| `payment_service/test_contract.py`          | consumer accepts `inventory.reserved`-schema messages; producer output validates against `payments.processed` schema; DLQ output validates against DLQ envelope schema |
| `payment_service/test_outbox_relay.py`      | publishes + marks pending rows, skips already-published, leaves a row unpublished on producer failure and retries it later, processes multiple rows in order, `stop()` |
| `payment_service/test_migrations.py`        | migration creates `payments` + `outbox_events`, is idempotent to re-run |
| `e2e/test_order_pipeline.py`                | full order saga over a real Kafka broker and a real Postgres instance, across all 4 services' actual code, asserting DB state at each step |

---

## API Reference

### `POST /orders`

Create a new order: persists it in Postgres and writes an outbox event in the same transaction (see [Persistence](#persistence)) for a background relay to publish to Kafka. Idempotent — POSTing the same `order_id` again returns the same response without creating a duplicate.

**Request body**
```json
{
  "order_id": "string",
  "product": "string",
  "quantity": 1
}
```

**Response `201`**
```json
{
  "message": "Order received",
  "order_id": "string"
}
```

### `GET /health`

**Response `200`**
```json
{"status": "ok"}
```

---

## Environment Variables

| Variable                 | Default                          | Description                          |
|--------------------------|-----------------------------------|----------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`| `localhost:9092`                 | Kafka broker address                 |
| `DATABASE_URL`           | `sqlite:///./<service>.db`       | SQLAlchemy connection string for this service's database (docker-compose sets a Postgres URL per service — see [Persistence](#persistence)) |

---

## Architecture Decision Records

- [ADR 0001: Postgres, database-per-service, on one shared instance](docs/adr/0001-postgres-database-per-service.md)
- [ADR 0002: In-process, poll-based Outbox relay](docs/adr/0002-in-process-outbox-relay.md)

---

## System Design

Open `architecture.html` in a browser for an interactive visual diagram of the full system.
