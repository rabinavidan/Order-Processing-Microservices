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
[Order Service]  ──── Kafka topic: orders ────▶  [Notification Service]
  FastAPI :8000                           │           kafka-python consumer
      │                                   │
      ▼                                   ▼
  GET /health                    [Inventory Service]
                                  checks & reserves stock
                                           │
                                           │ Kafka topic: inventory.reserved
                                           ▼
                                  [Payment Service]
                                  processes payment
                                           │
                                           │ Kafka topic: payments.processed
```

| Service              | Role                                                      | Tech             |
|----------------------|-----------------------------------------------------------|------------------|
| Order Service        | REST API — validates and publishes orders                 | FastAPI, Uvicorn |
| Notification Service | Consumes `orders`, logs notifications                     | kafka-python     |
| Inventory Service    | Consumes `orders`, reserves stock, publishes result       | kafka-python     |
| Payment Service      | Consumes `inventory.reserved`, processes payment          | kafka-python     |
| Kafka (KRaft)        | Message broker                                            | Confluent 7.6.1  |

### Kafka Topics

| Topic                  | Producer          | Consumer(s)                          |
|------------------------|-------------------|--------------------------------------|
| `orders`               | Order Service     | Notification Service, Inventory Service |
| `inventory.reserved`   | Inventory Service | Payment Service                      |
| `payments.processed`   | Payment Service   | —                                    |

### In-memory stock (Inventory Service)

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
├── architecture.html               # Visual system design diagram
├── contracts/                      # Kafka topic JSON Schemas (producer/consumer contract)
│   ├── orders.schema.json
│   ├── inventory_reserved.schema.json
│   └── payments_processed.schema.json
├── e2e/                             # Cross-service end-to-end test (real Kafka via testcontainers)
│   ├── conftest.py
│   ├── test_order_pipeline.py
│   └── requirements.txt
├── services/
│   ├── order_service/
│   │   ├── main.py                 # FastAPI app (POST /orders, GET /health)
│   │   ├── producer.py             # KafkaProducer wrapper
│   │   ├── models.py               # Pydantic Order model
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_api.py         # Endpoint tests (TestClient)
│   │       ├── test_producer.py    # Producer unit tests
│   │       └── test_contract.py    # `orders` topic contract tests
│   ├── notification_service/
│   │   ├── main.py                 # Consumer entry point
│   │   ├── consumer.py             # KafkaConsumer wrapper
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_consumer.py    # Consumer unit tests
│   │       └── test_contract.py    # `orders` topic contract tests
│   ├── inventory_service/
│   │   ├── main.py                 # Consumer entry point
│   │   ├── consumer.py             # Consumes orders, reserves stock
│   │   ├── producer.py             # Publishes to inventory.reserved
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_consumer.py    # Stock reservation logic tests
│   │       ├── test_producer.py    # Producer unit tests
│   │       └── test_contract.py    # `orders` / `inventory.reserved` contract tests
│   └── payment_service/
│       ├── main.py                 # Consumer entry point
│       ├── consumer.py             # Consumes inventory.reserved, processes payment
│       ├── producer.py             # Publishes to payments.processed
│       ├── requirements.txt
│       ├── Dockerfile
│       └── tests/
│           ├── test_consumer.py    # Payment processing logic tests
│           ├── test_producer.py    # Producer unit tests
│           └── test_contract.py    # `inventory.reserved` / `payments.processed` contract tests
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
- Order Service API on `http://localhost:8000`
- Notification Service (background consumer)
- Inventory Service (background consumer)
- Payment Service (background consumer)

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

---

## Testing Strategy

Tests are layered like a pyramid:

| Layer        | Where                                    | What it proves                                                                 | Speed / deps            |
|--------------|-------------------------------------------|---------------------------------------------------------------------------------|--------------------------|
| **Unit**     | `services/*/tests/test_{producer,consumer,api}.py` | Each service's business logic in isolation, Kafka fully mocked                  | Milliseconds, no Docker |
| **Contract** | `services/*/tests/test_contract.py`       | Every producer's output — and every consumer's input handling — matches the shared JSON Schema for its topic in [`contracts/`](contracts) | Milliseconds, no Docker |
| **End-to-end** | [`e2e/test_order_pipeline.py`](e2e/test_order_pipeline.py) | The full saga over a **real** Kafka broker (testcontainers): `orders` → `inventory.reserved` → `payments.processed`, plus the independent `notification_service` delivery | ~30s, needs Docker      |

The contract tests exist specifically to catch the failure mode unit tests miss: a producer and a downstream consumer silently drifting apart (e.g. a renamed field) while each service's own mocked tests keep passing. See [`contracts/README.md`](contracts/README.md) for details.

## Continuous Integration

Every push and pull request runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml), which:

- Runs `pytest` (unit + contract tests) for each of the 4 services in a matrix job (Python 3.11)
- Enforces a **70% coverage gate** per service (`--cov-fail-under=70`) — a service with a test suite too thin to reach 70% fails the build
- Collects coverage with `pytest-cov` and uploads an HTML/XML coverage report as a CI artifact per service
- Publishes coverage to Codecov (aggregated across all 4 services)
- Runs the [end-to-end pipeline test](e2e/test_order_pipeline.py) in its own job, spinning up a real Kafka broker via testcontainers

## Running Tests Locally

Unit and contract tests use mocked Kafka — no Docker required. Add `--cov=. --cov-report=term-missing` to any `pytest` command below to see coverage locally.

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

**End-to-end pipeline test** (needs Docker running locally)
```bash
cd e2e
pip install -r requirements.txt
pytest -v
```

### Test coverage

| File                                        | Tests                                                                 |
|---------------------------------------------|-----------------------------------------------------------------------|
| `order_service/test_api.py`                 | POST /orders → 201, invalid payload → 422, wrong type → 422, missing field → 422, GET /health → 200, GET /orders → 405, response body, producer called with payload |
| `order_service/test_producer.py`            | `send_order()` calls Kafka send + flush, `close()` calls Kafka close |
| `order_service/test_contract.py`            | producer output validates against `orders` schema; schema rejects missing/wrong-typed fields |
| `notification_service/test_consumer.py`     | logs order fields, handles missing fields, multiple messages, consumer topic/group config, `close()` |
| `notification_service/test_contract.py`     | consumer accepts any `orders`-schema-valid message                    |
| `inventory_service/test_consumer.py`        | reserved/insufficient/boundary/unknown product/field preservation    |
| `inventory_service/test_producer.py`        | `send()` calls Kafka send + flush, `close()` calls Kafka close       |
| `inventory_service/test_contract.py`        | consumer accepts `orders`-schema messages; producer output validates against `inventory.reserved` schema |
| `payment_service/test_consumer.py`          | paid when reserved, skips when insufficient/missing, field preservation |
| `payment_service/test_producer.py`          | `send()` calls Kafka send + flush, `close()` calls Kafka close       |
| `payment_service/test_contract.py`          | consumer accepts `inventory.reserved`-schema messages; producer output validates against `payments.processed` schema |
| `e2e/test_order_pipeline.py`                | full order saga over a real Kafka broker, across all 4 services' actual code |

---

## API Reference

### `POST /orders`

Create a new order and publish it to Kafka.

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

| Variable                 | Default          | Description          |
|--------------------------|------------------|----------------------|
| `KAFKA_BOOTSTRAP_SERVERS`| `localhost:9092` | Kafka broker address |

---

## System Design

Open `architecture.html` in a browser for an interactive visual diagram of the full system.
