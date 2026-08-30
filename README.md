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
├── services/
│   ├── order_service/
│   │   ├── main.py                 # FastAPI app (POST /orders, GET /health)
│   │   ├── producer.py             # KafkaProducer wrapper
│   │   ├── models.py               # Pydantic Order model
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_api.py         # Endpoint tests (TestClient)
│   │       └── test_producer.py    # Producer unit tests
│   ├── notification_service/
│   │   ├── main.py                 # Consumer entry point
│   │   ├── consumer.py             # KafkaConsumer wrapper
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       └── test_consumer.py    # Consumer unit tests
│   ├── inventory_service/
│   │   ├── main.py                 # Consumer entry point
│   │   ├── consumer.py             # Consumes orders, reserves stock
│   │   ├── producer.py             # Publishes to inventory.reserved
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── tests/
│   │       ├── test_consumer.py    # Stock reservation logic tests
│   │       └── test_producer.py    # Producer unit tests
│   └── payment_service/
│       ├── main.py                 # Consumer entry point
│       ├── consumer.py             # Consumes inventory.reserved, processes payment
│       ├── producer.py             # Publishes to payments.processed
│       ├── requirements.txt
│       ├── Dockerfile
│       └── tests/
│           ├── test_consumer.py    # Payment processing logic tests
│           └── test_producer.py    # Producer unit tests
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

## Continuous Integration

Every push and pull request runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml), which:

- Runs `pytest` for each of the 4 services in a matrix job (Python 3.11)
- Collects coverage with `pytest-cov` and uploads an HTML/XML coverage report as a CI artifact per service
- Publishes coverage to Codecov (aggregated across all 4 services)

## Running Tests Locally

Tests use mocked Kafka — no Docker required. Add `--cov=. --cov-report=term-missing` to any `pytest` command below to see coverage locally.

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

### Test coverage

| File                                        | Tests                                                                 |
|---------------------------------------------|-----------------------------------------------------------------------|
| `order_service/test_api.py`                 | POST /orders → 201, invalid payload → 422, GET /health → 200         |
| `order_service/test_producer.py`            | `send_order()` calls Kafka send + flush, `close()` calls Kafka close |
| `notification_service/test_consumer.py`     | `consume()` logs order data, `close()` calls Kafka close             |
| `inventory_service/test_consumer.py`        | reserved/insufficient/boundary/unknown product/field preservation    |
| `inventory_service/test_producer.py`        | `send()` calls Kafka send + flush, `close()` calls Kafka close       |
| `payment_service/test_consumer.py`          | paid when reserved, skips when insufficient/missing, field preservation |
| `payment_service/test_producer.py`          | `send()` calls Kafka send + flush, `close()` calls Kafka close       |

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
