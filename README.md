# Order Processing — Microservices

A mini microservices project demonstrating **FastAPI**, **Apache Kafka**, **Docker**, and **pytest** using an order processing domain.

---

## Architecture

```
[HTTP Client]
      │
      │ POST /orders
      ▼
[Order Service]  ──── Kafka topic: orders ────▶  [Notification Service]
  FastAPI :8000                                      kafka-python consumer
      │
      ▼
  GET /health
```

| Service              | Role                                          | Tech              |
|----------------------|-----------------------------------------------|-------------------|
| Order Service        | REST API — validates and publishes orders     | FastAPI, Uvicorn  |
| Notification Service | Consumes order events, logs notifications     | kafka-python      |
| Kafka (KRaft)        | Message broker, topic `orders`               | Confluent 7.6.1   |

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
│   └── notification_service/
│       ├── main.py                 # Consumer entry point
│       ├── consumer.py             # KafkaConsumer wrapper
│       ├── requirements.txt
│       ├── Dockerfile
│       └── tests/
│           └── test_consumer.py    # Consumer unit tests
└── examples/                       # Python learning examples
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

Check notification service logs in the docker-compose output — it will print the consumed event.

### Health check

```bash
curl http://localhost:8000/health
```

### Stop

```bash
docker-compose down
```

---

## Running Tests Locally

Tests use mocked Kafka — no Docker required.

**Order Service**
```bash
cd services/order_service
pip install -r requirements.txt httpx
pytest
```

**Notification Service**
```bash
cd services/notification_service
pip install -r requirements.txt
pytest
```

### Test coverage

| File                  | Tests                                              |
|-----------------------|----------------------------------------------------|
| `test_api.py`         | POST /orders → 201, invalid payload → 422, GET /health → 200 |
| `test_producer.py`    | `send_order()` calls Kafka send + flush, `close()` calls Kafka close |
| `test_consumer.py`    | `consume()` logs order data, `close()` calls Kafka close |

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

| Variable                 | Default          | Description                  |
|--------------------------|------------------|------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`| `localhost:9092` | Kafka broker address         |

---

## System Design

Open `architecture.html` in a browser for an interactive visual diagram of the full system.
