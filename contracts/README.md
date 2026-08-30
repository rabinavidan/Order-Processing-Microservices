# Kafka topic contracts

JSON Schema definitions for every message shape that crosses a Kafka topic in this system. Each service's test suite validates against these schemas so a producer and its downstream consumer(s) can never silently drift apart:

| Schema file                          | Topic                 | Producer            | Consumer(s)                             |
|---------------------------------------|------------------------|----------------------|-------------------------------------------|
| `orders.schema.json`                  | `orders`               | `order_service`      | `notification_service`, `inventory_service` |
| `inventory_reserved.schema.json`      | `inventory.reserved`   | `inventory_service`  | `payment_service`                         |
| `payments_processed.schema.json`      | `payments.processed`   | `payment_service`    | —                                          |

Each service's `tests/test_contract.py`:

- asserts its **producer** output validates against the schema for the topic it publishes to, and
- asserts its **consumer** correctly handles a schema-valid message from the topic it subscribes to.

If a service changes its message shape without updating the schema (or vice versa), the contract test for every service that touches that topic fails — this is what catches a breaking change before it reaches a downstream service at runtime.
