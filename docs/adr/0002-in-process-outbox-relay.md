# ADR 0002: In-process, poll-based Outbox relay

**Status:** Accepted

## Context

`order_service`, `inventory_service`, and `payment_service` each need to change durable state (write an order; reserve stock; record a payment) *and* publish a Kafka event about that change — and have the two never disagree. Before M3, they did this by calling `producer.send(...)` synchronously in the same code path as the state change. That's a real correctness gap: if the process crashes (or Kafka is unreachable) between the state change and the publish, the two drift apart with no way to reconcile them.

The [Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) fixes this by writing the event as a row in the *same* database transaction as the state change (see `OutboxEvent` in each service's `db.py`), then publishing it separately. The question this ADR answers is *how* that separate publish happens.

## Decision

A relay object per service (`outbox_relay.py`), run as a **daemon thread inside the same process**, polling its own `outbox_events` table on an interval (default 0.5s) and publishing unpublished rows via that service's existing Kafka producer:

```
main.py:
    relay = OutboxRelay(session_factory, producer=downstream_producer)
    threading.Thread(target=relay.run_forever, daemon=True).start()
```

`publish_pending()` is the unit of work: query up to a batch of unpublished rows, `producer.send()` each, mark `published_at` and commit — one row at a time, so one poison row doesn't block the rest of the batch and a producer failure just leaves that row for the next poll. Graceful shutdown (`stop()`) is wired into the same `SIGTERM`/`SIGINT` handlers already used for each consumer (see README ["Resilience"](../../README.md#resilience)).

## Alternatives considered

- **Change Data Capture (Debezium + Kafka Connect)** reading the outbox table's WAL and publishing automatically. This is the standard production answer and avoids polling entirely, but it means running and operating a Kafka Connect cluster plus per-table connector config — a lot of new infrastructure for a project at this scale, and it would obscure the Outbox pattern itself behind a framework instead of demonstrating it directly.
- **A separate relay microservice.** Cleaner separation of concerns (a crash in relay logic can't take down the consumer), but a 5th deployable for 3 outbox tables is disproportionate here, and the relay's failure mode (an unpublished row just waits for the next poll) doesn't actually need process isolation to be safe.

## Consequences

- **At-least-once publishing.** If `producer.send()` succeeds at the broker but the relay crashes (or the ack times out) before `published_at` commits, the row is retried on the next poll and gets published again. This is safe *because* every downstream consumer in this system is already idempotent (see README ["Resilience"](../../README.md#resilience)) — the same idempotency work from M2 is what makes this relay's simplicity acceptable instead of a latent bug.
- **Publish latency is bounded by the poll interval**, not synchronous with the request/message that caused the state change. For `order_service` this means `POST /orders` returns before the `orders` event is necessarily on the topic yet — a deliberate trade (durability first, publish shortly after) rather than the reverse.
- **Not horizontally safe as written.** Two instances of the same service polling the same outbox table would each try to publish the same pending rows (only mitigated by producers being idempotent downstream, not by any locking here). Running more than one replica per service would need `SELECT ... FOR UPDATE SKIP LOCKED` or equivalent — out of scope while each service runs as a single instance.
