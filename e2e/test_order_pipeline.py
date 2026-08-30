"""
One true end-to-end test for the order-processing saga.

Unlike the per-service unit tests (which mock kafka-python and use an
in-memory SQLite DB), this spins up a real Kafka broker AND a real Postgres
instance via testcontainers, and drives the actual, unmodified
producer/consumer/outbox classes from all 4 services against them — no
mocks. It proves the full chain works over the wire and through real
persistence, not just that each service's logic is individually correct in
isolation:

    order_service --(orders)--> inventory_service --(inventory.reserved)--> payment_service --(payments.processed)
                    \\-(orders)--> notification_service (logs)

Each consumer's `_process` method and each service's `OutboxRelay.publish_pending()`
are called explicitly (rather than run as free-running background
threads/loops) so the test stays deterministic: it polls the real broker for
the next message, feeds it to the same `_process` method the unit tests
exercise with mocks, then drains that service's outbox exactly once. This is
standard practice for testing Kafka consumers without relying on a
background loop that would need machinery to stop mid-test.

The resilience features added in M2 (idempotent dedup, retry+backoff, DLQ
routing, manual offset commit) and the persistence/outbox features added in
M3 (Postgres-backed state, Alembic migrations, the Outbox pattern) each have
dedicated unit tests per service — this test stays focused on proving the
wiring between all 4 services, and their databases, is correct end to end
over a real broker and a real database.
"""

import json
import time

import pytest
from kafka import KafkaConsumer
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

from conftest import load_service_modules

POLL_TIMEOUT_S = 30


def poll_one(kafka_consumer: KafkaConsumer, timeout_s: float = POLL_TIMEOUT_S) -> dict:
    """Block until exactly one raw message is available on a real KafkaConsumer, decode it as JSON, or time out."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        records = kafka_consumer.poll(timeout_ms=1000, max_records=1)
        for _, messages in records.items():
            if messages:
                return json.loads(messages[0].value.decode("utf-8"))
    raise TimeoutError(f"No message received within {timeout_s}s")


def database_url_for(base_connection_url: str, dbname: str) -> str:
    prefix = base_connection_url.rsplit("/", 1)[0]
    return f"{prefix}/{dbname}"


def create_databases(base_connection_url: str, names) -> None:
    import psycopg2

    dsn = base_connection_url.replace("postgresql+psycopg2://", "postgresql://")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for name in names:
                cur.execute(f"CREATE DATABASE {name}")
    finally:
        conn.close()


@pytest.fixture(scope="module")
def kafka_bootstrap_server():
    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture(scope="module")
def postgres_base_url():
    with PostgresContainer("postgres:16-alpine") as pg:
        base_url = pg.get_connection_url()
        create_databases(base_url, ["order_db", "inventory_db", "payment_db", "notification_db"])
        yield base_url


def test_full_order_pipeline_via_real_kafka_and_postgres(kafka_bootstrap_server, postgres_base_url):
    order_db_url = database_url_for(postgres_base_url, "order_db")
    inventory_db_url = database_url_for(postgres_base_url, "inventory_db")
    payment_db_url = database_url_for(postgres_base_url, "payment_db")
    notification_db_url = database_url_for(postgres_base_url, "notification_db")

    order_mods = load_service_modules("order_service", "db", "producer", "outbox_relay", "main")
    order_mods["main"].run_migrations(order_db_url)
    order_session_factory = order_mods["db"].make_session_factory(order_mods["db"].make_engine(order_db_url))
    order_producer = order_mods["producer"].OrderProducer(kafka_bootstrap_server)
    order_relay = order_mods["outbox_relay"].OutboxRelay(order_session_factory, producer=order_producer)

    inventory_mods = load_service_modules(
        "inventory_service", "db", "producer", "consumer", "dlq_producer", "outbox_relay", "main"
    )
    inventory_mods["main"].run_migrations(inventory_db_url)
    inventory_session_factory = inventory_mods["db"].make_session_factory(
        inventory_mods["db"].make_engine(inventory_db_url)
    )
    inventory_producer = inventory_mods["producer"].InventoryProducer(kafka_bootstrap_server)
    inventory_relay = inventory_mods["outbox_relay"].OutboxRelay(inventory_session_factory, producer=inventory_producer)
    inventory_dlq = inventory_mods["dlq_producer"].DLQProducer(
        kafka_bootstrap_server, source_topic="orders", consumer_group="inventory-group"
    )
    inventory_consumer = inventory_mods["consumer"].InventoryConsumer(
        kafka_bootstrap_server, dlq_producer=inventory_dlq, session_factory=inventory_session_factory
    )

    payment_mods = load_service_modules(
        "payment_service", "db", "producer", "consumer", "dlq_producer", "outbox_relay", "main"
    )
    payment_mods["main"].run_migrations(payment_db_url)
    payment_session_factory = payment_mods["db"].make_session_factory(payment_mods["db"].make_engine(payment_db_url))
    payment_producer = payment_mods["producer"].PaymentProducer(kafka_bootstrap_server)
    payment_relay = payment_mods["outbox_relay"].OutboxRelay(payment_session_factory, producer=payment_producer)
    payment_dlq = payment_mods["dlq_producer"].DLQProducer(
        kafka_bootstrap_server, source_topic="inventory.reserved", consumer_group="payment-group"
    )
    payment_consumer = payment_mods["consumer"].PaymentConsumer(
        kafka_bootstrap_server, dlq_producer=payment_dlq, session_factory=payment_session_factory
    )

    notification_mods = load_service_modules("notification_service", "db", "consumer", "dlq_producer", "main")
    notification_mods["main"].run_migrations(notification_db_url)
    notification_session_factory = notification_mods["db"].make_session_factory(
        notification_mods["db"].make_engine(notification_db_url)
    )
    notification_dlq = notification_mods["dlq_producer"].DLQProducer(
        kafka_bootstrap_server, source_topic="orders", consumer_group="notification-group"
    )
    notification_consumer = notification_mods["consumer"].OrderConsumer(
        kafka_bootstrap_server, dlq_producer=notification_dlq, session_factory=notification_session_factory
    )

    final_consumer = KafkaConsumer(
        "payments.processed",
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset="earliest",
        group_id="e2e-assertions",
    )

    try:
        order = {"order_id": "e2e-1", "product": "laptop", "quantity": 2}

        # 1. order_service: durably write the order + an outbox event in one
        #    transaction (what POST /orders does), then its relay publishes it.
        with order_session_factory() as session:
            session.add(order_mods["db"].OrderRecord(order_id=order["order_id"], product=order["product"], quantity=order["quantity"]))
            session.add(order_mods["db"].OutboxEvent(topic="orders", payload=order))
            session.commit()
        assert order_relay.publish_pending() == 1

        # 2. inventory_service consumes it, reserves stock (persisted in
        #    Postgres), and its relay publishes to "inventory.reserved".
        received_order = poll_one(inventory_consumer._consumer)
        assert received_order == order
        inventory_consumer._process(received_order)
        assert inventory_relay.publish_pending() == 1

        with inventory_session_factory() as session:
            stock = session.get(inventory_mods["db"].Stock, "laptop")
            assert stock.quantity == 8  # 10 seeded - 2 reserved, durably in Postgres

        # 3. notification_service independently receives its own copy of
        #    the same order (separate consumer group -> independent delivery).
        notified_order = poll_one(notification_consumer._consumer)
        assert notified_order == order
        notification_consumer._process(notified_order)

        # 4. payment_service consumes the reservation, records the payment
        #    (persisted in Postgres), and its relay publishes to "payments.processed".
        reservation = poll_one(payment_consumer._consumer)
        assert reservation["order_id"] == "e2e-1"
        assert reservation["inventory_status"] == "reserved"
        payment_consumer._process(reservation)
        assert payment_relay.publish_pending() == 1

        with payment_session_factory() as session:
            payment = session.get(payment_mods["db"].Payment, "e2e-1")
            assert payment.payment_status == "paid"

        # 5. assert the terminal event landed on "payments.processed".
        payment_event = poll_one(final_consumer)
        assert payment_event == {
            "order_id": "e2e-1",
            "product": "laptop",
            "quantity": 2,
            "inventory_status": "reserved",
            "payment_status": "paid",
        }
    finally:
        final_consumer.close()
        order_producer.close()
        inventory_consumer.close()
        payment_consumer.close()
        notification_consumer.close()
