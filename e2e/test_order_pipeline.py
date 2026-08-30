"""
One true end-to-end test for the order-processing saga.

Unlike the per-service unit tests (which mock kafka-python), this spins up a
real Kafka broker via testcontainers and drives the actual, unmodified
producer/consumer classes from all 4 services against it — no mocks. It
proves the full chain works over the wire, not just that each service's
logic is individually correct in isolation:

    order_service --(orders)--> inventory_service --(inventory.reserved)--> payment_service --(payments.processed)
                    \\-(orders)--> notification_service (logs)

Each consumer's `_process`/`consume` method is called explicitly (rather than
run as a free-running background thread) so the test stays deterministic: it
polls the real broker for the next message, then feeds it to the same
`_process` method the unit tests exercise with mocks. This is standard
practice for testing Kafka consumers without relying on a background loop
that would need machinery to stop mid-test.

The resilience features added in M2 (idempotent dedup, retry+backoff, DLQ
routing, manual offset commit) live in each consumer's `_handle_message`
wrapper and are covered by dedicated unit tests per service — this test
stays focused on proving the wiring between all 4 services is correct end
to end over a real broker.
"""

import json
import time

import pytest
from kafka import KafkaConsumer
from testcontainers.kafka import KafkaContainer

from conftest import load_module

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


@pytest.fixture(scope="module")
def kafka_bootstrap_server():
    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka:
        yield kafka.get_bootstrap_server()


def test_full_order_pipeline_via_real_kafka(kafka_bootstrap_server):
    order_producer_mod = load_module("e2e_order_producer", "order_service", "producer.py")
    inventory_consumer_mod = load_module("e2e_inventory_consumer", "inventory_service", "consumer.py")
    inventory_producer_mod = load_module("e2e_inventory_producer", "inventory_service", "producer.py")
    inventory_dlq_mod = load_module("e2e_inventory_dlq", "inventory_service", "dlq_producer.py")
    payment_consumer_mod = load_module("e2e_payment_consumer", "payment_service", "consumer.py")
    payment_producer_mod = load_module("e2e_payment_producer", "payment_service", "producer.py")
    payment_dlq_mod = load_module("e2e_payment_dlq", "payment_service", "dlq_producer.py")
    notification_consumer_mod = load_module("e2e_notification_consumer", "notification_service", "consumer.py")
    notification_dlq_mod = load_module("e2e_notification_dlq", "notification_service", "dlq_producer.py")

    order_producer = order_producer_mod.OrderProducer(kafka_bootstrap_server)

    inventory_producer = inventory_producer_mod.InventoryProducer(kafka_bootstrap_server)
    inventory_dlq = inventory_dlq_mod.DLQProducer(kafka_bootstrap_server, source_topic="orders", consumer_group="inventory-group")
    inventory_consumer = inventory_consumer_mod.InventoryConsumer(
        kafka_bootstrap_server, producer=inventory_producer, dlq_producer=inventory_dlq
    )

    payment_producer = payment_producer_mod.PaymentProducer(kafka_bootstrap_server)
    payment_dlq = payment_dlq_mod.DLQProducer(kafka_bootstrap_server, source_topic="inventory.reserved", consumer_group="payment-group")
    payment_consumer = payment_consumer_mod.PaymentConsumer(
        kafka_bootstrap_server, producer=payment_producer, dlq_producer=payment_dlq
    )

    notification_dlq = notification_dlq_mod.DLQProducer(kafka_bootstrap_server, source_topic="orders", consumer_group="notification-group")
    notification_consumer = notification_consumer_mod.OrderConsumer(kafka_bootstrap_server, dlq_producer=notification_dlq)

    final_consumer = KafkaConsumer(
        "payments.processed",
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset="earliest",
        group_id="e2e-assertions",
    )

    try:
        order = {"order_id": "e2e-1", "product": "laptop", "quantity": 2}

        # 1. order_service publishes the order to the "orders" topic.
        order_producer.send_order(order)

        # 2. inventory_service consumes it, reserves stock, and publishes
        #    to "inventory.reserved".
        received_order = poll_one(inventory_consumer._consumer)
        assert received_order == order
        inventory_consumer._process(received_order)

        # 3. notification_service independently receives its own copy of
        #    the same order (separate consumer group -> independent delivery).
        notified_order = poll_one(notification_consumer._consumer)
        assert notified_order == order

        # 4. payment_service consumes the reservation and processes payment,
        #    publishing to "payments.processed".
        reservation = poll_one(payment_consumer._consumer)
        assert reservation["order_id"] == "e2e-1"
        assert reservation["inventory_status"] == "reserved"
        payment_consumer._process(reservation)

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
