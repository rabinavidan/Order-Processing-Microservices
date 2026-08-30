import json
from unittest.mock import MagicMock, patch

from consumer import InventoryConsumer


def make_consumer(max_retries=3, retry_backoff_seconds=0.0):
    with patch("consumer.KafkaConsumer"):
        producer = MagicMock()
        dlq_producer = MagicMock()
        c = InventoryConsumer(
            "localhost:9092",
            producer=producer,
            dlq_producer=dlq_producer,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        return c, producer, dlq_producer


def encode(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_reserved_when_stock_sufficient():
    c, producer, _ = make_consumer()
    c._process({"order_id": "1", "product": "laptop", "quantity": 2})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "reserved"
    assert c._stock["laptop"] == 8


def test_insufficient_when_stock_low():
    c, producer, _ = make_consumer()
    c._process({"order_id": "2", "product": "phone", "quantity": 10})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "insufficient"
    assert c._stock["phone"] == 5


def test_unknown_product_is_insufficient():
    c, producer, _ = make_consumer()
    c._process({"order_id": "3", "product": "tablet", "quantity": 1})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "insufficient"


def test_exact_stock_boundary():
    c, producer, _ = make_consumer()
    c._process({"order_id": "4", "product": "phone", "quantity": 5})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "reserved"
    assert c._stock["phone"] == 0


def test_output_event_preserves_order_fields():
    c, producer, _ = make_consumer()
    c._process({"order_id": "5", "product": "keyboard", "quantity": 1})
    event = producer.send.call_args[0][0]
    assert event["order_id"] == "5"
    assert event["product"] == "keyboard"
    assert event["quantity"] == 1


def test_consumer_close():
    c, producer, dlq_producer = make_consumer()
    with patch.object(c, "_consumer") as mock_kafka:
        c.close()
        mock_kafka.close.assert_called_once()
    producer.close.assert_called_once()
    dlq_producer.close.assert_called_once()


# --- Idempotency -----------------------------------------------------------


def test_duplicate_order_id_is_processed_once():
    c, producer, _ = make_consumer()
    order = encode({"order_id": "dup-1", "product": "laptop", "quantity": 2})

    c._handle_message(order)
    c._handle_message(order)

    assert producer.send.call_count == 1
    assert c._stock["laptop"] == 8  # decremented only once, not twice


def test_different_order_ids_are_both_processed():
    c, producer, _ = make_consumer()
    c._handle_message(encode({"order_id": "1", "product": "laptop", "quantity": 1}))
    c._handle_message(encode({"order_id": "2", "product": "laptop", "quantity": 1}))

    assert producer.send.call_count == 2
    assert c._stock["laptop"] == 8


# --- Retry + DLQ -------------------------------------------------------------


def test_malformed_json_is_routed_to_dlq_without_crashing():
    c, producer, dlq_producer = make_consumer()
    bad_bytes = b"{not valid json"

    c._handle_message(bad_bytes)

    producer.send.assert_not_called()
    dlq_producer.send.assert_called_once()
    args, kwargs = dlq_producer.send.call_args
    assert args[0] == bad_bytes


def test_processing_failure_retries_then_succeeds():
    c, producer, dlq_producer = make_consumer(max_retries=3)
    producer.send.side_effect = [Exception("transient broker error"), None]
    order = encode({"order_id": "retry-1", "product": "laptop", "quantity": 1})

    c._handle_message(order)

    assert producer.send.call_count == 2
    dlq_producer.send.assert_not_called()
    assert "retry-1" in c._seen_order_ids


def test_processing_failure_exhausts_retries_then_routes_to_dlq():
    c, producer, dlq_producer = make_consumer(max_retries=3)
    producer.send.side_effect = Exception("downstream always down")
    order = encode({"order_id": "poison-1", "product": "laptop", "quantity": 1})

    c._handle_message(order)

    assert producer.send.call_count == 3
    dlq_producer.send.assert_called_once()
    assert "poison-1" not in c._seen_order_ids


# --- Graceful shutdown -------------------------------------------------------


def test_consume_stops_polling_once_stopped():
    c, _, _ = make_consumer()
    c._consumer.poll.return_value = {}

    c.stop()
    c.consume()  # should return immediately since _running is already False

    c._consumer.poll.assert_not_called()


def test_stop_can_be_called_mid_consume():
    c, producer, _ = make_consumer()
    message = MagicMock()
    message.value = encode({"order_id": "1", "product": "laptop", "quantity": 1})

    call_count = {"n": 0}

    def fake_poll(timeout_ms):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {"partition": [message]}
        c.stop()
        return {}

    c._consumer.poll.side_effect = fake_poll
    c.consume()

    assert producer.send.call_count == 1
    c._consumer.commit.assert_called_once()
