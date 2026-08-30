import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from consumer import OrderConsumer
from db import Base, make_session_factory


def make_session_factory_in_memory():
    # StaticPool keeps one shared connection alive for the whole in-memory
    # DB's lifetime, so multiple OrderConsumer instances (simulating a
    # restart) can see the same persisted rows within a test.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


def make_consumer(max_retries=3, retry_backoff_seconds=0.0, session_factory=None):
    with patch("consumer.KafkaConsumer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        dlq_producer = MagicMock()
        c = OrderConsumer(
            "localhost:9092",
            dlq_producer=dlq_producer,
            session_factory=session_factory or make_session_factory_in_memory(),
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        return c, mock_kafka, dlq_producer


def encode(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_process_logs_order_fields(caplog):
    c, _, _ = make_consumer()
    with caplog.at_level(logging.INFO):
        c._process({"order_id": "42", "product": "keyboard", "quantity": 1})

    assert "42" in caplog.text
    assert "keyboard" in caplog.text
    assert "1" in caplog.text


def test_process_handles_missing_fields_without_raising(caplog):
    c, _, _ = make_consumer()
    with caplog.at_level(logging.INFO):
        c._process({"order_id": "7"})

    assert "7" in caplog.text


@pytest.mark.parametrize(
    "order",
    [
        {"order_id": "1", "product": "laptop", "quantity": 2},
        {"order_id": "2", "product": "phone", "quantity": 5},
        {"order_id": "3", "product": "keyboard", "quantity": 10},
    ],
)
def test_process_logs_each_distinct_order(order, caplog):
    c, _, _ = make_consumer()
    with caplog.at_level(logging.INFO):
        c._process(order)

    assert order["order_id"] in caplog.text
    assert order["product"] in caplog.text


def test_consume_processes_every_message_on_the_topic(caplog):
    c, mock_kafka, _ = make_consumer()
    messages = []
    for order_id, product in [("1", "laptop"), ("2", "phone")]:
        msg = MagicMock()
        msg.value = encode({"order_id": order_id, "product": product, "quantity": 1})
        messages.append(msg)

    call_count = {"n": 0}

    def fake_poll(timeout_ms):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {"partition": messages}
        c.stop()
        return {}

    mock_kafka.poll.side_effect = fake_poll

    with caplog.at_level(logging.INFO):
        c.consume()

    assert "1" in caplog.text and "laptop" in caplog.text
    assert "2" in caplog.text and "phone" in caplog.text


def test_consumer_configured_with_correct_topic_and_group():
    with patch("consumer.KafkaConsumer") as mock_cls:
        OrderConsumer("localhost:9092", dlq_producer=MagicMock(), session_factory=make_session_factory_in_memory())
        args, kwargs = mock_cls.call_args
        assert args[0] == "orders"
        assert kwargs["group_id"] == "notification-group"
        assert kwargs["bootstrap_servers"] == "localhost:9092"


def test_consumer_close():
    c, mock_kafka, dlq_producer = make_consumer()
    c.close()
    mock_kafka.close.assert_called_once()
    dlq_producer.close.assert_called_once()


# --- Idempotency -----------------------------------------------------------


def test_duplicate_order_id_is_processed_once(caplog):
    c, _, _ = make_consumer()
    order = encode({"order_id": "dup-1", "product": "laptop", "quantity": 2})

    with caplog.at_level(logging.INFO):
        c._handle_message(order)
        caplog.clear()
        c._handle_message(order)

    # second call short-circuits on the dedup check, never re-running _process
    assert "Order received" not in caplog.text
    assert "Duplicate order_id=dup-1" in caplog.text


def test_different_order_ids_are_both_processed(caplog):
    c, _, _ = make_consumer()
    with caplog.at_level(logging.INFO):
        c._handle_message(encode({"order_id": "1", "product": "laptop", "quantity": 1}))
        c._handle_message(encode({"order_id": "2", "product": "laptop", "quantity": 1}))

    assert "1" in caplog.text
    assert "2" in caplog.text


def test_idempotency_survives_a_restart(caplog):
    """The dedup ledger is a DB table, not an in-memory set, so a brand new
    OrderConsumer instance pointed at the same database still recognizes a
    previously processed order_id — proving dedup survives a process restart."""
    session_factory = make_session_factory_in_memory()
    c1, _, _ = make_consumer(session_factory=session_factory)
    c1._handle_message(encode({"order_id": "restart-1", "product": "laptop", "quantity": 1}))

    c2, _, _ = make_consumer(session_factory=session_factory)  # simulates a fresh process
    with caplog.at_level(logging.INFO):
        c2._handle_message(encode({"order_id": "restart-1", "product": "laptop", "quantity": 1}))

    assert "Duplicate order_id=restart-1" in caplog.text


# --- Retry + DLQ -------------------------------------------------------------


def test_malformed_json_is_routed_to_dlq_without_crashing():
    c, _, dlq_producer = make_consumer()
    bad_bytes = b"{not valid json"

    c._handle_message(bad_bytes)

    dlq_producer.send.assert_called_once()
    args, kwargs = dlq_producer.send.call_args
    assert args[0] == bad_bytes


def test_processing_failure_retries_then_succeeds():
    c, _, dlq_producer = make_consumer(max_retries=3)
    order = encode({"order_id": "retry-1", "product": "laptop", "quantity": 1})

    with patch.object(c, "_process", side_effect=[Exception("boom"), None]) as mock_process:
        c._handle_message(order)

    assert mock_process.call_count == 2
    dlq_producer.send.assert_not_called()
    assert c._is_duplicate("retry-1")


def test_processing_failure_exhausts_retries_then_routes_to_dlq():
    c, _, dlq_producer = make_consumer(max_retries=3)
    order = encode({"order_id": "poison-1", "product": "laptop", "quantity": 1})

    with patch.object(c, "_process", side_effect=Exception("always fails")) as mock_process:
        c._handle_message(order)

    assert mock_process.call_count == 3
    dlq_producer.send.assert_called_once()
    assert not c._is_duplicate("poison-1")


# --- Graceful shutdown -------------------------------------------------------


def test_consume_stops_polling_once_stopped():
    c, mock_kafka, _ = make_consumer()
    mock_kafka.poll.return_value = {}

    c.stop()
    c.consume()  # should return immediately since _running is already False

    mock_kafka.poll.assert_not_called()
