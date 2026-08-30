import json
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from consumer import InventoryConsumer
from db import Base, ProcessedMessage, Stock, make_session_factory

_DEFAULT_STOCK = {"laptop": 10, "phone": 5, "keyboard": 20}


def make_session_factory_in_memory():
    # StaticPool keeps one shared connection alive for the whole in-memory
    # DB's lifetime, so multiple InventoryConsumer instances (simulating a
    # restart) can see the same persisted rows within a test.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_factory = make_session_factory(engine)
    with session_factory() as session:
        for product, quantity in _DEFAULT_STOCK.items():
            session.add(Stock(product=product, quantity=quantity))
        session.commit()
    return session_factory


def make_consumer(max_retries=3, retry_backoff_seconds=0.0, session_factory=None):
    with patch("consumer.KafkaConsumer"):
        dlq_producer = MagicMock()
        session_factory = session_factory or make_session_factory_in_memory()
        c = InventoryConsumer(
            "localhost:9092",
            dlq_producer=dlq_producer,
            session_factory=session_factory,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        return c, dlq_producer, session_factory


def encode(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def outbox_events(session_factory):
    from db import OutboxEvent

    with session_factory() as session:
        return [row.payload for row in session.query(OutboxEvent).all()]


def stock_of(session_factory, product):
    with session_factory() as session:
        return session.get(Stock, product).quantity


def test_reserved_when_stock_sufficient():
    c, _, sf = make_consumer()
    c._process({"order_id": "1", "product": "laptop", "quantity": 2})
    events = outbox_events(sf)
    assert events[-1]["inventory_status"] == "reserved"
    assert stock_of(sf, "laptop") == 8


def test_insufficient_when_stock_low():
    c, _, sf = make_consumer()
    c._process({"order_id": "2", "product": "phone", "quantity": 10})
    events = outbox_events(sf)
    assert events[-1]["inventory_status"] == "insufficient"
    assert stock_of(sf, "phone") == 5


def test_unknown_product_is_insufficient():
    c, _, sf = make_consumer()
    c._process({"order_id": "3", "product": "tablet", "quantity": 1})
    events = outbox_events(sf)
    assert events[-1]["inventory_status"] == "insufficient"


def test_exact_stock_boundary():
    c, _, sf = make_consumer()
    c._process({"order_id": "4", "product": "phone", "quantity": 5})
    events = outbox_events(sf)
    assert events[-1]["inventory_status"] == "reserved"
    assert stock_of(sf, "phone") == 0


def test_output_event_preserves_order_fields():
    c, _, sf = make_consumer()
    c._process({"order_id": "5", "product": "keyboard", "quantity": 1})
    event = outbox_events(sf)[-1]
    assert event["order_id"] == "5"
    assert event["product"] == "keyboard"
    assert event["quantity"] == 1


def test_consumer_close():
    c, dlq_producer, _ = make_consumer()
    with patch.object(c, "_consumer") as mock_kafka:
        c.close()
        mock_kafka.close.assert_called_once()
    dlq_producer.close.assert_called_once()


# --- Idempotency -----------------------------------------------------------


def test_duplicate_order_id_is_processed_once():
    c, _, sf = make_consumer()
    order = encode({"order_id": "dup-1", "product": "laptop", "quantity": 2})

    c._handle_message(order)
    c._handle_message(order)

    assert len(outbox_events(sf)) == 1
    assert stock_of(sf, "laptop") == 8  # decremented only once, not twice


def test_different_order_ids_are_both_processed():
    c, _, sf = make_consumer()
    c._handle_message(encode({"order_id": "1", "product": "laptop", "quantity": 1}))
    c._handle_message(encode({"order_id": "2", "product": "laptop", "quantity": 1}))

    assert len(outbox_events(sf)) == 2
    assert stock_of(sf, "laptop") == 8


def test_idempotency_survives_a_restart():
    """The dedup ledger and stock table live in the DB, not process memory, so
    a brand new InventoryConsumer sharing the same database still recognizes
    a previously processed order_id — proving dedup survives a restart."""
    sf = make_session_factory_in_memory()
    c1, _, _ = make_consumer(session_factory=sf)
    c1._handle_message(encode({"order_id": "restart-1", "product": "laptop", "quantity": 2}))

    c2, _, _ = make_consumer(session_factory=sf)  # simulates a fresh process
    c2._handle_message(encode({"order_id": "restart-1", "product": "laptop", "quantity": 2}))

    assert len(outbox_events(sf)) == 1
    assert stock_of(sf, "laptop") == 8


# --- Retry + DLQ -------------------------------------------------------------


def test_malformed_json_is_routed_to_dlq_without_crashing():
    c, dlq_producer, _ = make_consumer()
    bad_bytes = b"{not valid json"

    c._handle_message(bad_bytes)

    dlq_producer.send.assert_called_once()
    args, kwargs = dlq_producer.send.call_args
    assert args[0] == bad_bytes


def test_processing_failure_retries_then_succeeds():
    c, dlq_producer, sf = make_consumer(max_retries=3)
    order = encode({"order_id": "retry-1", "product": "laptop", "quantity": 1})

    with patch.object(c, "_process", side_effect=[Exception("boom"), None]) as mock_process:
        c._handle_message(order)

    assert mock_process.call_count == 2
    dlq_producer.send.assert_not_called()


def test_processing_failure_exhausts_retries_then_routes_to_dlq():
    c, dlq_producer, sf = make_consumer(max_retries=3)
    order = encode({"order_id": "poison-1", "product": "laptop", "quantity": 1})

    with patch.object(c, "_process", side_effect=Exception("always fails")) as mock_process:
        c._handle_message(order)

    assert mock_process.call_count == 3
    dlq_producer.send.assert_called_once()
    assert not c._is_duplicate("poison-1")


# --- Graceful shutdown -------------------------------------------------------


def test_consume_stops_polling_once_stopped():
    c, _, _ = make_consumer()
    c._consumer.poll.return_value = {}

    c.stop()
    c.consume()  # should return immediately since _running is already False

    c._consumer.poll.assert_not_called()


def test_stop_can_be_called_mid_consume():
    c, _, sf = make_consumer()
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

    assert len(outbox_events(sf)) == 1
    c._consumer.commit.assert_called_once()
