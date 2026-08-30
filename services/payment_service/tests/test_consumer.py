import json
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from consumer import PaymentConsumer
from db import Base, OutboxEvent, Payment, make_session_factory


def make_session_factory_in_memory():
    # StaticPool keeps one shared connection alive for the whole in-memory
    # DB's lifetime, so multiple PaymentConsumer instances (simulating a
    # restart) can see the same persisted rows within a test.
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


def make_consumer(max_retries=3, retry_backoff_seconds=0.0, session_factory=None):
    with patch("consumer.KafkaConsumer"):
        dlq_producer = MagicMock()
        session_factory = session_factory or make_session_factory_in_memory()
        c = PaymentConsumer(
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
    with session_factory() as session:
        return [row.payload for row in session.query(OutboxEvent).all()]


def payment_row(session_factory, order_id):
    with session_factory() as session:
        return session.get(Payment, order_id)


def test_payment_processed_when_reserved():
    c, _, sf = make_consumer()
    c._process({"order_id": "1", "product": "laptop", "quantity": 2, "inventory_status": "reserved"})
    event = outbox_events(sf)[0]
    assert event["payment_status"] == "paid"
    assert len(outbox_events(sf)) == 1
    assert payment_row(sf, "1").payment_status == "paid"


def test_skips_when_insufficient():
    c, _, sf = make_consumer()
    c._process({"order_id": "2", "product": "phone", "quantity": 10, "inventory_status": "insufficient"})
    assert outbox_events(sf) == []
    assert payment_row(sf, "2").payment_status is None


def test_skips_when_status_missing():
    c, _, sf = make_consumer()
    c._process({"order_id": "3", "product": "keyboard", "quantity": 1})
    assert outbox_events(sf) == []
    assert payment_row(sf, "3").inventory_status == "unknown"


def test_output_event_preserves_upstream_fields():
    c, _, sf = make_consumer()
    c._process({"order_id": "4", "product": "laptop", "quantity": 1, "inventory_status": "reserved"})
    event = outbox_events(sf)[0]
    assert event["order_id"] == "4"
    assert event["inventory_status"] == "reserved"
    assert event["payment_status"] == "paid"


def test_consumer_close():
    c, dlq_producer, _ = make_consumer()
    with patch.object(c, "_consumer") as mock_kafka:
        c.close()
        mock_kafka.close.assert_called_once()
    dlq_producer.close.assert_called_once()


# --- Idempotency -----------------------------------------------------------


def test_duplicate_order_id_is_processed_once():
    c, _, sf = make_consumer()
    event = encode({"order_id": "dup-1", "product": "laptop", "quantity": 2, "inventory_status": "reserved"})

    c._handle_message(event)
    c._handle_message(event)

    assert len(outbox_events(sf)) == 1


def test_different_order_ids_are_both_processed():
    c, _, sf = make_consumer()
    c._handle_message(encode({"order_id": "1", "product": "laptop", "quantity": 1, "inventory_status": "reserved"}))
    c._handle_message(encode({"order_id": "2", "product": "laptop", "quantity": 1, "inventory_status": "reserved"}))

    assert len(outbox_events(sf)) == 2


def test_idempotency_survives_a_restart():
    """The Payment table (business record + idempotency ledger) lives in the
    DB, not process memory, so a brand new PaymentConsumer sharing the same
    database still recognizes a previously processed order_id."""
    sf = make_session_factory_in_memory()
    c1, _, _ = make_consumer(session_factory=sf)
    c1._handle_message(encode({"order_id": "restart-1", "product": "laptop", "quantity": 1, "inventory_status": "reserved"}))

    c2, _, _ = make_consumer(session_factory=sf)  # simulates a fresh process
    c2._handle_message(encode({"order_id": "restart-1", "product": "laptop", "quantity": 1, "inventory_status": "reserved"}))

    assert len(outbox_events(sf)) == 1


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
    event = encode({"order_id": "retry-1", "product": "laptop", "quantity": 1, "inventory_status": "reserved"})

    with patch.object(c, "_process", side_effect=[Exception("boom"), None]) as mock_process:
        c._handle_message(event)

    assert mock_process.call_count == 2
    dlq_producer.send.assert_not_called()


def test_processing_failure_exhausts_retries_then_routes_to_dlq():
    c, dlq_producer, sf = make_consumer(max_retries=3)
    event = encode({"order_id": "poison-1", "product": "laptop", "quantity": 1, "inventory_status": "reserved"})

    with patch.object(c, "_process", side_effect=Exception("always fails")) as mock_process:
        c._handle_message(event)

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
    message.value = encode({"order_id": "1", "product": "laptop", "quantity": 1, "inventory_status": "reserved"})

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
