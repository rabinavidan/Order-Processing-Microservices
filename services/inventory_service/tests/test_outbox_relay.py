from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from db import Base, OutboxEvent, make_session_factory
from outbox_relay import OutboxRelay


def make_session_factory_in_memory():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


def add_event(session_factory, topic="inventory.reserved", payload=None):
    with session_factory() as session:
        row = OutboxEvent(topic=topic, payload=payload or {"order_id": "1"})
        session.add(row)
        session.commit()
        return row.id


def test_publish_pending_sends_and_marks_published():
    sf = make_session_factory_in_memory()
    add_event(sf, payload={"order_id": "1"})
    producer = MagicMock()
    relay = OutboxRelay(sf, producer=producer)

    published = relay.publish_pending()

    assert published == 1
    producer.send.assert_called_once_with({"order_id": "1"})
    with sf() as session:
        row = session.query(OutboxEvent).one()
        assert row.published_at is not None


def test_publish_pending_skips_already_published_rows():
    sf = make_session_factory_in_memory()
    add_event(sf)
    producer = MagicMock()
    relay = OutboxRelay(sf, producer=producer)

    relay.publish_pending()
    published_again = relay.publish_pending()

    assert published_again == 0
    producer.send.assert_called_once()


def test_publish_pending_leaves_row_unpublished_on_producer_failure():
    sf = make_session_factory_in_memory()
    add_event(sf, payload={"order_id": "poison"})
    producer = MagicMock()
    producer.send.side_effect = Exception("broker unreachable")
    relay = OutboxRelay(sf, producer=producer)

    published = relay.publish_pending()

    assert published == 0
    with sf() as session:
        row = session.query(OutboxEvent).one()
        assert row.published_at is None


def test_publish_pending_retries_a_previously_failed_row_once_producer_recovers():
    sf = make_session_factory_in_memory()
    add_event(sf, payload={"order_id": "1"})
    producer = MagicMock()
    producer.send.side_effect = [Exception("broker unreachable"), None]
    relay = OutboxRelay(sf, producer=producer)

    assert relay.publish_pending() == 0
    assert relay.publish_pending() == 1


def test_publish_pending_processes_multiple_pending_rows_in_order():
    sf = make_session_factory_in_memory()
    add_event(sf, payload={"order_id": "1"})
    add_event(sf, payload={"order_id": "2"})
    producer = MagicMock()
    relay = OutboxRelay(sf, producer=producer)

    published = relay.publish_pending()

    assert published == 2
    sent_ids = [call.args[0]["order_id"] for call in producer.send.call_args_list]
    assert sent_ids == ["1", "2"]


def test_stop_before_run_forever_returns_immediately():
    sf = make_session_factory_in_memory()
    producer = MagicMock()
    relay = OutboxRelay(sf, producer=producer, poll_interval_seconds=0.0)

    relay.stop()
    relay.run_forever()  # should return immediately, not loop forever

    producer.send.assert_not_called()
