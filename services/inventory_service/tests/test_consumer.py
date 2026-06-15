from unittest.mock import MagicMock, patch
from consumer import InventoryConsumer


def make_consumer():
    with patch("consumer.KafkaConsumer"):
        producer = MagicMock()
        return InventoryConsumer("localhost:9092", producer=producer), producer


def test_reserved_when_stock_sufficient():
    c, producer = make_consumer()
    c._process({"order_id": "1", "product": "laptop", "quantity": 2})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "reserved"
    assert c._stock["laptop"] == 8


def test_insufficient_when_stock_low():
    c, producer = make_consumer()
    c._process({"order_id": "2", "product": "phone", "quantity": 10})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "insufficient"
    assert c._stock["phone"] == 5


def test_unknown_product_is_insufficient():
    c, producer = make_consumer()
    c._process({"order_id": "3", "product": "tablet", "quantity": 1})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "insufficient"


def test_exact_stock_boundary():
    c, producer = make_consumer()
    c._process({"order_id": "4", "product": "phone", "quantity": 5})
    event = producer.send.call_args[0][0]
    assert event["inventory_status"] == "reserved"
    assert c._stock["phone"] == 0


def test_output_event_preserves_order_fields():
    c, producer = make_consumer()
    c._process({"order_id": "5", "product": "keyboard", "quantity": 1})
    event = producer.send.call_args[0][0]
    assert event["order_id"] == "5"
    assert event["product"] == "keyboard"
    assert event["quantity"] == 1


def test_consumer_close():
    c, producer = make_consumer()
    with patch.object(c, "_consumer") as mock_kafka:
        c.close()
        mock_kafka.close.assert_called_once()
    producer.close.assert_called_once()