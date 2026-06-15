from unittest.mock import MagicMock, patch
from consumer import PaymentConsumer


def make_consumer():
    with patch("consumer.KafkaConsumer"):
        producer = MagicMock()
        return PaymentConsumer("localhost:9092", producer=producer), producer


def test_payment_processed_when_reserved():
    c, producer = make_consumer()
    c._process({"order_id": "1", "product": "laptop", "quantity": 2, "inventory_status": "reserved"})
    event = producer.send.call_args[0][0]
    assert event["payment_status"] == "paid"
    producer.send.assert_called_once()


def test_skips_when_insufficient():
    c, producer = make_consumer()
    c._process({"order_id": "2", "product": "phone", "quantity": 10, "inventory_status": "insufficient"})
    producer.send.assert_not_called()


def test_skips_when_status_missing():
    c, producer = make_consumer()
    c._process({"order_id": "3", "product": "keyboard", "quantity": 1})
    producer.send.assert_not_called()


def test_output_event_preserves_upstream_fields():
    c, producer = make_consumer()
    c._process({"order_id": "4", "product": "laptop", "quantity": 1, "inventory_status": "reserved"})
    event = producer.send.call_args[0][0]
    assert event["order_id"] == "4"
    assert event["inventory_status"] == "reserved"
    assert event["payment_status"] == "paid"


def test_consumer_close():
    c, producer = make_consumer()
    with patch.object(c, "_consumer") as mock_kafka:
        c.close()
        mock_kafka.close.assert_called_once()
    producer.close.assert_called_once()