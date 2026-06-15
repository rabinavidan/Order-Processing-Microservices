from unittest.mock import MagicMock, patch
from producer import InventoryProducer


def test_send_calls_kafka_send_and_flush():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = InventoryProducer("localhost:9092")
        event = {"order_id": "1", "inventory_status": "reserved"}
        p.send(event)
        mock_kafka.send.assert_called_once_with("inventory.reserved", event)
        mock_kafka.flush.assert_called_once()


def test_producer_close():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = InventoryProducer("localhost:9092")
        p.close()
        mock_kafka.close.assert_called_once()