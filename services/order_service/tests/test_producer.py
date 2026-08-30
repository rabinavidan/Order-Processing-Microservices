from unittest.mock import MagicMock, patch
from producer import OrderProducer


def test_send_calls_kafka_send():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = OrderProducer("localhost:9092")
        order = {"order_id": "1", "product": "book", "quantity": 3}
        p.send(order)
        mock_kafka.send.assert_called_once_with("orders", order)
        mock_kafka.flush.assert_called_once()


def test_producer_close_calls_kafka_close():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = OrderProducer("localhost:9092")
        p.close()
        mock_kafka.close.assert_called_once()
