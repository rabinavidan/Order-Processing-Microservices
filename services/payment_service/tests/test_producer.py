from unittest.mock import MagicMock, patch
from producer import PaymentProducer


def test_send_calls_kafka_send_and_flush():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = PaymentProducer("localhost:9092")
        event = {"order_id": "1", "payment_status": "paid"}
        p.send(event)
        mock_kafka.send.assert_called_once_with("payments.processed", event)
        mock_kafka.flush.assert_called_once()


def test_producer_close():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = PaymentProducer("localhost:9092")
        p.close()
        mock_kafka.close.assert_called_once()