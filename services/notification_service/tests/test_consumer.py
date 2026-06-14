import logging
from unittest.mock import MagicMock, patch
from consumer import OrderConsumer


def test_consume_logs_message(caplog):
    mock_message = MagicMock()
    mock_message.value = {"order_id": "42", "product": "keyboard", "quantity": 1}

    with patch("consumer.KafkaConsumer") as mock_cls:
        mock_kafka = MagicMock()
        mock_kafka.__iter__ = MagicMock(return_value=iter([mock_message]))
        mock_cls.return_value = mock_kafka

        c = OrderConsumer("localhost:9092")
        with caplog.at_level(logging.INFO):
            c.consume()

        assert "42" in caplog.text
        assert "keyboard" in caplog.text


def test_consumer_close():
    with patch("consumer.KafkaConsumer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        c = OrderConsumer("localhost:9092")
        c.close()
        mock_kafka.close.assert_called_once()
