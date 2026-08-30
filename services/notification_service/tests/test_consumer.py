import logging
from unittest.mock import MagicMock, patch

import pytest
from consumer import OrderConsumer


def make_consumer():
    with patch("consumer.KafkaConsumer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        return OrderConsumer("localhost:9092"), mock_kafka


def test_process_logs_order_fields(caplog):
    c, _ = make_consumer()
    with caplog.at_level(logging.INFO):
        c._process({"order_id": "42", "product": "keyboard", "quantity": 1})

    assert "42" in caplog.text
    assert "keyboard" in caplog.text
    assert "1" in caplog.text


def test_process_handles_missing_fields_without_raising(caplog):
    c, _ = make_consumer()
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
    c, _ = make_consumer()
    with caplog.at_level(logging.INFO):
        c._process(order)

    assert order["order_id"] in caplog.text
    assert order["product"] in caplog.text


def test_consume_processes_every_message_on_the_topic(caplog):
    c, mock_kafka = make_consumer()
    messages = []
    for order_id, product in [("1", "laptop"), ("2", "phone")]:
        msg = MagicMock()
        msg.value = {"order_id": order_id, "product": product, "quantity": 1}
        messages.append(msg)
    mock_kafka.__iter__ = MagicMock(return_value=iter(messages))

    with caplog.at_level(logging.INFO):
        c.consume()

    assert "1" in caplog.text and "laptop" in caplog.text
    assert "2" in caplog.text and "phone" in caplog.text


def test_consumer_configured_with_correct_topic_and_group():
    with patch("consumer.KafkaConsumer") as mock_cls:
        OrderConsumer("localhost:9092")
        args, kwargs = mock_cls.call_args
        assert args[0] == "orders"
        assert kwargs["group_id"] == "notification-group"
        assert kwargs["bootstrap_servers"] == "localhost:9092"


def test_consumer_close():
    c, mock_kafka = make_consumer()
    c.close()
    mock_kafka.close.assert_called_once()
