import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
from consumer import OrderConsumer
from dlq_producer import DLQProducer

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def load_schema(name: str) -> dict:
    return json.loads((CONTRACTS_DIR / name).read_text())


ORDERS_SCHEMA = load_schema("orders.schema.json")
DLQ_ENVELOPE_SCHEMA = load_schema("dlq_envelope.schema.json")


@pytest.mark.parametrize(
    "order",
    [
        {"order_id": "1", "product": "laptop", "quantity": 2},
        {"order_id": "2", "product": "phone", "quantity": 1},
    ],
)
def test_consumer_accepts_any_orders_contract_valid_message(order, caplog):
    jsonschema.validate(instance=order, schema=ORDERS_SCHEMA)

    with patch("consumer.KafkaConsumer"):
        c = OrderConsumer("localhost:9092", dlq_producer=MagicMock(), session_factory=MagicMock())
        with caplog.at_level(logging.INFO):
            c._process(order)

    assert order["order_id"] in caplog.text


def test_dlq_producer_output_conforms_to_dlq_envelope_contract():
    with patch("dlq_producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = DLQProducer("localhost:9092", source_topic="orders", consumer_group="notification-group")
        p.send(b'{"order_id": "1", "product": "laptop", "quantity": "two"}', error="boom")

        published = mock_kafka.send.call_args[0][1]
        jsonschema.validate(instance=published, schema=DLQ_ENVELOPE_SCHEMA)
        assert published["source_topic"] == "orders"
        assert published["consumer_group"] == "notification-group"
