import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
from consumer import PaymentConsumer
from dlq_producer import DLQProducer
from producer import PaymentProducer

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def load_schema(name: str) -> dict:
    return json.loads((CONTRACTS_DIR / name).read_text())


INVENTORY_RESERVED_SCHEMA = load_schema("inventory_reserved.schema.json")
PAYMENTS_PROCESSED_SCHEMA = load_schema("payments_processed.schema.json")
DLQ_ENVELOPE_SCHEMA = load_schema("dlq_envelope.schema.json")


def make_consumer():
    with patch("consumer.KafkaConsumer"):
        producer = MagicMock()
        dlq_producer = MagicMock()
        return PaymentConsumer("localhost:9092", producer=producer, dlq_producer=dlq_producer), producer


def test_consumer_accepts_reserved_inventory_contract_message_and_publishes_valid_payment():
    event = {"order_id": "1", "product": "laptop", "quantity": 2, "inventory_status": "reserved"}
    jsonschema.validate(instance=event, schema=INVENTORY_RESERVED_SCHEMA)

    c, producer = make_consumer()
    c._process(event)

    published = producer.send.call_args[0][0]
    jsonschema.validate(instance=published, schema=PAYMENTS_PROCESSED_SCHEMA)


def test_consumer_skips_insufficient_inventory_contract_message_without_publishing():
    event = {"order_id": "2", "product": "phone", "quantity": 10, "inventory_status": "insufficient"}
    jsonschema.validate(instance=event, schema=INVENTORY_RESERVED_SCHEMA)

    c, producer = make_consumer()
    c._process(event)

    producer.send.assert_not_called()


def test_producer_output_conforms_to_payments_processed_contract():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = PaymentProducer("localhost:9092")
        event = {
            "order_id": "1",
            "product": "laptop",
            "quantity": 2,
            "inventory_status": "reserved",
            "payment_status": "paid",
        }
        p.send(event)

        published = mock_kafka.send.call_args[0][1]
        jsonschema.validate(instance=published, schema=PAYMENTS_PROCESSED_SCHEMA)


def test_dlq_producer_output_conforms_to_dlq_envelope_contract():
    with patch("dlq_producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = DLQProducer("localhost:9092", source_topic="inventory.reserved", consumer_group="payment-group")
        p.send(b'{"order_id": "1", "inventory_status": "reserved"}', error="boom")

        published = mock_kafka.send.call_args[0][1]
        jsonschema.validate(instance=published, schema=DLQ_ENVELOPE_SCHEMA)
        assert published["source_topic"] == "inventory.reserved"
        assert published["consumer_group"] == "payment-group"
