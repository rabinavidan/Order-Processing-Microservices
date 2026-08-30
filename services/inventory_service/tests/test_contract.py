import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from consumer import InventoryConsumer
from db import Base, OutboxEvent, Stock, make_session_factory
from dlq_producer import DLQProducer
from producer import InventoryProducer

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"

_DEFAULT_STOCK = {"laptop": 10, "phone": 5, "keyboard": 20}


def load_schema(name: str) -> dict:
    return json.loads((CONTRACTS_DIR / name).read_text())


ORDERS_SCHEMA = load_schema("orders.schema.json")
INVENTORY_RESERVED_SCHEMA = load_schema("inventory_reserved.schema.json")
DLQ_ENVELOPE_SCHEMA = load_schema("dlq_envelope.schema.json")


def make_consumer():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    session_factory = make_session_factory(engine)
    with session_factory() as session:
        for product, quantity in _DEFAULT_STOCK.items():
            session.add(Stock(product=product, quantity=quantity))
        session.commit()

    with patch("consumer.KafkaConsumer"):
        dlq_producer = MagicMock()
        return InventoryConsumer(
            "localhost:9092", dlq_producer=dlq_producer, session_factory=session_factory
        ), session_factory


@pytest.mark.parametrize(
    "order",
    [
        {"order_id": "1", "product": "laptop", "quantity": 2},
        {"order_id": "2", "product": "phone", "quantity": 10},
        {"order_id": "3", "product": "unknown-product", "quantity": 1},
    ],
)
def test_consumer_accepts_any_orders_contract_valid_message(order):
    jsonschema.validate(instance=order, schema=ORDERS_SCHEMA)

    c, session_factory = make_consumer()
    c._process(order)

    with session_factory() as session:
        published = session.query(OutboxEvent).order_by(OutboxEvent.id.desc()).first().payload
    jsonschema.validate(instance=published, schema=INVENTORY_RESERVED_SCHEMA)


def test_producer_output_conforms_to_inventory_reserved_contract():
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = InventoryProducer("localhost:9092")
        event = {"order_id": "1", "product": "laptop", "quantity": 2, "inventory_status": "reserved"}
        p.send(event)

        published = mock_kafka.send.call_args[0][1]
        jsonschema.validate(instance=published, schema=INVENTORY_RESERVED_SCHEMA)


def test_inventory_reserved_contract_rejects_unknown_status():
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"order_id": "1", "product": "laptop", "quantity": 2, "inventory_status": "maybe"},
            schema=INVENTORY_RESERVED_SCHEMA,
        )


def test_dlq_producer_output_conforms_to_dlq_envelope_contract():
    with patch("dlq_producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = DLQProducer("localhost:9092", source_topic="orders", consumer_group="inventory-group")
        p.send(b'{"order_id": "1", "product": "laptop", "quantity": "two"}', error="boom")

        published = mock_kafka.send.call_args[0][1]
        jsonschema.validate(instance=published, schema=DLQ_ENVELOPE_SCHEMA)
        assert published["source_topic"] == "orders"
        assert published["consumer_group"] == "inventory-group"
