import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
from consumer import InventoryConsumer
from producer import InventoryProducer

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def load_schema(name: str) -> dict:
    return json.loads((CONTRACTS_DIR / name).read_text())


ORDERS_SCHEMA = load_schema("orders.schema.json")
INVENTORY_RESERVED_SCHEMA = load_schema("inventory_reserved.schema.json")


def make_consumer():
    with patch("consumer.KafkaConsumer"):
        producer = MagicMock()
        return InventoryConsumer("localhost:9092", producer=producer), producer


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

    c, producer = make_consumer()
    c._process(order)

    published = producer.send.call_args[0][0]
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
