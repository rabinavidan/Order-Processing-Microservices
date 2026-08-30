import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
from consumer import OrderConsumer

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def load_schema(name: str) -> dict:
    return json.loads((CONTRACTS_DIR / name).read_text())


ORDERS_SCHEMA = load_schema("orders.schema.json")


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
        c = OrderConsumer("localhost:9092")
        with caplog.at_level(logging.INFO):
            c._process(order)

    assert order["order_id"] in caplog.text
