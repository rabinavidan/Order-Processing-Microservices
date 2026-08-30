import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
from producer import OrderProducer

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
def test_producer_output_conforms_to_orders_contract(order):
    with patch("producer.KafkaProducer") as mock_cls:
        mock_kafka = MagicMock()
        mock_cls.return_value = mock_kafka
        p = OrderProducer("localhost:9092")
        p.send_order(order)

        published = mock_kafka.send.call_args[0][1]
        jsonschema.validate(instance=published, schema=ORDERS_SCHEMA)


def test_orders_contract_rejects_missing_required_field():
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"order_id": "1", "product": "laptop"}, schema=ORDERS_SCHEMA)


def test_orders_contract_rejects_wrong_type():
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"order_id": "1", "product": "laptop", "quantity": "two"},
            schema=ORDERS_SCHEMA,
        )
