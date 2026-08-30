from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient
import main


@pytest.fixture
def client():
    with patch("main.OrderProducer") as mock_cls:
        mock_cls.return_value = MagicMock()
        with TestClient(main.app) as c:
            yield c


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_create_order_returns_201(client):
    payload = {"order_id": "123", "product": "laptop", "quantity": 2}
    response = client.post("/orders", json=payload)
    assert response.status_code == 201
    assert response.json()["order_id"] == "123"


def test_create_order_invalid_payload_returns_422(client):
    response = client.post("/orders", json={"bad": "data"})
    assert response.status_code == 422


def test_create_order_wrong_quantity_type_returns_422(client):
    payload = {"order_id": "124", "product": "laptop", "quantity": "two"}
    response = client.post("/orders", json=payload)
    assert response.status_code == 422


def test_create_order_missing_product_returns_422(client):
    payload = {"order_id": "125", "quantity": 1}
    response = client.post("/orders", json=payload)
    assert response.status_code == 422


def test_create_order_publishes_to_kafka(client):
    payload = {"order_id": "126", "product": "phone", "quantity": 3}
    client.post("/orders", json=payload)
    main.producer.send_order.assert_called_once_with(payload)


def test_create_order_response_body(client):
    payload = {"order_id": "127", "product": "keyboard", "quantity": 1}
    response = client.post("/orders", json=payload)
    assert response.json() == {"message": "Order received", "order_id": "127"}


def test_orders_get_not_allowed_returns_405(client):
    response = client.get("/orders")
    assert response.status_code == 405
