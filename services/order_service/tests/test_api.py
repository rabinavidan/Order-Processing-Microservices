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
