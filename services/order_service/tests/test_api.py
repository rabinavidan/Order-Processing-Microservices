from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import main
from db import OrderRecord, OutboxEvent


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "test_order.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
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


def test_create_order_persists_order_and_outbox_event(client):
    payload = {"order_id": "126", "product": "phone", "quantity": 3}
    client.post("/orders", json=payload)

    with main.session_factory() as session:
        order = session.get(OrderRecord, "126")
        assert order is not None
        assert order.product == "phone"
        assert order.quantity == 3

        event = session.query(OutboxEvent).filter_by(topic="orders").one()
        assert event.payload == payload
        # The relay thread publishes asynchronously, so this may already be
        # published by the time we check — either state is valid here.


def test_create_order_response_body(client):
    payload = {"order_id": "127", "product": "keyboard", "quantity": 1}
    response = client.post("/orders", json=payload)
    assert response.json() == {"message": "Order received", "order_id": "127"}


def test_orders_get_not_allowed_returns_405(client):
    response = client.get("/orders")
    assert response.status_code == 405


def test_create_order_is_idempotent_on_repeated_order_id(client):
    payload = {"order_id": "128", "product": "laptop", "quantity": 1}

    response1 = client.post("/orders", json=payload)
    response2 = client.post("/orders", json=payload)

    assert response1.status_code == 201
    assert response2.status_code == 201
    assert response1.json() == response2.json()

    with main.session_factory() as session:
        assert session.query(OrderRecord).filter_by(order_id="128").count() == 1
        # This test's DB is isolated per-test (fresh tmp_path file), so the
        # total outbox row count is exactly the count for this one order_id.
        assert session.query(OutboxEvent).count() == 1
