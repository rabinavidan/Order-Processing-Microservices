from datetime import datetime, timezone

from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class Payment(Base):
    """One row per order_id this consumer has seen on `inventory.reserved`.

    Doubles as both the durable business record (was this order paid, or
    skipped because inventory wasn't reserved?) and the idempotency ledger:
    a redelivered event for an order_id already present here is recognized
    and skipped before it reaches business logic."""

    __tablename__ = "payments"

    order_id = Column(String, primary_key=True)
    product = Column(String, nullable=True)
    quantity = Column(Integer, nullable=True)
    inventory_status = Column(String, nullable=False)
    payment_status = Column(String, nullable=True)
    processed_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class OutboxEvent(Base):
    """Outbox pattern: a consumer writes its business-state change and this
    row in the SAME DB transaction, so the two can never disagree. A
    separate OutboxRelay (see outbox_relay.py) publishes unpublished rows to
    Kafka asynchronously and marks them published — a Kafka/broker outage at
    processing time can never lose or half-apply an update."""

    __tablename__ = "outbox_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    published_at = Column(DateTime(timezone=True), nullable=True)


def make_engine(database_url: str):
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, connect_args=connect_args)


def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
