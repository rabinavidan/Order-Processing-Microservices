from datetime import datetime, timezone

from sqlalchemy import JSON, Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class OrderRecord(Base):
    """Durable order record — replaces publishing straight to Kafka with no
    local record at all. Named OrderRecord (not Order) to stay distinct from
    the Pydantic request model in models.py."""

    __tablename__ = "orders"

    order_id = Column(String, primary_key=True)
    product = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class OutboxEvent(Base):
    """Outbox pattern: POST /orders writes the OrderRecord and this row in
    the SAME DB transaction, so the two can never disagree. A separate
    OutboxRelay (see outbox_relay.py) publishes unpublished rows to Kafka
    asynchronously and marks them published — if Kafka is down at request
    time, the order is still durably stored and gets published as soon as
    Kafka recovers, instead of the request failing or silently losing the
    event."""

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
