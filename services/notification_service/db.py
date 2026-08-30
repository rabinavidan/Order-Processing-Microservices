from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class ProcessedMessage(Base):
    """Durable idempotency ledger: one row per order_id this consumer has already handled.

    Replaces the in-memory `set` used before M3 — that reset on every
    restart, so a redelivery landing after a restart wasn't caught. This
    table survives restarts, so idempotency actually holds across the
    service's full lifecycle, not just within one process run.
    """

    __tablename__ = "processed_messages"

    order_id = Column(String, primary_key=True)
    processed_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


def make_engine(database_url: str):
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, connect_args=connect_args)


def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
