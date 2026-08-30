import logging
import time
from datetime import datetime, timezone

from db import OutboxEvent

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_SECONDS = 0.5
DEFAULT_BATCH_SIZE = 50


class OutboxRelay:
    """Publishes OutboxEvent rows a consumer wrote as part of its own DB
    transaction, to Kafka.

    Decoupling the Kafka publish from message processing this way means a
    producer/broker outage never loses or duplicates the business-state
    change: the row simply stays unpublished until a later poll succeeds,
    and the DB write (already committed) is the source of truth regardless
    of Kafka's availability at the time processing happened. A publish that
    times out after actually reaching the broker can cause an at-least-once
    duplicate publish here — the same idempotent-consumer design used
    throughout this system (see README "Resilience") is what makes that
    safe downstream.
    """

    def __init__(
        self,
        session_factory,
        producer,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self._session_factory = session_factory
        self._producer = producer
        self._poll_interval_seconds = poll_interval_seconds
        self._batch_size = batch_size
        self._running = True

    def publish_pending(self) -> int:
        """Publish up to one batch of unpublished outbox rows. Returns how many succeeded."""
        published = 0
        with self._session_factory() as session:
            pending = (
                session.query(OutboxEvent)
                .filter(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(self._batch_size)
                .all()
            )
            for row in pending:
                try:
                    self._producer.send(row.payload)
                    row.published_at = datetime.now(timezone.utc)
                    session.commit()
                    published += 1
                except Exception as exc:
                    logger.warning("Failed to publish outbox event id=%s topic=%s: %s", row.id, row.topic, exc)
                    session.rollback()
        return published

    def run_forever(self) -> None:
        while self._running:
            published = self.publish_pending()
            if published == 0:
                time.sleep(self._poll_interval_seconds)

    def stop(self) -> None:
        self._running = False
