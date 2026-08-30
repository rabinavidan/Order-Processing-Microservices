import json
import logging
import time

from kafka import KafkaConsumer

from db import OutboxEvent, Payment

logger = logging.getLogger(__name__)

IN_TOPIC = "inventory.reserved"
OUT_TOPIC = "payments.processed"
GROUP_ID = "payment-group"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 0.5


class PaymentConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        dlq_producer,
        session_factory,
        topic: str = IN_TOPIC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    ):
        self._dlq_producer = dlq_producer
        self._session_factory = session_factory
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._running = True
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            group_id=GROUP_ID,
            # Offsets are committed manually, only once a message has been
            # fully handled (processed or routed to the DLQ) — see consume().
            # This gives correct at-least-once delivery: a crash between
            # receiving and committing redelivers the message on restart,
            # which idempotent processing (dedup via the Payment table, see
            # _is_duplicate) makes safe even across a service restart.
            enable_auto_commit=False,
        )

    def consume(self) -> None:
        """Poll for messages until stop() is called.

        Uses poll() with a timeout rather than the blocking `for message in
        self._consumer` idiom so a graceful shutdown signal can interrupt the
        loop between messages instead of only between broker round-trips.
        """
        while self._running:
            records = self._consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    if not self._running:
                        return
                    self._handle_message(message.value)
                    self._consumer.commit()

    def stop(self) -> None:
        self._running = False

    def _is_duplicate(self, order_id) -> bool:
        if order_id is None:
            return False
        with self._session_factory() as session:
            return session.get(Payment, order_id) is not None

    def _handle_message(self, raw_value: bytes) -> None:
        try:
            event = json.loads(raw_value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Malformed message on %s, routing to DLQ: %s", IN_TOPIC, exc)
            self._dlq_producer.send(raw_value, error=str(exc))
            return

        order_id = event.get("order_id")
        if self._is_duplicate(order_id):
            logger.info("Duplicate order_id=%s already processed, skipping (idempotent)", order_id)
            return

        for attempt in range(1, self._max_retries + 1):
            try:
                self._process(event)
                return
            except Exception as exc:
                logger.warning(
                    "Failed to process order_id=%s (attempt %s/%s): %s",
                    order_id, attempt, self._max_retries, exc,
                )
                if attempt < self._max_retries:
                    time.sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))
                else:
                    logger.error("order_id=%s exceeded max retries, routing to DLQ", order_id)
                    self._dlq_producer.send(raw_value, error=str(exc))

    def _process(self, event: dict) -> None:
        """Record the payment outcome — atomically, in one DB transaction:
        the Payment row (business record + idempotency ledger) and, when a
        payment is actually made, the outbox event that will publish to
        `payments.processed`, commit together or not at all (the Outbox
        pattern)."""
        order_id = event.get("order_id")
        product = event.get("product")
        quantity = event.get("quantity")
        inventory_status = event.get("inventory_status") or "unknown"

        with self._session_factory() as session:
            if inventory_status != "reserved":
                logger.info("Payment skipped: order_id=%s reason=inventory_%s", order_id, inventory_status)
                session.add(Payment(
                    order_id=order_id, product=product, quantity=quantity,
                    inventory_status=inventory_status, payment_status=None,
                ))
                session.commit()
                return

            logger.info("Payment processed: order_id=%s product=%s qty=%s status=paid", order_id, product, quantity)
            payment_event = {**event, "payment_status": "paid"}
            session.add(OutboxEvent(topic=OUT_TOPIC, payload=payment_event))
            session.add(Payment(
                order_id=order_id, product=product, quantity=quantity,
                inventory_status=inventory_status, payment_status="paid",
            ))
            session.commit()

    def close(self) -> None:
        self._consumer.close()
        self._dlq_producer.close()
