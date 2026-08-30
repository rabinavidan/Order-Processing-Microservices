import json
import logging
import time

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

IN_TOPIC = "orders"
GROUP_ID = "inventory-group"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 0.5

_DEFAULT_STOCK = {"laptop": 10, "phone": 5, "keyboard": 20}


class InventoryConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        producer,
        dlq_producer,
        topic: str = IN_TOPIC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    ):
        self._stock = dict(_DEFAULT_STOCK)
        self._producer = producer
        self._dlq_producer = dlq_producer
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._seen_order_ids: set = set()
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
            # which idempotent processing (dedup by order_id) makes safe.
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

    def _handle_message(self, raw_value: bytes) -> None:
        try:
            order = json.loads(raw_value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Malformed message on %s, routing to DLQ: %s", IN_TOPIC, exc)
            self._dlq_producer.send(raw_value, error=str(exc))
            return

        order_id = order.get("order_id")
        if order_id is not None and order_id in self._seen_order_ids:
            logger.info("Duplicate order_id=%s already processed, skipping (idempotent)", order_id)
            return

        for attempt in range(1, self._max_retries + 1):
            try:
                self._process(order)
                if order_id is not None:
                    self._seen_order_ids.add(order_id)
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

    def _process(self, order: dict) -> None:
        product = order.get("product")
        qty = order.get("quantity", 0)
        available = self._stock.get(product, 0)

        if available >= qty:
            self._stock[product] = available - qty
            status = "reserved"
            logger.info("Inventory reserved: product=%s qty=%s remaining=%s", product, qty, self._stock[product])
        else:
            status = "insufficient"
            logger.warning("Inventory insufficient: product=%s requested=%s available=%s", product, qty, available)

        self._producer.send({**order, "inventory_status": status})

    def close(self) -> None:
        self._consumer.close()
        self._producer.close()
        self._dlq_producer.close()
