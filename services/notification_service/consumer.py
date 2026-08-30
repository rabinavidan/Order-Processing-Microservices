import json
import logging
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class OrderConsumer:
    def __init__(self, bootstrap_servers: str, topic: str = "orders"):
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="notification-group",
        )

    def consume(self) -> None:
        for message in self._consumer:
            self._process(message.value)

    def _process(self, order: dict) -> None:
        logger.info(
            "Notification: Order received — id=%s product=%s qty=%s",
            order.get("order_id"),
            order.get("product"),
            order.get("quantity"),
        )

    def close(self) -> None:
        self._consumer.close()
