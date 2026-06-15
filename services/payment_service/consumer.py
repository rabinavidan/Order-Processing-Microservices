import json
import logging
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

IN_TOPIC = "inventory.reserved"


class PaymentConsumer:
    def __init__(self, bootstrap_servers: str, producer, topic: str = IN_TOPIC):
        self._producer = producer
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="payment-group",
        )

    def consume(self) -> None:
        for message in self._consumer:
            self._process(message.value)

    def _process(self, event: dict) -> None:
        if event.get("inventory_status") != "reserved":
            logger.info("Payment skipped: order_id=%s reason=inventory_%s",
                        event.get("order_id"), event.get("inventory_status", "unknown"))
            return

        logger.info("Payment processed: order_id=%s product=%s qty=%s status=paid",
                    event.get("order_id"), event.get("product"), event.get("quantity"))
        self._producer.send({**event, "payment_status": "paid"})

    def close(self) -> None:
        self._consumer.close()
        self._producer.close()