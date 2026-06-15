import json
import logging
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

IN_TOPIC = "orders"

_DEFAULT_STOCK = {"laptop": 10, "phone": 5, "keyboard": 20}


class InventoryConsumer:
    def __init__(self, bootstrap_servers: str, producer, topic: str = IN_TOPIC):
        self._stock = dict(_DEFAULT_STOCK)
        self._producer = producer
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="inventory-group",
        )

    def consume(self) -> None:
        for message in self._consumer:
            self._process(message.value)

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