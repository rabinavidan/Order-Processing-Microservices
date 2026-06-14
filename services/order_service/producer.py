import json
from kafka import KafkaProducer

TOPIC = "orders"


class OrderProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_order(self, order: dict) -> None:
        self._producer.send(TOPIC, order)
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()
