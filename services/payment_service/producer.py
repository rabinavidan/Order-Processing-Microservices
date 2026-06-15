import json
from kafka import KafkaProducer

TOPIC = "payments.processed"


class PaymentProducer:
    def __init__(self, bootstrap_servers: str, topic: str = TOPIC):
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, event: dict) -> None:
        self._producer.send(self._topic, event)
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()