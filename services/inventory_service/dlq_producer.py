import json
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class DLQProducer:
    """Publishes messages a consumer could not process to `<source_topic>.dlq`.

    The DLQ topic is shared by every consumer group of the source topic; the
    envelope's `consumer_group` field identifies which one gave up on the
    message, so a poison message never crashes or stalls a consumer — it is
    quarantined for inspection/replay instead.
    """

    def __init__(self, bootstrap_servers: str, source_topic: str, consumer_group: str):
        self._source_topic = source_topic
        self._consumer_group = consumer_group
        self._dlq_topic = f"{source_topic}.dlq"
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, raw_value: bytes, error: str) -> None:
        try:
            original_value = json.loads(raw_value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            original_value = raw_value.decode("utf-8", errors="replace")

        envelope = {
            "source_topic": self._source_topic,
            "consumer_group": self._consumer_group,
            "error": error,
            "original_value": original_value,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        logger.error("Routing message to DLQ topic=%s error=%s", self._dlq_topic, error)
        self._producer.send(self._dlq_topic, envelope)
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()
