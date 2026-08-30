import logging
import os
import signal

from consumer import IN_TOPIC, InventoryConsumer
from dlq_producer import DLQProducer
from producer import InventoryProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = InventoryProducer(bootstrap_servers)
    dlq_producer = DLQProducer(bootstrap_servers, source_topic=IN_TOPIC, consumer_group="inventory-group")
    consumer = InventoryConsumer(bootstrap_servers, producer=producer, dlq_producer=dlq_producer)

    def _handle_shutdown(signum, _frame):
        logger.info("Received signal %s, shutting down gracefully...", signum)
        consumer.stop()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    try:
        consumer.consume()
    finally:
        consumer.close()
