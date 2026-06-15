import logging
import os

from consumer import InventoryConsumer
from producer import InventoryProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = InventoryProducer(bootstrap_servers)
    consumer = InventoryConsumer(bootstrap_servers, producer=producer)
    try:
        consumer.consume()
    finally:
        consumer.close()