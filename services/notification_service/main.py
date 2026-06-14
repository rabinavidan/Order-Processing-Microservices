import logging
import os

from consumer import OrderConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = OrderConsumer(bootstrap_servers)
    try:
        consumer.consume()
    finally:
        consumer.close()
