import logging
import os
import signal
import threading

from alembic import command
from alembic.config import Config

from consumer import IN_TOPIC, InventoryConsumer
from db import make_engine, make_session_factory
from dlq_producer import DLQProducer
from outbox_relay import OutboxRelay
from producer import InventoryProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_migrations(database_url: str) -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cfg = Config(os.path.join(base_dir, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(base_dir, "alembic"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")


if __name__ == "__main__":
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    database_url = os.getenv("DATABASE_URL", "sqlite:///./inventory.db")

    logger.info("Running database migrations...")
    run_migrations(database_url)

    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)

    dlq_producer = DLQProducer(bootstrap_servers, source_topic=IN_TOPIC, consumer_group="inventory-group")
    downstream_producer = InventoryProducer(bootstrap_servers)
    relay = OutboxRelay(session_factory, producer=downstream_producer)
    consumer = InventoryConsumer(bootstrap_servers, dlq_producer=dlq_producer, session_factory=session_factory)

    def _handle_shutdown(signum, _frame):
        logger.info("Received signal %s, shutting down gracefully...", signum)
        consumer.stop()
        relay.stop()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    relay_thread = threading.Thread(target=relay.run_forever, daemon=True)
    relay_thread.start()

    try:
        consumer.consume()
    finally:
        consumer.close()
        relay.stop()
        relay_thread.join(timeout=5)
        downstream_producer.close()
