import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from db import OrderRecord, OutboxEvent, make_engine, make_session_factory
from models import Order
from outbox_relay import OutboxRelay
from producer import OrderProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

session_factory: Optional[sessionmaker] = None
downstream_producer: Optional[OrderProducer] = None
relay: Optional[OutboxRelay] = None
relay_thread: Optional[threading.Thread] = None


def run_migrations(database_url: str) -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cfg = Config(os.path.join(base_dir, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(base_dir, "alembic"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(cfg, "head")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global session_factory, downstream_producer, relay, relay_thread

    database_url = os.getenv("DATABASE_URL", "sqlite:///./order.db")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    logger.info("Running database migrations...")
    run_migrations(database_url)

    engine = make_engine(database_url)
    session_factory = make_session_factory(engine)

    downstream_producer = OrderProducer(bootstrap_servers)
    relay = OutboxRelay(session_factory, producer=downstream_producer)
    relay_thread = threading.Thread(target=relay.run_forever, daemon=True)
    relay_thread.start()

    yield

    # uvicorn forwards SIGTERM/SIGINT into ASGI shutdown, which resumes here
    # — this is this service's graceful shutdown path (see README
    # "Resilience"), mirroring the consumer services' signal handlers.
    relay.stop()
    relay_thread.join(timeout=5)
    downstream_producer.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=201)
def create_order(order: Order):
    """Writes the order and an outbox event in one DB transaction (the
    Outbox pattern — see db.py / outbox_relay.py), so the order is durably
    stored even if Kafka is unreachable at request time; a background relay
    publishes it once Kafka is available.

    A repeated POST for an order_id that already exists is idempotent: it
    returns the same response without creating a second order or a second
    outbox event.
    """
    with session_factory() as session:
        if session.get(OrderRecord, order.order_id) is None:
            session.add(OrderRecord(order_id=order.order_id, product=order.product, quantity=order.quantity))
            session.add(OutboxEvent(topic="orders", payload=order.model_dump()))
            try:
                session.commit()
            except IntegrityError:
                # Lost a race with a concurrent request for the same
                # order_id — the order exists either way, so this is fine.
                session.rollback()

    return {"message": "Order received", "order_id": order.order_id}
