import os
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
from models import Order
from producer import OrderProducer

producer: Optional[OrderProducer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = OrderProducer(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    yield
    producer.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=201)
def create_order(order: Order):
    producer.send_order(order.model_dump())
    return {"message": "Order received", "order_id": order.order_id}
