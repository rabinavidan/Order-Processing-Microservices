from pydantic import BaseModel


class Order(BaseModel):
    order_id: str
    product: str
    quantity: int
