from datetime import datetime

from pydantic import BaseModel


class OrderBookSnapshot(BaseModel):
    timestamp: datetime
    symbol: str
    bid_price: float
    ask_price: float
    bid_qty: float
    ask_qty: float


class AggregatedCandle(BaseModel):
    minute: datetime
    symbol: str
    avg_mid_price: float
    open_bid_qty: float
    close_bid_qty: float
    open_ask_qty: float
    close_ask_qty: float
