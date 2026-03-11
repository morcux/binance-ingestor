import asyncio
from collections import deque

import aiohttp
import structlog

logger = structlog.get_logger(__name__)


class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id = 0
        self.synced = False
        self.buffer = deque()
        self._lock = asyncio.Lock()

    async def sync(self):
        url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol}&limit=5000"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.error(
                            "orderbook.sync_failed",
                            symbol=self.symbol,
                            status=resp.status,
                        )
                        return
                    data = await resp.json()

            async with self._lock:
                self.last_update_id = data["lastUpdateId"]
                self.bids = {float(p): float(q) for p, q in data["bids"]}
                self.asks = {float(p): float(q) for p, q in data["asks"]}

                while self.buffer:
                    event = self.buffer.popleft()
                    if event["u"] <= self.last_update_id:
                        continue
                    self._apply_diff(event["b"], event["a"])
                    self.last_update_id = event["u"]

                self.synced = True
                logger.info("orderbook.synced", symbol=self.symbol)

        except Exception as e:
            logger.error("orderbook.sync_error", symbol=self.symbol, error=str(e))

    def _apply_diff(self, bids: list, asks: list):
        for p, q in bids:
            price, qty = float(p), float(q)
            if qty == 0.0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for p, q in asks:
            price, qty = float(p), float(q)
            if qty == 0.0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    async def process_diff(self, event: dict):
        async with self._lock:
            if not self.synced:
                self.buffer.append(event)
                if len(self.buffer) > 2000:
                    self.buffer.popleft()
                return

            if event["u"] <= self.last_update_id:
                return

            if event["U"] > self.last_update_id + 1:
                logger.warning("orderbook.desync", symbol=self.symbol)
                self.synced = False
                asyncio.create_task(self.sync())
                return

            self._apply_diff(event["b"], event["a"])
            self.last_update_id = event["u"]

    async def get_depth_volumes(self, percent: float) -> tuple[float, float]:
        async with self._lock:
            if not self.synced or not self.bids or not self.asks:
                return 0.0, 0.0

            best_bid = max(self.bids.keys())
            best_ask = min(self.asks.keys())

            bid_limit = best_bid * (1.0 - percent)
            ask_limit = best_ask * (1.0 + percent)

            bid_vol = sum(q for p, q in self.bids.items() if p >= bid_limit)
            ask_vol = sum(q for p, q in self.asks.items() if p <= ask_limit)

            return bid_vol, ask_vol
