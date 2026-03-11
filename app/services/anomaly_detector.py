import asyncio
import math
from collections import deque

import structlog

from app.core.config import settings
from app.services.orderbook import LocalOrderBook
from app.services.telegram import tg_notifier

logger = structlog.get_logger(__name__)


class RollingStats:
    def __init__(self, window_size=60):
        self.values = deque(maxlen=window_size)

    def update(self, value: float):
        self.values.append(value)

    @property
    def mean(self) -> float:
        if not self.values:
            return 0.0
        return sum(self.values) / len(self.values)

    @property
    def std(self) -> float:
        if len(self.values) < 2:
            return 0.0
        m = self.mean
        variance = max(
            0.0, sum((x - m) ** 2 for x in self.values) / (len(self.values) - 1)
        )
        return math.sqrt(variance)


class AnomalyDetector:
    def __init__(self):
        self.orderbooks = {s.upper(): LocalOrderBook(s) for s in settings.SYMBOLS}
        self.stats = {s.upper(): RollingStats(window_size=60) for s in settings.SYMBOLS}
        self.running = False

    async def process_depth_update(self, data: dict):
        symbol = data.get("s")
        if symbol and symbol in self.orderbooks:
            await self.orderbooks[symbol].process_diff(data)

    async def run_analysis_loop(self):
        self.running = True
        logger.info("anomaly.analysis_loop.started")

        for ob in self.orderbooks.values():
            asyncio.create_task(ob.sync())

        while self.running:
            await asyncio.sleep(1.0)
            try:
                for symbol, ob in self.orderbooks.items():
                    if not ob.synced:
                        continue

                    bid_vol, ask_vol = await ob.get_depth_volumes(
                        settings.DEPTH_PERCENT
                    )
                    if bid_vol == 0 and ask_vol == 0:
                        continue

                    vol_diff = bid_vol - ask_vol
                    stat = self.stats[symbol]

                    if len(stat.values) < 10:
                        stat.update(vol_diff)
                        continue

                    mean = stat.mean
                    std = stat.std
                    stat.update(vol_diff)

                    if (
                        std > 0
                        and abs(vol_diff - mean)
                        >= settings.ANOMALY_SIGMA_THRESHOLD * std
                    ):
                        await self._trigger_anomaly(
                            symbol, bid_vol, ask_vol, vol_diff, mean, std
                        )

            except Exception as e:
                logger.error("anomaly.loop.error", error=str(e))

    async def _trigger_anomaly(self, symbol, bid_vol, ask_vol, vol_diff, mean, std):
        logger.warning("anomaly.detected", symbol=symbol, diff=vol_diff, std=std)
        msg = (
            f"Різниця об'ємів відхилилася від норми (>{settings.ANOMALY_SIGMA_THRESHOLD}σ)\n\n"
            f"<b>Bid Vol (5%):</b> {bid_vol:,.2f}\n"
            f"<b>Ask Vol (5%):</b> {ask_vol:,.2f}\n"
            f"<b>Поточна різниця (Δ):</b> {vol_diff:,.2f}\n"
            f"<b>Очікувана (μ):</b> {mean:,.2f}\n"
            f"<b>Сигма (σ):</b> {std:,.2f}"
        )
        await tg_notifier.send_anomaly(symbol, msg)


anomaly_detector = AnomalyDetector()
