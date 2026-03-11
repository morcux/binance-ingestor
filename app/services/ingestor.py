import asyncio
import json
from datetime import datetime

import aiohttp
import structlog

from app.core.config import settings
from app.db.clickhouse import db
from app.db.redis import redis_client
from app.services.anomaly_detector import anomaly_detector

logger = structlog.get_logger("ingestor")


class IngestorService:
    def __init__(self):
        self.buffer = []
        self.last_update = {}
        self.running = False

    async def flush_loop(self):
        logger.info("service.flush_loop.started")
        while self.running:
            await asyncio.sleep(1)
            if self.buffer:
                batch = self.buffer
                self.buffer = []

                try:
                    await db.insert("market_data.snapshots", batch)
                    logger.info("db.flush.success", count=len(batch))
                except Exception as e:
                    logger.error("db.flush.failed", error=str(e))
                    self.buffer.extend(batch)

    async def run(self):
        self.running = True
        await db.connect()
        await redis_client.connect()

        flush_task = asyncio.create_task(self.flush_loop())
        analysis_task = asyncio.create_task(anomaly_detector.run_analysis_loop())

        book_streams = [f"{s}@bookTicker" for s in settings.SYMBOLS]
        depth_streams = [f"{s}@depth@100ms" for s in settings.SYMBOLS]
        streams = "/".join(book_streams + depth_streams)

        url = f"{settings.BINANCE_WS_URL}/{streams}"

        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        logger.info("binance.ws.connected")

                        async for msg in ws:
                            if not self.running:
                                break

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)

                                if "e" in data and data["e"] == "depthUpdate":
                                    await anomaly_detector.process_depth_update(data)
                                else:
                                    await self.process_msg(data)

                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(
                                    "binance.ws.error", error=str(ws.exception())
                                )
                                break
            except Exception as e:
                logger.error("binance.ws.disconnected", error=str(e))
                await asyncio.sleep(5)

        analysis_task.cancel()
        await flush_task

    async def process_msg(self, data):
        # {'u': 40702220668, 's': 'BTCUSDT', 'b': '89000.01', 'B': '1.2', 'a': '89000.02', 'A': '0.5'}
        if "s" not in data:
            return

        symbol = data["s"]
        now = datetime.now()

        if (now - self.last_update.get(symbol, datetime.min)).total_seconds() < 0.1:
            return
        self.last_update[symbol] = now

        try:
            bid = float(data["b"])
            ask = float(data["a"])
            row = (
                now,
                symbol,
                bid,
                ask,
                float(data["B"]),
                float(data["A"]),
            )
            self.buffer.append(row)

            pub_data = {
                "ts": now.timestamp(),
                "s": symbol,
                "p": round((bid + ask) / 2, 2),
            }
            await redis_client.publish(f"ticker:{symbol.lower()}", json.dumps(pub_data))

        except (ValueError, KeyError) as e:
            logger.warning("msg.parse_error", error=str(e), data=data)
