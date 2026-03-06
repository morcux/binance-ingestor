import asyncio
import json
from datetime import datetime, timedelta

import structlog

from app.core.config import settings
from app.db.clickhouse import db
from app.db.redis import redis_client

logger = structlog.get_logger("broadcaster")


class CandleBroadcaster:
    def __init__(self):
        self.running = False

    async def run(self):
        self.running = True
        logger.info("broadcaster.started")

        while self.running:
            now = datetime.now()
            next_minute = (now + timedelta(minutes=1)).replace(second=3, microsecond=0)
            sleep_seconds = (next_minute - now).total_seconds()

            logger.debug("broadcaster.sleeping", seconds=sleep_seconds)
            await asyncio.sleep(sleep_seconds)

            if not self.running:
                break

            await self.broadcast_last_minute()

    async def broadcast_last_minute(self):
        target_minute = (datetime.now() - timedelta(minutes=1)).replace(
            second=0, microsecond=0
        )
        formatted_time = target_minute.strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
        SELECT 
            symbol,
            avg_mid_price,
            open_bid_qty,
            close_bid_qty,
            open_ask_qty,
            close_ask_qty
        FROM market_data.candles_1m
        WHERE minute = '{formatted_time}'
        AND symbol IN {tuple(s.upper() for s in settings.SYMBOLS)}
        """

        try:
            rows = await db.fetch(query)

            if not rows:
                logger.warning("broadcaster.no_data", minute=formatted_time)
                return

            tasks = []
            for row in rows:
                symbol = row[0].lower()

                payload = {
                    "event": "1m_candle_dump",
                    "minute": formatted_time,
                    "symbol": symbol,
                    "data": {
                        "avg_price": row[1],
                        "bid_qty_delta": row[3] - row[2],
                        "ask_qty_delta": row[5] - row[4],
                    },
                }

                channel = f"ticker:{symbol}_1m_candle"
                tasks.append(redis_client.publish(channel, json.dumps(payload)))

            await asyncio.gather(*tasks)
            logger.info("broadcaster.published", count=len(rows), minute=formatted_time)

        except Exception as e:
            logger.error("broadcaster.error", error=str(e))


broadcaster = CandleBroadcaster()
