import asyncio

import structlog
from aiochclient import ChClient
from aiohttp import ClientSession

from app.core.config import settings

logger = structlog.get_logger()


class AsyncClickHouse:
    def __init__(self):
        self.session: ClientSession | None = None
        self._client: ChClient | None = None

    async def connect(self):
        if not self.session:
            self.session = ClientSession()
            self._client = ChClient(
                self.session,
                url=settings.CLICKHOUSE_URL,
                database=settings.CLICKHOUSE_DB,
            )

            retries = 10
            for i in range(retries):
                try:
                    await self.client.execute("SELECT 1")
                    logger.info("db.clickhouse.connected")
                    return
                except Exception as e:
                    if i == retries - 1:
                        logger.error("db.clickhouse.connection_failed", error=str(e))
                        raise

                    wait_time = 2
                    logger.warning(
                        "db.clickhouse.waiting",
                        attempt=i + 1,
                        retries=retries,
                        wait=wait_time,
                        error=str(e),
                    )
                    await asyncio.sleep(wait_time)

    @property
    def client(self) -> ChClient:
        if self._client is None:
            raise ValueError("Not initialized")
        return self._client

    async def close(self):
        if self.session:
            await self.session.close()
            logger.info("db.clickhouse.closed")

    async def execute(self, query: str, *args):
        return await self.client.execute(query, *args)

    async def fetch(self, query: str, *args):
        return await self.client.fetch(query, *args)

    async def insert(self, table: str, data: list):
        if not data:
            return
        query = f"INSERT INTO {table} (timestamp, symbol, bid_price, ask_price, bid_qty, ask_qty) VALUES"
        await self.client.execute(query, *data)


db = AsyncClickHouse()
