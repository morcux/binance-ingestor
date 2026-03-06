import asyncio
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.v1 import endpoints
from app.db.clickhouse import db
from app.db.redis import redis_client
from app.services.broadcaster import broadcaster

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app.startup")
    await db.connect()
    await redis_client.connect()
    broadcaster_task = asyncio.create_task(broadcaster.run())
    logger.info("Printing all registered routes:")
    yield
    broadcaster_task.cancel()
    await db.close()
    await redis_client.close()


app = FastAPI(title="Binance Ingestor", lifespan=lifespan)

app.include_router(endpoints.router, prefix="/api/v1")
