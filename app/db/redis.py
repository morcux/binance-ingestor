import redis.asyncio as redis
import structlog

from app.core.config import settings

logger = structlog.get_logger()


class AsyncRedis:
    def __init__(self):
        self.redis: redis.Redis | None = None

    async def connect(self):
        self.redis = redis.from_url(
            settings.REDIS_URL, encoding="utf-8", decode_responses=True
        )
        try:
            await self.redis.ping()  # type: ignore
            logger.info("db.redis.connected")
        except Exception as e:
            logger.error("db.redis.connection_failed", error=str(e))
            raise

    async def close(self):
        if self.redis:
            await self.redis.close()
            logger.info("db.redis.closed")

    async def publish(self, channel: str, message: str):
        if self.redis:
            await self.redis.publish(channel, message)


redis_client = AsyncRedis()
