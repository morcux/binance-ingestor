import structlog
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "Crypto Feed High-Load"

    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_DB: str = "market_data"
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = "clickhouse"

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379

    BINANCE_WS_URL: str = "wss://stream.binance.com:9443/ws"
    SYMBOLS: list[str] = ["btcusdt", "ethusdt", "solusdt", "bnbusdt"]

    @property
    def CLICKHOUSE_URL(self) -> str:
        return f"http://{self.CLICKHOUSE_USER}:{self.CLICKHOUSE_PASSWORD}@{self.CLICKHOUSE_HOST}:{self.CLICKHOUSE_PORT}"

    @property
    def REDIS_URL(self) -> str:
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}"

    class Config:
        env_file = ".env"


settings = Settings()

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
