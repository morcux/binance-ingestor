import asyncio

import aiohttp
import structlog

from app.core.config import settings

logger = structlog.get_logger(__name__)


class TelegramNotifier:
    def __init__(self):
        self.token = settings.TG_BOT_TOKEN
        self.chat_id = settings.TG_CHAT_ID
        self._cooldowns = {}
        self._lock = asyncio.Lock()

    async def send_anomaly(self, symbol: str, msg: str):
        if not self.token or not self.chat_id:
            return

        now = asyncio.get_event_loop().time()

        async with self._lock:
            last_sent = self._cooldowns.get(symbol, 0)
            if now - last_sent < settings.TG_COOLDOWN_SEC:
                return
            self._cooldowns[symbol] = now

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": f"🚨 <b>Спред Аномалія: {symbol}</b>\n\n{msg}",
            "parse_mode": "HTML",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info("telegram.sent", symbol=symbol)
                    else:
                        logger.error("telegram.bad_response", status=resp.status)
        except Exception as e:
            logger.error("telegram.error", error=str(e))


tg_notifier = TelegramNotifier()
