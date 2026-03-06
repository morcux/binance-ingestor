from typing import List

import structlog
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from app.db.clickhouse import db
from app.db.redis import redis_client
from app.models.market_data import AggregatedCandle

logger = structlog.get_logger()
router = APIRouter()


@router.get("/history/{symbol}", response_model=List[AggregatedCandle])
async def get_history(symbol: str, limit: int = 60):
    query = f"""
    SELECT minute, symbol, avg_mid_price, open_bid_qty, close_bid_qty, open_ask_qty, close_ask_qty
    FROM market_data.candles_1m 
    WHERE symbol = '{symbol.upper()}' 
    ORDER BY minute DESC LIMIT {limit}
    """
    try:
        rows = await db.fetch(query)

        results = []
        for row in rows:
            results.append(
                AggregatedCandle(
                    minute=row[0],
                    symbol=row[1],
                    avg_mid_price=row[2],
                    open_bid_qty=row[3],
                    close_bid_qty=row[4],
                    open_ask_qty=row[5],
                    close_ask_qty=row[6],
                )
            )
        return results
    except Exception as e:
        logger.error("api.history.error", error=str(e))
        raise HTTPException(status_code=500, detail="Internal DB error")


@router.websocket("/ws/{symbol}/1m_candle")
async def websocket_1m_candle(websocket: WebSocket, symbol: str):
    await websocket.accept()
    logger.info("ws.client.connected", symbol=symbol)

    if not redis_client.redis:
        await websocket.close(code=1011)
        return

    pubsub = redis_client.redis.pubsub()
    channel = f"ticker:{symbol.lower()}_1m_candle"
    await pubsub.subscribe(channel)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        logger.info("ws.client.disconnected", symbol=symbol)
    except Exception as e:
        logger.error("ws.client.error", error=str(e))
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()


@router.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await websocket.accept()
    logger.info("ws.client.connected", symbol=symbol)

    if not redis_client.redis:
        await websocket.close(code=1011)
        return

    pubsub = redis_client.redis.pubsub()
    channel = f"ticker:{symbol.lower()}"
    await pubsub.subscribe(channel)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        logger.info("ws.client.disconnected", symbol=symbol)
    except Exception as e:
        logger.error("ws.client.error", error=str(e))
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
