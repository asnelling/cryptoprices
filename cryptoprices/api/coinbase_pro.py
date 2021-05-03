import asyncio
import json
import websockets

from datetime import datetime
from typing import Sequence

from .tick import Tick


class CoinbaseProApi:
    def __init__(self, ticks: asyncio.Queue):
        self.ticks = ticks
    

    async def subscribe(self, products: Sequence[str]):
        url = "wss://ws-feed.pro.coinbase.com"

        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": products,
                "channels": [
                    "ticker",
                ],
            }))
            await ws.recv()
            async for message in ws:
                data = json.loads(message)
                if data["type"] != "ticker":
                    continue

                created_at = datetime.fromisoformat(data["time"][:-1] + "+00:00")
                product_id = data["product_id"]
                price = data["price"]
                last_size = data["last_size"]
                await self.ticks.put(Tick(
                    exchange="coinbase_pro",
                    pair=product_id,
                    price=float(price),
                    created_at=created_at,
                ))
