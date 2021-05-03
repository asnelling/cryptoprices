import asyncio
import json
import websockets

from datetime import datetime
from typing import Sequence

from .tick import Tick


class KrakenApi:
    def __init__(self, ticks: asyncio.Queue):
        self.ticks = ticks
    
    async def subscribe(self, products: Sequence[str]):
        while True:
            try:
                await self._subscribe_once(products)
            except websockets.exceptions.ConnectionClosedError:
                pass

    async def _subscribe_once(self, products: Sequence[str]):
        url = "wss://ws.kraken.com"

        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({
                "event": "subscribe",
                "pair": products,
                "subscription": {
                    "name": "ticker",
                }
            }))
            await ws.recv()
            async for message in ws:
                data = json.loads(message)

                if "ticker" not in data:
                    continue
                
                pair = data[3]
                price = data[1]["c"][0]

                await self.ticks.put(Tick(
                    exchange="kraken",
                    pair=pair,
                    price=float(price),
                ))
