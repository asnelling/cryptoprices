import asyncio
import json
import websockets
import requests

from datetime import datetime
from typing import Sequence

from .tick import Tick


class KuCoinApi:
    def __init__(self, ticks: asyncio.Queue):
        self.ticks = ticks
    

    async def subscribe(self, products: Sequence[str]):
        (token, server) = self.get_connect_params()
        url = f"{server}?token={token}"

        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "topic": "/market/ticker:" + ",".join(products),
                "response": False,
            }))
            await ws.recv()
            async for message in ws:
                data = json.loads(message)

                if data.get("subject") != "trade.ticker":
                    continue
                
                pair = data["topic"].split(":")[1]
                price = data["data"]["price"]

                await self.ticks.put(Tick(
                    exchange="kucoin",
                    pair=pair,
                    price=float(price),
                ))
    
    def get_connect_params(self):
        url = "https://api.kucoin.com/api/v1/bullet-public"
        r = requests.post(url)
        data = r.json()["data"]
        token = data["token"]
        server = data["instanceServers"][0]["endpoint"]
        return (token, server)
