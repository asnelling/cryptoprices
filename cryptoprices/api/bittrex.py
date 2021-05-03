import asyncio
import json

from base64 import b64decode
from typing import Sequence
from zlib import decompress, MAX_WBITS

from signalr_aio import Connection

from .tick import Tick


class BittrexApi:
    url = "https://socket-v3.bittrex.com/signalr"

    def __init__(self, ticks: asyncio.Queue):
        self.ticks = ticks
    
    async def subscribe(self, products: Sequence[str]):
        self.hub.client.on("ticker", self.on_ticker)
        self.hub.server.invoke("Subscribe", [f"ticker_{x}" for x in products])
        # await asyncio.to_thread(self.connection.start)
    
    async def connect(self):
        connection = Connection(self.url)
        self.hub = connection.register_hub("c3")
        connection.error += self.on_error
        connection.start()
        print("connected to bittrex")
    
    async def start(self, products: Sequence[str]):
        await self.connect()
        await self.subscribe(products)
        print("starting bittrex")
    
    async def on_error(self, msg):
        print(msg)
    
    async def on_message(self, **msg):
        if "R" in msg and type(msg["R"]) is not bool:
            print(f"R: {msg['R']}")

    async def on_ticker(self, msg):
        decoded_msg = self.process_message(msg[0])
        symbol = decoded_msg["symbol"]
        rate = decoded_msg["lastTradeRate"]
        # print(f"Bittrex {symbol} {rate}")
        await self.ticks.put(Tick(
            exchange="bittrex",
            pair=symbol,
            price=float(rate),
        ))
    
    def process_message(self, message):
        decompressed_msg = b""
        try:
            decompressed_msg = decompress(b64decode(message), -MAX_WBITS)
        except SyntaxError:
            decompressed_msg = decompress(b64decode(message))
        
        return json.loads(decompressed_msg.decode())
