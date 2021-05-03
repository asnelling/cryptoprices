import asyncio
import os

from argparse import ArgumentParser
from configparser import ConfigParser
from typing import Sequence

from cryptoprices.api.bittrex import BittrexApi
from cryptoprices.api.coinbase_pro import CoinbaseProApi
from cryptoprices.api.kraken import KrakenApi
from cryptoprices.api.kucoin import KuCoinApi
from cryptoprices.api.tick import Tick

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


class Config:
    def __init__(self, filename: str):
        self.config = ConfigParser()
        self.config.read(filename)
        enabled_products = {}
        for (exchange, products) in self.config.items():
            enabled_products[exchange] = []
            for pid in products.keys():
                if products.getboolean(pid):
                    enabled_products[exchange].append(pid.upper())
        self.enabled_products = enabled_products
    

    def get_products(self, exchange: str) -> Sequence[str]:
        return self.enabled_products[exchange]


async def watch_queue(queue: asyncio.Queue):
    bucket = os.environ["CRYPTOPRICES_INFLUXDB_BUCKET"]
    org = os.environ["CRYPTOPRICES_INFLUXDB_ORG"]
    token = os.environ["CRYPTOPRICES_INFLUXDB_TOKEN"]
    url = os.environ["CRYPTOPRICES_INFLUXDB_URL"]
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    while True:
        records = []
        while not queue.empty():
            tick: Tick = await queue.get()
            p = Point("tick").tag("exchange", tick.exchange).tag("pair", tick.pair).field("price", tick.price).time(tick.created_at.isoformat())
            records.append(p)

        if len(records) > 0:
            write_api.write(bucket=bucket, org=org, record=records)
            records = []
        
        await asyncio.sleep(5)


async def start(args):
    config = Config(args.config_file)

    queue = asyncio.Queue()
    api = BittrexApi(queue)
    coinbase_pro_api = CoinbaseProApi(queue)
    kraken_api = KrakenApi(queue)
    kucoin_api = KuCoinApi(queue)

    await asyncio.gather(
        watch_queue(queue),
        api.start(config.get_products("bittrex")),
        coinbase_pro_api.subscribe(config.get_products("coinbase_pro")),
        kraken_api.subscribe(config.get_products("kraken")),
        kucoin_api.subscribe(config.get_products("kucoin")),
    )


def main():
    parser = ArgumentParser(description="Monitor crypto prices across exchanges")
    parser.add_argument("-c", "--config-file", default="cryptoprices.conf", help="Configuration file location")
    args = parser.parse_args()

    asyncio.run(start(args))


if __name__ == "__main__":
    main()
