import asyncio

from argparse import ArgumentParser
from configparser import ConfigParser

from cryptoprices.api.bittrex import BittrexApi
from cryptoprices.api.coinbase_pro import CoinbaseProApi
from cryptoprices.api.kraken import KrakenApi
from cryptoprices.api.kucoin import KuCoinApi


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
    

    def get_products(self, exchange: str) -> list[str]:
        return self.enabled_products[exchange]


async def watch_queue(queue: asyncio.Queue):
    while True:
        tick = await queue.get()
        print(f"{tick.exchange} {tick.pair} {tick.price} {tick.created_at}")


async def start(args):
    config = Config(args.config_file)

    queue = asyncio.Queue()
    api = BittrexApi(queue)
    coinbase_pro_api = CoinbaseProApi(queue)
    kraken_api = KrakenApi(queue)
    kucoin_api = KuCoinApi(queue)

    await asyncio.gather(
        watch_queue(queue),
        api.subscribe(config.get_products("bittrex")),
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
