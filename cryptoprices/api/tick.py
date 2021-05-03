from datetime import datetime, timezone


class Tick:
    def __init__(self, exchange: str, pair: str, price: str, created_at: datetime = None):
        self.exchange = exchange
        self.pair = pair
        self.price = price
        self.created_at = created_at or datetime.now(timezone.utc)
