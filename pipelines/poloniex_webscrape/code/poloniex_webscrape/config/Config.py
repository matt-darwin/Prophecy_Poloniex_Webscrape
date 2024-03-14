from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, currency_pair: str=None, limit: int=None, **kwargs):
        self.spark = None
        self.update(currency_pair, limit)

    def update(self, currency_pair: str="BTC_USDT", limit: int=1000, **kwargs):
        prophecy_spark = self.spark
        self.currency_pair = currency_pair
        self.limit = self.get_int_value(limit)
        pass
