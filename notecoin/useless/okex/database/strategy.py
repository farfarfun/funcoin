import time

from notecoin.database.base import Base, BaseTable
from notecoin.strategy.utils import trade_api
from sqlalchemy import Column, Float, String


class OkexStrategyAutoSeller(Base, BaseTable):
    __tablename__ = 'okex_strategy_auto_seller'
    coin_id = Column(String(20), comment='coin_id', primary_key=True)
    price = Column(Float, comment='price')
    count = Column(Float, comment='count')
    worth = Column(Float, comment='worth')
    init_worth = Column(Float, comment='init_worth')
    max_worth = Column(Float, comment='max_worth')
    min_worth = Column(Float, comment='min_worth')
    updateTime = Column(Float, comment='updateTime')

    def __init__(self, *args, **kwargs):
        self.coin_id = kwargs.get("coin_id")
        self.price = kwargs.get("price")
        self.count = kwargs.get("count")
        self.worth = kwargs.get("worth")
        self.init_worth = kwargs.get("init_worth") or kwargs.get("worth")
        self.max_worth = kwargs.get("max_worth") or kwargs.get("init_worth") or kwargs.get("worth")
        self.min_worth = kwargs.get("min_worth") or kwargs.get("init_worth") or kwargs.get("worth")
        self.updateTime = int(time.time() * 1000)
        super(OkexStrategyAutoSeller, self).__init__(*args, **kwargs)

    def update_worth(self, worth, price=None, count=None):
        self.worth = worth
        self.price = price or self.price
        self.count = count or self.count
        if worth > self.max_worth:
            self.max_worth = worth
        if worth < self.min_worth:
            self.min_worth = worth
        self.updateTime = int(time.time() * 1000)

    def check(self):
        if self.max_worth * 0.95 > self.worth > self.init_worth * 1.01:
            return True
        else:
            return False

    def to_json(self):
        return {
            "coin_id": self.coin_id,
            "usdt": round(self.worth, 2)
        }

    def buy(self, worth=50):
        return trade_api.place_order(instId=self.coin_id, tdMode='cash', side='buy', ordType='market', sz=worth)

    def sell(self):
        return trade_api.place_order(instId=self.coin_id, tdMode='cash', side='sell', ordType='market', sz=self.count)

    @staticmethod
    def instance(coin_id, price=None, count=None, worth=None, init_worth=50):
        return OkexStrategyAutoSeller(coin_id=coin_id, price=price, count=count, worth=worth, init_worth=init_worth)
