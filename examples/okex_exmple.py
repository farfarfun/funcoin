import time

from notecoin.okex.strategy.domain import OkexCoin
from notecoin.okex.strategy.utils import account_api


class CoinList:
    def __init__(self):
        self.usdt = 0
        self.coin_map = {}

    def load(self):
        data = account_api.get_account().data[0]

        for detail in data['details']:
            if detail['ccy'] == 'USDT':
                self.usdt = detail['availBal']
                continue
            coin = OkexCoin.instance_by_account(detail)
            if coin.money > 1:
                self.coin_map[coin.coin_id] = coin

    def print(self):
        print("****************************************************")
        print(self.usdt)
        for coin in self.coin_map.values():
            print(coin)

    def watch(self):
        while True:
            self.load()
            self.print()
            time.sleep(1)
            for coin in self.coin_map.values():
                coin.watch()


col = CoinList()
col.watch()
