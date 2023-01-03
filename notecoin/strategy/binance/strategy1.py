import json
import time

import pandas as pd
from notecoin.base.tables.strategy import StrategyTable
from notecoin.task import AccountTask, BaseTask, MarketTask, Ticker24HTask
from notecoin.utils import logger


class StrategyTask(BaseTask):

    def __init__(self, *args, **kwargs):
        super(StrategyTask, self).__init__(*args, **kwargs)
        self.table = StrategyTable(db_suffix=self.exchange.name)
        self.table.create()

    def buy_auto(self):
        account = pd.read_sql(sql=f"select * from {AccountTask.table_name}", con=self.engine.connect())
        curr = 0
        for symbol in json.loads(account.to_json(orient='records')):
            if symbol['symbol'] == 'BUSD':
                curr = symbol['free']
        if curr < 12:
            logger.info("account<12")
            return
        market = pd.read_sql(sql=f"select * from {MarketTask.table_name}", con=self.engine.connect())
        ticker24h = pd.read_sql(sql=f"select * from {Ticker24HTask.table_name}", con=self.engine.connect())
        ticker24h = ticker24h.sort_values(['quoteVolume'], ascending=False).reset_index(drop=True)
        
        for symbol_info in json.loads(ticker24h.to_json(orient='records')):
            try:
                symbol = symbol_info['symbol']
                if symbol not in ("BTCBUSD", "BTC/BUSD"):
                    continue

                time.sleep(1)
                tmp = market[market['id'] == symbol]
                print(f'{symbol},{len(tmp)}')
                if len(tmp) == 1:
                    self.buy_market(tmp['symbol'].values[0])
            except Exception as e:
                return

        logger.info("done")

    def sell_auto(self):
        data = pd.read_sql(f"select * from {self.table.table_name}", con=self.engine.connect())
        for row in json.loads(data.to_json(orient='records')):
            try:
                time.sleep(1)
                symbol = row['symbol']
                if row['status'] != 2:
                    continue
                price = self.current_price(symbol)

                buy_info = json.loads(row['buy_json'])
                buy_price = buy_info['price']
                amount = buy_info['amount']  # *0.995
                timestamp = buy_info['timestamp']
                if time.time() * 1000 - timestamp > 30 * 60 * 1000 and abs((buy_price - price) / price) > 0:
                    logger.info(f"out of time,sell {buy_price} vs {price}")
                    self.sell_market(row['id'], symbol, amount)
                elif abs((buy_price - price) / price) > 0.001:
                    logger.info(f"buy price {buy_price} vs {price}")
                    self.sell_market(row['id'], symbol, amount)
            except Exception as e:
                logger.info(f"sell error {e}")

    def buy_market(self, symbol, total=12):
        price = self.current_price(symbol)
        if price == 0:
            return
        logger.info(f"buy {symbol}")

        amount = total / price
        buy_json = self.exchange.create_order(symbol, 'market', 'buy', amount)
        value = {
            "status": 2,
            "ext_json": {},
            "symbol": symbol,
            "amount": amount,
            "buy_json": buy_json,
        }

        self.table.upsert(value=value)

    def sell_market(self, id, symbol, amount):
        logger.info(f"sell {symbol}")

        sell_json = self.exchange.create_order(symbol, 'market', 'sell', amount)
        value = {
            "id": id,
            "status":3,
            "sell_json": sell_json,
        }

        self.table.upsert(value=value)
