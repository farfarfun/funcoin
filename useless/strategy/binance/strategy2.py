import json
import time
from asyncio import run

import ccxt.pro as ccxtpro
import pandas as pd
from funcoin.base.tables.strategy import StrategyTable
from funcoin.task import AccountTask, BaseTask
from funcoin.utils import logger
from funsecret import read_secret


class Strategy2Task(BaseTask):

    def __init__(self, *args, **kwargs):
        super(Strategy2Task, self).__init__(*args, **kwargs)
        self.table = StrategyTable(db_suffix=self.exchange.name)
        self.table.create()

        self.residue = 0
        self.strategy_df = None
        self.account_task = AccountTask()
        self.update_account()

    def update_account(self):
        with self.engine.connect() as conn:
            self.account_task.refresh(conn=conn)
            account = pd.read_sql(sql=f"select * from {AccountTask.table_name}", con=conn)
            for symbol in json.loads(account.to_json(orient='records')):
                if symbol['symbol'] == 'BUSD':
                    self.residue = symbol['free']
                    break
            self.strategy_df = pd.read_sql(f"select * from {self.table.table_name} where status=2", con=conn)
        time.sleep(1)

    async def buy_auto(self, price_map):

        if self.residue < 12:
            return
        data = self.exchange.fetch_ohlcv(symbol='BTC/BUSD', timeframe='1m', limit=60)
        df = pd.DataFrame(data)
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df.sort_values('timestamp', ascending=False)
        opens = df['open'].values
        if opens[7] < opens[1]:
            return

        try:
            self.buy_market("BTC/BUSD", 12)
            logger.info("buy BTC/BUSD success")
        except Exception as e:
            return

    async def sell_auto(self, price_map):
        print(price_map)
        for row in json.loads(self.strategy_df.to_json(orient='records')):
            try:
                symbol = row['symbol']
                if symbol not in price_map.keys():
                    continue
                price = price_map[symbol]

                buy_info = json.loads(row['buy_json'])
                buy_price = buy_info['price']
                amount = buy_info['amount']
                timestamp = buy_info['timestamp']
                pct = price / buy_price
                if pct > 1.0006:
                    self.sell_market(row['id'], symbol, amount)
                elif pct > 1.0005:
                    self.sell_limit(row['id'], symbol, amount, buy_price * 1.0005)
                elif pct > 1.0004:
                    self.sell_limit(row['id'], symbol, amount, buy_price * 1.0004)
                elif time.time() * 1000 - timestamp > 10 * 60 * 1000 and pct > 1.0001:
                    self.sell_limit(row['id'], symbol, amount, buy_price * 1.00005)
                elif time.time() * 1000 - timestamp > 30 * 60 * 1000 and pct >= 0.99999:
                    self.sell_market(row['id'], symbol, amount)
                elif time.time() * 1000 - timestamp > 120 * 60 * 1000:
                    self.sell_market(row['id'], symbol, amount)
                else:
                    continue
                logger.info(f"sell {buy_price} vs {price}")
            except Exception as e:
                logger.info(f"sell error {e}")

    def buy_market(self, symbol, dollar):
        price = self.current_price(symbol)
        if price == 0:
            return
        amount = dollar / price
        buy_json = self.exchange.create_order(symbol, 'market', 'buy', amount)
        value = {
            "status": 2,
            "ext_json": {},
            "symbol": symbol,
            "amount": amount,
            "buy_json": buy_json,
        }
        self.table.upsert(value=value)
        self.update_account()

    def sell_market(self, id, symbol, amount):
        logger.info(f"sell {symbol}")

        try:
            sell_json = self.exchange.create_order(symbol, 'market', 'sell', amount)
        except Exception as e:
            sell_json = {}
        value = {
            "id": id,
            "status": 3,
            "sell_json": sell_json,
        }
        self.table.upsert(value=value)
        self.update_account()

    def sell_limit(self, id, symbol, amount, price):
        logger.info(f"sell {symbol}")

        try:
            sell_json = self.exchange.create_order(symbol, 'limit', 'sell', amount, price=price)
        except Exception as e:
            sell_json = {}
        value = {
            "id": id,
            "status": 3,
            "sell_json": sell_json,
        }
        self.table.upsert(value=value)

    async def watch_symbol(self, symbol='BTC/BUSD', amount=0.0015):
        d = {
            'newUpdates': False,
            'apiKey': read_secret('coin', 'binance', 'api_key'),
            'secretKey': read_secret('coin', 'binance', 'secret_key')
        }
        exchange = ccxtpro.binance(d)
        await exchange.watch_trades(symbol)
        # await exchange.watch_trades('ETC/USDT')

        while True:
            try:
                trades = exchange.trades
                price_map = {}
                for sym in trades.keys():
                    price_map[sym] = float(trades[sym][-1]['info']['p'])
                self.update_account()
                await self.buy_auto(price_map)
                await self.sell_auto(price_map)
                await exchange.sleep(10000)
            except Exception as e:
                print(e)

    def run_job(self):
        run(self.watch_symbol())

# Strategy2Task().run_job()
