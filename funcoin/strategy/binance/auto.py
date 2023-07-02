import json
from datetime import datetime, timedelta

import ccxt
import pandas as pd
from funcoin.base.db.mysql import get_engine
from funsecret import read_secret
from noteworker import get_default_app
from sqlalchemy.dialects.mysql import insert
from tqdm import tqdm

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 500)
pd.set_option('max_colwidth', 1000)

app = get_default_app()


def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)


class Strategy:
    def __init__(self):
        self.exchange = ccxt.binance({
            'apiKey': read_secret('coin', 'binance', 'api_key'),
            'secret': read_secret('coin', 'binance', 'secret_key')
        })
        self.engine = get_engine(self.exchange.name)
        # self.exchange.verbose = True
        self.account = None
        self.table_account = f'account'.lower()
        self.table_markets = f'markets'.lower()
        self.table_24h = f'ticket_24h'.lower()
        self.table_ohlcv = f'ohlcv'.lower()

    def refresh_markets(self):
        self.exchange.load_markets()
        df = pd.DataFrame([dict((k, str(v)) for k, v in symbol.items()) for symbol in self.exchange.markets.values()])
        df.to_sql(name=self.table_markets, con=self.engine.connect(), if_exists='replace')

    def refresh_data_24h(self):
        param = {"type": "MINI"}
        df = pd.DataFrame(self.exchange.public_get_ticker_24hr(param))
        df.to_sql(name=self.table_24h, con=self.engine.connect(), if_exists='replace')

    def refresh_account(self):
        data = self.exchange.fetch_balance()
        df = pd.DataFrame([data['total'], data['free'], data['used']]).transpose()
        df.columns = ['total', 'free', 'used']
        self.account = df.sort_values(['total'], ascending=False).reset_index()
        self.account.columns = ['symbol', 'total', 'free', 'used']

        def fun(data):
            if data['symbol'] == 'BUSD':
                return 0
            if data['total'] == 0:
                return 0
            return self.current_price(f"{data['symbol']}/BUSD")

        self.account['price'] = self.account[['symbol', 'total']].apply(fun, axis=1)
        self.account.to_sql(name=self.table_account, con=self.engine.connect(), if_exists='replace')

    def update_ohlcv_once(self, since, symbol, timeframe='1m', limit=500, *args, **kwargs):
        result = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=limit)
        df = pd.DataFrame(result, columns=['timestamp', 'open', 'close', 'low', 'high', 'vol'])
        df['symbol'] = symbol
        df.to_sql(f'{self.table_ohlcv}_{timeframe}', con=self.engine.connect(), if_exists='append',
                  index=False, index_label=['symbol', 'timestamp'], method=insert_on_duplicate)

    def update_ohlcv_batch(self, timeframe='1m', limit=50, *args, **kwargs):
        sql = f"""
        SELECT
            *
        FROM
            (
            SELECT
                t1.symbol,
                CAST(t2.quoteVolume AS DECIMAL) AS quoteVolume
            FROM
                {self.table_markets} t1
            JOIN {self.table_24h} t2 ON
                t1.id = t2.symbol
        ) t
        WHERE
            symbol RLIKE '/busd'
        ORDER BY
            quoteVolume
        DESC
        LIMIT {limit}
        """
        symbols = pd.read_sql(sql, self.engine.connect())['symbol'].values
        t = datetime.now()
        if timeframe == '1m':
            for _ in tqdm(range(4 * 30 * 3)):
                t += timedelta(minutes=-600)
                for symbol in symbols:
                    self.update_ohlcv_once(int(t.timestamp() * 1000), symbol, timeframe, limit=600)

        elif 'h' in timeframe:
            t += timedelta(hours=-500)
            for symbol in tqdm(symbols):
                self.update_ohlcv_once(int(t.timestamp() * 1000), symbol, timeframe, limit=500)
        else:
            t += timedelta(days=-500)
            for symbol in tqdm(symbols):
                self.update_ohlcv_once(int(t.timestamp() * 1000), symbol, timeframe, limit=500)

    def current_price(self, symbol='ETH/USDT'):
        data = self.exchange.fetch_ohlcv(symbol, since=int((datetime.now() + timedelta(minutes=-1)).timestamp() * 1000))
        if len(data) > 0:
            return data[-1][2]
        else:
            return 0

    def buy_market(self, symbol, total=15):

        price = self.current_price(symbol)
        if price == 0:
            return
        amount = total / price
        self.exchange.create_order(symbol, 'market', 'buy', amount)
        self.sell_oco(symbol, amount)

    def sell_oco(self, symbol, amount, total=15):
        market = self.exchange.market(symbol)
        price = total / amount

        high_price = price * 1.03
        stop_price = price * 0.98
        stop_limit_price = price * 0.97

        data = {
            'symbol': market['id'],
            'side': 'SELL',
            'quantity': self.exchange.amount_to_precision(symbol, amount),
            'price': self.exchange.price_to_precision(symbol, high_price),
            'stopPrice': self.exchange.price_to_precision(symbol, stop_price),
            'stopLimitPrice': self.exchange.price_to_precision(symbol, stop_limit_price),
            'stopLimitTimeInForce': 'GTC',  # GTC, FOK, IOC
        }
        return self.exchange.private_post_order_oco(data)

    def auto_oco(self, total2=15):
        for symbol_info in json.loads(self.account.to_json(orient='records')):
            symbol, price, total = symbol_info['symbol'], symbol_info['price'], symbol_info['total']
            if total2 * 0.5 < price * total < total2 * 1.5:
                continue
            if symbol_info['used'] == 0:
                continue
            self.sell_oco(symbol, amount=symbol_info['amount'])

    def save_ohlcv_to_csv(self, filename='data', timeframe='1m', page_size=500000, total_step=1000):
        for step in tqdm(range(total_step)):
            start, stop = page_size * step, page_size * (step + 1)
            sql = f"""
            select * 
            from {self.table_ohlcv}_{timeframe} limit {step * page_size},{page_size}
            """
            df = pd.read_sql(sql, self.engine)

            if len(df) == 0:
                break
            file_path = f'{filename}-{timeframe}.csv'
            if start == 0:
                df.to_csv(file_path, header=True, index=False, mode='w')
            else:
                df.to_csv(file_path, header=False, index=False, mode='a')
