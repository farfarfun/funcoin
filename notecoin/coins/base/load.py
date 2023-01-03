import csv
import json
import logging
import time

import ccxt
import pandas as pd
from ccxt.base.exchange import Exchange
from tqdm import tqdm

logger = logging.getLogger()
unix_month = 2678400000
one_hour = 3600 * 1000


class LoadData:
    def __init__(self, exchange: Exchange, csv_path, unix_start, unix_end, *args, **kwargs):
        self.exchange = exchange
        self.csv_path = csv_path
        self.unix_start = unix_start
        self.unix_end = unix_end
        self.cache_data = []

    def load_open(self):
        pass

    def load_all(self, *args, **kwargs):
        self.load_open()
        self.exchange.load_markets()
        pbr = tqdm(self.exchange.symbols)
        for sym in pbr:
            if ':' not in sym:
                pbr.set_description(sym)
                self.load(sym, pbr, *args, **kwargs)
        self.write_data([], False)
        self.load_close()

    def load_close(self):
        pass

    def load(self, symbol, pbr, *args, **kwargs):
        pass

    def write_data(self, data_list, cache=True):
        self.cache_data.extend(data_list)
        if cache and len(self.cache_data) < 10000:
            return
        if len(self.cache_data) == 0:
            return
        df = pd.DataFrame(self.cache_data)
        df = df[(df['timestamp'] >= self.unix_start) & (df['timestamp'] <= self.unix_end)]
        self._write(json.loads(df.to_json(orient='records')))
        self.cache_data.clear()

    def _write(self, data_list):
        pass


class LoadDataKline(LoadData):
    def __init__(self, *args, timeframe='1m', **kwargs):
        super(LoadDataKline, self).__init__(*args, **kwargs)
        self.timeframe = timeframe
        self.csv_file = open(self.csv_path, mode="w")
        self.csv_writer = csv.DictWriter(self.csv_file, delimiter=",",
                                         fieldnames=["symbol", "timestamp", "open", "close", "low", "high", "vol"])
        self.csv_writer.writeheader()

    def _write(self, data_list):
        self.csv_writer.writerows(data_list)
        self.csv_file .flush()

    def load(self, symbol, pbr, *args, **kwargs):
        unix_temp = self.unix_start
        for _ in range(1000):
            if unix_temp >= self.unix_end:
                break
            try:
                result = self.exchange.fetch_ohlcv(symbol, self.timeframe, unix_temp, limit=100)
                result = self.exchange.sort_by(result, 0)
                if len(result) == 0:
                    break
                unix_temp = result[-1][0]
                df = pd.DataFrame(result, columns=['timestamp', 'open', 'close', 'low', 'high', 'vol'])
                df['symbol'] = symbol
                self.write_data(json.loads(df.to_json(orient='records')))
                # time.sleep(int(self.exchange.rateLimit / 1000))
            except Exception as e:
                print(type(e).__name__, str(e))
                self.exchange.sleep(10000)


class LoadTradeKline(LoadData):
    def __init__(self, *args, **kwargs):
        super(LoadTradeKline, self).__init__(*args, **kwargs)
        self.csv_file = open(self.csv_path, mode="w")
        self.csv_writer = csv.DictWriter(self.csv_file, delimiter=",", fieldnames=[
                                         "symbol", "id", "timestamp", "side", "price", "amount"])
        self.csv_writer.writeheader()

    def load_close(self):
        pass

    def _write(self, data_list):
        self.csv_writer.writerows(data_list)
        self.csv_file.flush()

    def load(self, symbol, pbr, *args, **kwargs):
        unix_temp = self.unix_start
        previous_trade_id = None
        for _ in range(10000):
            pbr.set_description(f'{symbol}-{unix_temp}')
            if unix_temp >= self.unix_end:
                break
            try:
                trades = self.exchange.fetch_trades(symbol, unix_temp, limit=1000)
                if len(trades) == 0:
                    unix_temp += one_hour
                    continue
                last_trade = trades[-1]
                if previous_trade_id == last_trade['id']:
                    unix_temp += one_hour
                    continue
                unix_temp = last_trade['timestamp']
                previous_trade_id = last_trade['id']

                result = [{
                    'symbol': trade['symbol'],
                    'id': trade['id'].replace('\n', ''),
                    'timestamp': int(trade['timestamp']),
                    'side': trade['side'][0],
                    'price': trade['price'],
                    'amount': trade['amount'],
                } for trade in trades]

                self.write_data(result)
                #time.sleep(int(self.exchange.rateLimit / 1000))
            except ccxt.NetworkError as e:
                print(type(e).__name__, str(e))
                self.exchange.sleep(1000)
