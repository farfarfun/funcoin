import csv
import json
import logging

import ccxt
import pandas as pd
from ccxt.base.exchange import Exchange
from tqdm import tqdm

logger = logging.getLogger("funcoin")
unix_month = 2678400000
one_hour = 3600 * 1000


class BaseLoader:
    def __init__(self, unix_start, unix_end, *args, **kwargs):
        self.unix_start = unix_start
        self.unix_end = unix_end
        self.cache_data = []

    def _open(self, *args, **kwargs):
        pass

    def _write(self, data_list):
        pass

    def _close(self, *args, **kwargs):
        pass

    def _load_symbols(self, *args, **kwargs):
        pass

    def _load_symbol(self, symbol, pbr=None, *args, **kwargs):
        pass

    def load_symbols(self, *args, **kwargs):
        self._open(*args, **kwargs)
        self._load_symbols(*args, **kwargs)
        self._close(*args, **kwargs)

    def load_symbol(self, symbol, pbr=None, *args, **kwargs):
        self._open(*args, **kwargs)
        self._load_symbol(symbol=symbol, pbr=pbr, *args, **kwargs)
        self._close(*args, **kwargs)

    def write_data(self, data_list, cache=True):
        self.cache_data.extend(data_list)
        if cache and len(self.cache_data) < 10000:
            return
        if len(self.cache_data) == 0:
            return
        df = pd.DataFrame(self.cache_data)
        df = df[(df["timestamp"] >= self.unix_start) & (df["timestamp"] <= self.unix_end)]
        self._write(json.loads(df.to_json(orient="records")))
        self.cache_data.clear()

    def __enter__(self):
        self._handle = self
        self._open()
        return self._handle

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cache_data is not None:
            self.write_data([], cache=False)
        self._close()
        return True


class CSVLoader(BaseLoader):
    def __init__(self, csv_path, fieldnames, *args, **kwargs):
        self.csv_path = csv_path
        super().__init__(*args, **kwargs)
        self.csv_file = open(self.csv_path, mode="w")
        self.csv_writer = csv.DictWriter(self.csv_file, delimiter=",", fieldnames=fieldnames)
        self.csv_writer.writeheader()

    def _write(self, data_list):
        self.csv_writer.writerows(data_list)
        self.csv_file.flush()

    def _close(self, *args, **kwargs):
        self.csv_file.close()


class CCXTBaseLoader(CSVLoader):
    def __init__(self, exchange: Exchange, *args, **kwargs):
        self.exchange = exchange
        super().__init__(*args, **kwargs)
        self.exchange.load_markets()

    def _load_symbols(self, *args, **kwargs):
        pbr = tqdm(self.exchange.symbols)
        for sym in pbr:
            if ":" not in sym:
                pbr.set_description(sym)
                self._load_symbol(sym, pbr, *args, **kwargs)
        self.write_data([], False)


class KlineLoder(CCXTBaseLoader):
    def __init__(self, *args, timeframe="1m", **kwargs):
        super(KlineLoder, self).__init__(
            fieldnames=["symbol", "timestamp", "open", "close", "low", "high", "vol"],
            *args,
            **kwargs,
        )
        self.timeframe = timeframe

    def _load_symbol(self, symbol, pbr=None, *args, **kwargs):
        unix_temp = self.unix_start
        for _ in range(1000):
            if unix_temp >= self.unix_end:
                break
            try:
                result = self.exchange.fetch_ohlcv(symbol, self.timeframe, unix_temp, limit=500)
                result = self.exchange.sort_by(result, 0)
                if len(result) == 0:
                    break
                unix_temp = result[-1][0]
                df = pd.DataFrame(result, columns=["timestamp", "open", "close", "low", "high", "vol"])
                df["symbol"] = symbol
                self.write_data(json.loads(df.to_json(orient="records")))
                # time.sleep(int(self.exchange.rateLimit / 1000))
            except Exception as e:
                logger.error(e)
                self.exchange.sleep(1000)


class TradeLoader(CCXTBaseLoader):
    def __init__(self, *args, **kwargs):
        super(TradeLoader, self).__init__(
            fieldnames=["symbol", "id", "timestamp", "side", "price", "amount"],
            *args,
            **kwargs,
        )

    def _load_symbol(self, symbol, pbr=None, *args, **kwargs):
        unix_temp = self.unix_start
        previous_trade_id = None
        for _ in range(10000):
            pbr.set_description(f"{symbol}-{unix_temp}")
            if unix_temp >= self.unix_end:
                break
            try:
                trades = self.exchange.fetch_trades(symbol, unix_temp, limit=1000)
                if len(trades) == 0:
                    unix_temp += one_hour
                    continue
                last_trade = trades[-1]
                if previous_trade_id == last_trade["id"]:
                    unix_temp += one_hour
                    continue
                unix_temp = last_trade["timestamp"]
                previous_trade_id = last_trade["id"]

                result = [
                    {
                        "symbol": trade["symbol"],
                        "id": trade["id"].replace("\n", ""),
                        "timestamp": int(trade["timestamp"]),
                        "side": trade["side"][0],
                        "price": trade["price"],
                        "amount": trade["amount"],
                    }
                    for trade in trades
                ]

                self.write_data(result)
                # time.sleep(int(self.exchange.rateLimit / 1000))
            except ccxt.NetworkError as e:
                logger.error(e)
                self.exchange.sleep(1000)
