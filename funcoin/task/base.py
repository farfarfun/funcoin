from datetime import datetime, timedelta

import ccxt
import pandas as pd
from funcoin.base.db import BaseTable
from funsecret import read_secret
from sqlalchemy import DOUBLE


class BaseTask(BaseTable):
    def __init__(self, exchange=None, *args, **kwargs):
        self.exchange = exchange or ccxt.binance(
            {
                "apiKey": read_secret("coin", "binance", "api_key"),
                "secret": read_secret("coin", "binance", "secret_key"),
            }
        )
        super().__init__(MyTable=None, *args, **kwargs)
        self.table_name = "base"

    def load(self):
        return pd.read_sql(sql=f"select * from {self.table_name}", con=self.engine.connect())

    def current_price(self, symbol="ETH/USDT"):
        data = self.exchange.fetch_ohlcv(symbol, since=int((datetime.now() + timedelta(minutes=-1)).timestamp() * 1000))
        if len(data) > 0:
            return data[-1][2]
        else:
            return 0


class AccountTask(BaseTask):
    table_name = "account"

    def __init__(self, *args, **kwargs):
        super(AccountTask, self).__init__(*args, **kwargs)

    def refresh(self, conn=None):
        data = self.exchange.fetch_balance()
        df = pd.DataFrame([data["total"], data["free"], data["used"]]).transpose()
        df.columns = ["total", "free", "used"]
        account = df.sort_values(["total"], ascending=False).reset_index()
        account.columns = ["symbol", "total", "free", "used"]

        def fun(data):
            if data["symbol"] == "BUSD":
                return 0
            if data["total"] == 0:
                return 0
            return self.current_price(f"{data['symbol']}/BUSD")

        account["price"] = account[["symbol", "total"]].apply(fun, axis=1)
        account.to_sql(name=self.table_name, con=conn or self.engine.connect(), if_exists="replace")


class MarketTask(BaseTask):
    table_name = "market"

    def __init__(self, *args, **kwargs):
        super(MarketTask, self).__init__(*args, **kwargs)

    def refresh(self, conn=None):
        self.exchange.load_markets()
        df = pd.DataFrame([dict((k, str(v)) for k, v in symbol.items()) for symbol in self.exchange.markets.values()])
        df.to_sql(name=self.table_name, con=conn or self.engine.connect(), if_exists="replace")


class Ticker24HTask(BaseTask):
    table_name = "ticker24h"

    def __init__(self, *args, **kwargs):
        super(Ticker24HTask, self).__init__(*args, **kwargs)

    def refresh(self, conn=None):
        param = {"type": "MINI"}
        df = pd.DataFrame(self.exchange.public_get_ticker_24hr(param))
        dtype = {"volume": DOUBLE, "quoteVolume": DOUBLE}
        df.to_sql(name=self.table_name, con=conn or self.engine.connect(), if_exists="replace", dtype=dtype)
