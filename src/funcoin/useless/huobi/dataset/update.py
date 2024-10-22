from funcoin.huobi.client import GenericClient, MarketClient
from funcoin.huobi.dataset.core import *
from funcoin.huobi.history.core import HistoryDownload
from funtool.tool.secret import read_secret
from tqdm import tqdm


class TradeUpdateHistory:
    def __init__(self, db_path="/root/workspace/tmp/coin/huobi.db", date="20210101"):
        db_path = db_path.replace(".db", f"-{date}.db")
        api_key = read_secret(cate1="coin", cate2="huobi", cate3="api_key")
        secret_key = read_secret(cate1="coin", cate2="huobi", cate3="secret_key")
        self.market = MarketClient(api_key=api_key, secret_key=secret_key)
        self.generic = GenericClient(api_key=api_key, secret_key=secret_key)

        self.symbolDB = SymbolInfo(db_path=db_path)
        self.tradeDB = TradeDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1min = Kline1MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_5min = Kline5MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_15min = Kline15MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_30min = Kline30MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_60min = Kline60MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_4hour = Kline4HourDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1day = Kline1DayDetail(db_path=db_path, conn=self.symbolDB.conn)

        self.init()

    def init(self):
        self.symbolDB.create()
        self.tradeDB.create()
        self.kline_1min.create()
        self.kline_5min.create()
        self.kline_15min.create()
        self.kline_30min.create()
        self.kline_60min.create()
        self.kline_4hour.create()
        self.kline_1day.create()

    @staticmethod
    def replace_key(data_dicts: list, symbol=None):
        res = []
        for _data_dict in data_dicts:
            data_dict = {"symbol": symbol}
            for key in _data_dict.keys():
                data_dict[key.replace("-", "_")] = _data_dict[key]
            res.append(data_dict)
        return res

    def insert_symbol(self):
        self.symbolDB.insert_list(self.replace_key(self.generic.get_exchange_symbols().data))

    def insert_trade(self, symbol):
        history = self.market.get_history_trade(symbol=symbol, size=2000)
        datas = history.dict_data["data"]
        result = []
        [result.extend(data["data"]) for data in datas]
        self.tradeDB.insert_list(self.replace_key(result, symbol=symbol))

    def insert_kline(self, symbol):
        history = HistoryDownload(
            data_type="klines",
            start_date=datetime.datetime(2021, 5, 21),
            end_date=datetime.datetime(2021, 5, 23),
            all_period=["1min", "5min", "15min", "30min", "60min", "4hour", "1day"],
            all_symbols=[symbol],
            download_dir="/root/workspace/tmp/data",
        )
        history.b_download()

        self.kline_1min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_5min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="5min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_15min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="15min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_30min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="30min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_60min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="60min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_4hour.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="4hour", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_1day.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1day", size=2000).dict_data["data"], symbol
            )
        )

    def auto_save(self):
        self.symbolDB.auto_save()
        self.tradeDB.auto_save()
        self.kline_1min.auto_save()
        self.kline_5min.auto_save()
        self.kline_15min.auto_save()
        self.kline_30min.auto_save()
        self.kline_60min.auto_save()
        self.kline_4hour.auto_save()
        self.kline_1day.auto_save()
        self.kline_1mon.auto_save()
        self.kline_1week.auto_save()
        self.kline_1year.auto_save()

    def insert_data(self, trade=True, kline=True):
        symbols = self.symbolDB.select("select symbol from {} where state ='online'".format(self.symbolDB.table_name))
        for symbol in tqdm(symbols):
            try:
                symbol = symbol["symbol"]
                if trade:
                    self.insert_trade(symbol)
                if kline:
                    self.insert_kline(symbol)
                time.sleep(0.3)
            except Exception as e:
                print(e)

        print(self.symbolDB.select("select count(1) as num from " + self.symbolDB.table_name))
        print(self.tradeDB.select("select count(1) as num from " + self.tradeDB.table_name))
        print("1min\t{}".format(self.kline_1min.select("select count(1) as num from " + self.kline_1min.table_name)))
        print("5min\t{}".format(self.kline_5min.select("select count(1) as num from " + self.kline_5min.table_name)))
        print("15min\t{}".format(self.kline_15min.select("select count(1) as num from " + self.kline_15min.table_name)))
        print("30min\t{}".format(self.kline_30min.select("select count(1) as num from " + self.kline_30min.table_name)))
        print("60min\t{}".format(self.kline_60min.select("select count(1) as num from " + self.kline_60min.table_name)))
        print("4hour\t{}".format(self.kline_4hour.select("select count(1) as num from " + self.kline_4hour.table_name)))
        print("1day\t{}".format(self.kline_1day.select("select count(1) as num from " + self.kline_1day.table_name)))
        print("1week\t{}".format(self.kline_1week.select("select count(1) as num from " + self.kline_1week.table_name)))
        print("1mon\t{}".format(self.kline_1mon.select("select count(1) as num from " + self.kline_1mon.table_name)))
        print("1year\t{}".format(self.kline_1year.select("select count(1) as num from " + self.kline_1year.table_name)))

    def run(self):
        t = 0

        while True:
            self.insert_symbol()
            if t % 100 == 0:
                self.insert_data(trade=False, kline=True)
                self.auto_save()
            else:
                self.insert_data(trade=False, kline=False)
                time.sleep(120)
            t += 1

    def test(self):
        import pandas as pd

        condition = "id>=1619400078439 and id<=1619452799999 limit 111"
        sql = "select * from {} where {}".format(self.kline_1day.table_name, condition)
        print(sql)
        result = pd.read_sql(sql=sql, con=self.kline_1day.conn)
        print(result.head(10))


class TradeUpdate:
    def __init__(self, db_path="/root/workspace/tmp/coin/huobi.db"):
        api_key = read_secret(cate1="coin", cate2="huobi", cate3="api_key")
        secret_key = read_secret(cate1="coin", cate2="huobi", cate3="secret_key")
        self.market = MarketClient(api_key=api_key, secret_key=secret_key)
        self.generic = GenericClient(api_key=api_key, secret_key=secret_key)

        self.symbolDB = SymbolInfo(db_path=db_path)
        self.tradeDB = TradeDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1min = Kline1MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_5min = Kline5MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_15min = Kline15MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_30min = Kline30MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_60min = Kline60MinDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_4hour = Kline4HourDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1day = Kline1DayDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1mon = Kline1MonDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1week = Kline1WeekDetail(db_path=db_path, conn=self.symbolDB.conn)
        self.kline_1year = Kline1YearDetail(db_path=db_path, conn=self.symbolDB.conn)

        self.init()

    def init(self):
        self.symbolDB.create()
        self.tradeDB.create()
        self.kline_1min.create()
        self.kline_5min.create()
        self.kline_15min.create()
        self.kline_30min.create()
        self.kline_60min.create()
        self.kline_4hour.create()
        self.kline_1day.create()
        self.kline_1mon.create()
        self.kline_1week.create()
        self.kline_1year.create()

    @staticmethod
    def replace_key(data_dicts: list, symbol=None):
        res = []
        for _data_dict in data_dicts:
            data_dict = {"symbol": symbol}
            for key in _data_dict.keys():
                data_dict[key.replace("-", "_")] = _data_dict[key]
            res.append(data_dict)
        return res

    def insert_symbol(self):
        self.symbolDB.insert_list(self.replace_key(self.generic.get_exchange_symbols().data))

    def insert_trade(self, symbol):
        history = self.market.get_history_trade(symbol=symbol, size=2000)
        datas = history.dict_data["data"]
        result = []
        [result.extend(data["data"]) for data in datas]
        self.tradeDB.insert_list(self.replace_key(result, symbol=symbol))

    def insert_kline(self, symbol):
        self.kline_1min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_5min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="5min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_15min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="15min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_30min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="30min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_60min.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="60min", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_4hour.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="4hour", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_1day.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1day", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_1mon.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1mon", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_1week.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1week", size=2000).dict_data["data"], symbol
            )
        )
        self.kline_1year.insert_list(
            self.replace_key(
                self.market.get_candlestick(symbol=symbol, period="1year", size=2000).dict_data["data"], symbol
            )
        )

    def auto_save(self):
        self.symbolDB.auto_save()
        self.tradeDB.auto_save()
        self.kline_1min.auto_save()
        self.kline_5min.auto_save()
        self.kline_15min.auto_save()
        self.kline_30min.auto_save()
        self.kline_60min.auto_save()
        self.kline_4hour.auto_save()
        self.kline_1day.auto_save()
        self.kline_1mon.auto_save()
        self.kline_1week.auto_save()
        self.kline_1year.auto_save()

    def insert_data(self, trade=True, kline=True):
        symbols = self.symbolDB.select("select symbol from {} where state ='online'".format(self.symbolDB.table_name))
        for symbol in tqdm(symbols):
            try:
                symbol = symbol["symbol"]
                if trade:
                    self.insert_trade(symbol)
                if kline:
                    self.insert_kline(symbol)
                time.sleep(0.3)
            except Exception as e:
                print(e)

        print(self.symbolDB.select("select count(1) as num from " + self.symbolDB.table_name))
        print(self.tradeDB.select("select count(1) as num from " + self.tradeDB.table_name))
        print("1min\t{}".format(self.kline_1min.select("select count(1) as num from " + self.kline_1min.table_name)))
        print("5min\t{}".format(self.kline_5min.select("select count(1) as num from " + self.kline_5min.table_name)))
        print("15min\t{}".format(self.kline_15min.select("select count(1) as num from " + self.kline_15min.table_name)))
        print("30min\t{}".format(self.kline_30min.select("select count(1) as num from " + self.kline_30min.table_name)))
        print("60min\t{}".format(self.kline_60min.select("select count(1) as num from " + self.kline_60min.table_name)))
        print("4hour\t{}".format(self.kline_4hour.select("select count(1) as num from " + self.kline_4hour.table_name)))
        print("1day\t{}".format(self.kline_1day.select("select count(1) as num from " + self.kline_1day.table_name)))
        print("1week\t{}".format(self.kline_1week.select("select count(1) as num from " + self.kline_1week.table_name)))
        print("1mon\t{}".format(self.kline_1mon.select("select count(1) as num from " + self.kline_1mon.table_name)))
        print("1year\t{}".format(self.kline_1year.select("select count(1) as num from " + self.kline_1year.table_name)))

    def run(self):
        t = 0

        while True:
            self.insert_symbol()
            if t % 100 == 0:
                self.insert_data(trade=False, kline=True)
                self.auto_save()
            else:
                self.insert_data(trade=False, kline=False)
                time.sleep(120)
            t += 1

    def test(self):
        import pandas as pd

        condition = "id>=1619400078439 and id<=1619452799999 limit 111"
        sql = "select * from {} where {}".format(self.kline_1day.table_name, condition)
        print(sql)
        result = pd.read_sql(sql=sql, con=self.kline_1day.conn)
        print(result.head(10))


update = TradeUpdate()
update.run()
# update.test()

# ps -elf|grep update
# nohup /root/anaconda3/bin/python3.8 /root/workspace/fundata/funcoin/funcoin/huobi/dataset/update.py >>/root/workspace/tmp/funcoin-run-$(date +%Y-%m-%d).log 2>&1 &
# 2509
# sqlite3 -header -csv huobi.db "select * from trade_detail;" > trade_detail.csv
