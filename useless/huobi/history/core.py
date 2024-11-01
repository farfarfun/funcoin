"""

"""
import json
import zipfile
from datetime import datetime

import pandas as pd
from funcoin.huobi.dataset.core import (
    Kline1DayDetail,
    Kline1MinDetail,
    Kline4HourDetail,
    Kline5MinDetail,
    Kline15MinDetail,
    Kline30MinDetail,
    Kline60MinDetail,
    KlineDetail,
    TradeDetail,
)
from funcoin.huobi.history.utils import *
from funtool.log import log, logger
from funtool.path import list_file, removedirs
from tqdm import tqdm

_SWAP = "swap"
_SPOT = "spot"
_FUTURE = "future"
_OPTION = "option"
_LINEARSWAP = "linear-swap"
ALL_TYPES = ["future", "spot", "swap", "option", "linear-swap"]
ALL_DATA_TYPES = ["klines", "trades"]
ALL_PERIODS = ["1min", "5min", "15min", "30min", "60min", "4hour", "1day"]
ALL_FREQ = ["daily"]
PRE_URL = "https://futures.huobi.com/data"

logger = log()


class Response:
    def __init__(self, status: bool = True, data: dict = None):
        self.status = status or True
        self.data = data or {}

    def success(self):
        self.status = True

    def error(self):
        self.status = False

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def extend(self, data: dict = None):
        if isinstance(data, dict):
            self.data.update(data)


class HistoryDownload:
    def __init__(
        self,
        pre_url=None,
        data_type: str = None,
        all_periods: list = None,
        _type: str = None,
        freq: str = None,
        start_date=None,
        end_date=None,
        all_symbols=None,
        download_dir=None,
    ):
        self.freq = freq or ALL_FREQ[0]
        self.type = _type or ALL_TYPES[0]
        self.pre_url = pre_url or PRE_URL
        self.data_type = data_type or ALL_DATA_TYPES[0]

        self.all_symbols = all_symbols
        self.download_dir = download_dir or "./data"
        self.all_periods = all_periods or ALL_PERIODS
        self.start_date = start_date or datetime(2017, 10, 27)
        self.end_date = end_date or datetime(2021, 7, 27)

    @staticmethod
    def _file_unzip(file_path) -> Response:
        """unzip zip file"""
        response = Response(True)
        zip_file = zipfile.ZipFile(file_path)
        file_dir = os.path.split(file_path)[0]
        for name in zip_file.namelist():
            zip_file.extract(name, path=file_dir)
            response.put("file_path", os.path.join(file_dir, name))
        zip_file.close()

        return response

    @staticmethod
    def _http_download(url: str, download_dir) -> Response:
        response = Response()
        try:
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            if url is None:
                return Response(False, {"msg": "url is null"})
            data = requests.get(url, allow_redirects=True)
            file_name = os.path.basename(url)
            file_path = os.path.join(download_dir, file_name)
            if len(data.content) > 30:
                with open(file_path, "wb") as f:
                    f.write(data.content)
                response.put("file_path", file_path)
            else:
                response.error()
                response.put("file_path", "File not exists.")

        except Exception as e:
            response.put("download error", str(e))

        return response

    def init_parameters(self):
        print(self.type)
        if self.all_symbols is None:
            ok, all_symbols = False, []
            if self.type == _SPOT:
                ok, all_symbols = get_all_spot_symbols()
            elif self.type == _FUTURE:
                ok, all_symbols = get_all_future_symbols()
            elif self.type == _SWAP:
                ok, all_symbols = get_all_swap_symbols()
            elif self.type == _OPTION:
                ok, all_symbols = get_all_option_symbols()
            elif self.type == _LINEARSWAP:
                ok, all_symbols = get_all_linearswap_symbols()

            if not ok:
                logger.warning(all_symbols)

                return
            self.all_symbols = all_symbols

    def download_symbol_period_day(
        self, path_url, symbol, period, current, delete_zip=True, delete_check=True, *args, **kwargs
    ) -> Response:
        url = f"{path_url}/{symbol.upper()}-{period}-{current.year}-{current.month:02}-{current.day:02}"
        download_dir = os.path.join(self.download_dir, self.data_type, self.type, period)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        zip_file = f"{url}.zip"
        check_file = f"{url}.CHECKSUM"
        response1 = self._http_download(zip_file, download_dir)
        response2 = self._http_download(check_file, download_dir)
        response = Response(response1.status)

        response.extend(
            {
                "zip_url": zip_file,
                "zip_file": response1.get("file_path"),
                "check_url": check_file,
                "check_file": response2.get("file_path"),
            }
        )

        if response1.status:
            response3 = self._file_unzip(response1.get("file_path"))
            response3.put("file", response3.get("file_path"))
            if delete_zip:
                os.remove(response.get("zip_file"))
            if delete_check:
                os.remove(response.get("check_file"))

        return response

    def download_symbol_period(self, symbol, period, start_date=None, end_date=None, *args, **kwargs) -> tuple:
        if period in [
            "trades",
        ]:
            path_url = f"{self.pre_url}/{self.data_type}/{self.type}/{self.freq}/{symbol}"
        else:
            path_url = f"{self.pre_url}/{self.data_type}/{self.type}/{self.freq}/{symbol}/{period}"
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        all_res = []
        interval = end_date - start_date + timedelta(days=1)
        if interval.days > 10:
            for index in tqdm(range(interval.days), desc=f"{symbol}"):
                current = start_date + timedelta(days=index)
                all_res.append(self.download_symbol_period_day(path_url, symbol, period, current, *args, **kwargs))
        else:
            for index in range(interval.days):
                current = start_date + timedelta(days=index)
                all_res.append(self.download_symbol_period_day(path_url, symbol, period, current, *args, **kwargs))
        all_oks = [response for response in all_res if response.status]
        all_errs = [response for response in all_res if not response.status]
        return all_oks, all_errs

    def download_symbols(self, all_symbols=None, all_periods=None, start_date=None, end_date=None, *args, **kwargs):
        """return date is: [start, end)"""
        self.init_parameters()
        all_symbols = all_symbols or self.all_symbols
        all_periods = all_periods or self.all_periods
        if len(all_symbols) > 10:
            for symbol in tqdm(all_symbols, desc="download symbols"):
                for period in all_periods:
                    all_oks, all_errs = self.download_symbol_period(
                        symbol, period, start_date, end_date, *args, **kwargs
                    )
                    logger.warning(f"success:{all_oks}")
                    logger.warning(f"failed:{all_errs}")
        else:
            for symbol in all_symbols:
                for period in all_periods:
                    all_oks, all_errs = self.download_symbol_period(
                        symbol, period, start_date, end_date, *args, **kwargs
                    )
                    logger.warning(f"success:{all_oks}")
                    logger.warning(f"failed:{all_errs}")
        logger.info("done")

    def download_klines(self, _type=None, *args, **kwargs):
        """return date is: [start, end)"""
        self.data_type = ALL_DATA_TYPES[0]
        self.type = _type or ALL_TYPES[0]
        self.download_symbols(*args, **kwargs)

    def download_trades(self, _type=None, *args, **kwargs):
        self.type = _type or ALL_TYPES[0]
        self.data_type = ALL_DATA_TYPES[1]
        self.all_periods = ["trades"]
        self.download_symbols(*args, **kwargs)


class SymbolHistory:
    def __init__(self, dir_path="/root/workspace/tmp/coin/", symbol="SHIBUSDT"):
        self.db_path = os.path.join(dir_path, "dbs", f"huobi-{symbol}.db")

        self.file_dir = os.path.join(dir_path, "files")

        self.symbol = symbol
        self.tradeDB = TradeDetail(db_path=self.db_path)
        self.kline_1min = Kline1MinDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.kline_5min = Kline5MinDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.kline_15min = Kline15MinDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.kline_30min = Kline30MinDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.kline_60min = Kline60MinDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.kline_4hour = Kline4HourDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.kline_1day = Kline1DayDetail(db_path=self.db_path, conn=self.tradeDB.conn)
        self.init()

    def init(self):
        self.tradeDB.create()
        self.kline_1min.create()
        self.kline_5min.create()
        self.kline_15min.create()
        self.kline_30min.create()
        self.kline_60min.create()
        self.kline_4hour.create()
        self.kline_1day.create()

    def insert_trades(self, start_date, end_date):
        pass

    def insert_kline(self, kline_db: KlineDetail, file_path):
        try:
            df = pd.read_csv(file_path, index_col=None, header=None)
            df.columns = ["id", "open", "close", "high", "low", "vol", "amount"]
            df["symbol"] = self.symbol
            kline_db.insert_list(json.loads(df.to_json(orient="records")))
        except Exception as e:
            logger.info(f"error file {file_path}")
            print(e)

    def insert_klines(self, start_date, end_date):
        self.insert_kline(
            self.kline_1min,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="1min"),
        )
        self.insert_kline(
            self.kline_5min,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="5min"),
        )
        self.insert_kline(
            self.kline_15min,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="15min"),
        )
        self.insert_kline(
            self.kline_30min,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="30min"),
        )
        self.insert_kline(
            self.kline_60min,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="60min"),
        )
        self.insert_kline(
            self.kline_4hour,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="4hour"),
        )
        self.insert_kline(
            self.kline_1day,
            load_symbol_all(symbol=self.symbol, start_date=start_date, end_date=end_date, period="1day"),
        )

    def insert_data(self, start_date, end_date):
        self.insert_trades(start_date, end_date)
        self.insert_klines(start_date, end_date)

        print(self.tradeDB.select("select count(1) as num from " + self.tradeDB.table_name))
        print("1min\t{}".format(self.kline_1min.select("select count(1) as num from " + self.kline_1min.table_name)))
        print("5min\t{}".format(self.kline_5min.select("select count(1) as num from " + self.kline_5min.table_name)))
        print("15min\t{}".format(self.kline_15min.select("select count(1) as num from " + self.kline_15min.table_name)))
        print("30min\t{}".format(self.kline_30min.select("select count(1) as num from " + self.kline_30min.table_name)))
        print("60min\t{}".format(self.kline_60min.select("select count(1) as num from " + self.kline_60min.table_name)))
        print("4hour\t{}".format(self.kline_4hour.select("select count(1) as num from " + self.kline_4hour.table_name)))
        print("1day\t{}".format(self.kline_1day.select("select count(1) as num from " + self.kline_1day.table_name)))

    def run(self, start_date=datetime(2021, 5, 25), end_date=datetime(2021, 6, 23)):
        self.insert_data(start_date, end_date)

    def test(self):
        import pandas as pd

        condition = "id>=1619400078439 and id<=1619452799999 limit 111"
        sql = "select * from {} where {}".format(self.kline_1day.table_name, condition)
        print(sql)
        result = pd.read_sql(sql=sql, con=self.kline_1day.conn)
        print(result.head(10))


def load_daily_all(
    period="1min",
    data_type=ALL_DATA_TYPES[0],
    _type=ALL_TYPES[0],
    date=datetime(2021, 5, 21),
    download_dir="/root/workspace/tmp/coin/daily",
    delete_file=True,
    overwrite=False,
):
    download_dir2 = os.path.join(download_dir, date.strftime("%Y%m%d"))
    history = HistoryDownload(
        all_symbols=None,
        data_type=data_type,
        all_periods=[period],
        _type=_type,
        start_date=date,
        end_date=date,
        download_dir=download_dir2,
    )
    file_path = os.path.join(download_dir, f'{history.data_type}_{history.type}_{period}-{date.strftime("%Y%m%d")}.csv')
    if os.path.exists(file_path) and not overwrite:
        return file_path
    if history.data_type == "trade":
        history.download_trades()
    else:
        history.download_klines()

    with open(file_path, "w") as new_file:
        for item in list_file(download_dir2, deep=10):
            symbol = os.path.basename(item).split("-")[0]
            for txt in open(item, "r"):
                txt.replace("\n", f"\n{symbol},")
                new_file.write(f"{symbol},{txt}")

    if delete_file:
        removedirs(download_dir2)
    return file_path


def load_symbol_all(
    symbol="SHIBUSDT",
    period="1min",
    _type=ALL_TYPES[0],
    data_type=ALL_DATA_TYPES[0],
    start_date=datetime(2021, 5, 1),
    end_date=datetime(2021, 5, 23),
    download_dir="/root/workspace/tmp/coin/symbol",
    delete_file=True,
):
    download_dir2 = os.path.join(download_dir, symbol)
    history = HistoryDownload(
        all_symbols=[symbol],
        data_type=data_type,
        all_periods=[period],
        start_date=start_date,
        end_date=end_date,
        _type=_type,
        download_dir=download_dir2,
    )
    history.download_klines()
    file_path = os.path.join(download_dir, f"{history.data_type}_{history.type}_{period}-{symbol}.csv")
    with open(file_path, "w") as new_file:
        for item in list_file(download_dir2, deep=10):
            for txt in open(item, "r"):
                new_file.write(txt)

    if delete_file:
        removedirs(download_dir2)
    return file_path


def load_symbol_all_to_db(symbol="SHIBUSDT", start_date=datetime(2021, 5, 1), end_date=datetime(2021, 5, 3)):
    history = SymbolHistory(symbol=symbol)
    history.run(start_date=start_date, end_date=end_date)
    return history.db_path
