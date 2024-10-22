from datetime import datetime, timedelta

from ccxt import binance
from funsecret import read_secret

from funcoin.coins.base.file import DataFileProperty
from funcoin.task import BaseTask


class LoadTask(BaseTask):
    table_name = "load"

    def __init__(self, *args, **kwargs):
        self.path_root = (
            read_secret(cate1="funcoin", cate2="path", cate3="local", cate4="path_root") or "~/workspace/tmp"
        )
        super(LoadTask, self).__init__(*args, **kwargs)

    @staticmethod
    def parse_day(ds, days=0):
        ds = ds
        first = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=days)
        last = first + timedelta(days=1) - timedelta(seconds=1)
        return first, last

    @staticmethod
    def parse_week(ds, weeks=0):
        ds = ds
        first = datetime.strptime(ds, "%Y-%m-%d") + timedelta(weeks=weeks)
        first = first - timedelta(days=first.weekday())
        first = datetime(first.year, first.month, first.day)
        last = first + timedelta(weeks=1) - timedelta(seconds=1)
        return first, last


class LoadKlineDailyTask(LoadTask):
    def refresh(self, ds):
        start, end = self.parse_day(ds)
        file_pro = DataFileProperty(exchange=binance(), path=self.path_root)
        file_pro.file_format = "%Y%m%d"
        file_pro.change_data_type("kline")
        file_pro.change_timeframe("1m")
        file_pro.change_freq("daily")
        file_pro.load_daily(start, end)


class LoadKlineWeeklyTask(LoadTask):
    def refresh(self, ds):
        start, end = self.parse_week(ds, weeks=-1)
        file_pro = DataFileProperty(exchange=binance(), path=self.path_root)
        file_pro.file_format = "%Y%m%d"
        file_pro.change_data_type("kline")
        file_pro.change_timeframe("1m")
        file_pro.change_freq("weekly")
        print((start, end))
        file_pro.load_merge(start, end)


class LoadTradeDailyTask(LoadTask):
    def refresh(self, ds):
        start, end = self.parse_day(ds)
        file_pro = DataFileProperty(exchange=binance(), path=self.path_root)
        file_pro.file_format = "%Y%m%d"
        file_pro.change_data_type("trade")
        file_pro.change_timeframe("detail")
        file_pro.change_freq("daily")
        file_pro.load_daily(start, end)


# LoadKlineWeeklyTask().refresh('2021-9-14')
