import logging
import os
from datetime import datetime, timedelta

import ccxt
from ccxt import Exchange
from funcoin.coins.base.load import LoadDataKline
from fundrive import LanZouDrive
from fundrive.core.table import DriveTable
from funfile.compress import tarfile
from funsecret import read_secret

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


class FileProperty:
    def __init__(self, exchange: Exchange, data_type="kline", timeframe="1m"):
        self.exchange = exchange
        self.data_type = data_type
        self.timeframe = timeframe
        self.exchange_name = exchange.name.lower()

        self.freq = None
        self.end_date = None
        self.start_date = None
        self.par_format = None
        self.file_format = None

    def daily(self, ds):
        self.freq = "daily"
        self.file_format = "%Y%m%d"
        self.par_format = "%Y%m"
        day = datetime.strptime(ds, "%Y%m%d")
        self.start_date = day
        self.end_date = day + timedelta(days=1)
        return self

    @property
    def partition(self):
        return self.start_date.strftime(self.par_format)

    @property
    def filename_prefix(self):
        return f"{self.exchange_name}_{self.data_type}_{self.freq}_{self.timeframe}-{self.partition}"

    @property
    def file_path_csv(self):
        return f"{self.filename_prefix}.csv"

    @property
    def file_path_tar(self):
        return f"{self.filename_prefix}.tar"


class LoadTask:
    def __init__(self, table: DriveTable):
        self.table = table

    def download_kline(self, file_pro: FileProperty) -> bool:
        logger.info(f"download for {file_pro.file_path_tar}")
        exchan = LoadDataKline(
            file_pro.exchange,
            unix_start=int(file_pro.start_date.timestamp() * 1000),
            unix_end=int(file_pro.end_date.timestamp() * 1000),
            csv_path=file_pro.file_path_csv,
            timeframe=file_pro.timeframe,
        )
        # 下载
        exchan.load_all()
        # 压缩
        with tarfile.open(file_pro.file_path_tar, "w|xz") as tar:
            tar.add(file_pro.file_path_csv)
        self.table.upload(file=file_pro.file_path_tar, partition=file_pro.partition)

        # 删除
        if os.path.exists(file_pro.file_path_csv):
            os.remove(file_pro.file_path_csv)
        return True

    def run(self, days=365):
        start_day = datetime.now() - timedelta(days=-1)
        file_pro = FileProperty(
            ccxt.binance(
                {
                    "apiKey": read_secret("coin", "binance", "api_key"),
                    "secret": read_secret("coin", "binance", "secret_key"),
                }
            )
        ).daily(
            "20240203"
        )  # .daily(start_day.strftime("%Y%m%d"))

        # self.table.upload(file=file_pro.file_path_tar, partition=file_pro.partition)
        exists_data = self.table.partition_meta()
        print(exists_data)

        for i in range(days):
            print(start_day.strftime("%Y%m%d"))
            start_day += timedelta(days=-1)
            file_pro.daily("20240203")
            if file_pro.file_path_tar in exists_data.keys():
                continue


drive = LanZouDrive()
drive.login()

task = LoadTask(DriveTable("10677308", drive))
task.run()
