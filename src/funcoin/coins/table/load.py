import logging
import os
from datetime import datetime, timedelta

import ccxt
from fundrive.core.table import DriveTable
from funfile.compress import tarfile

from funcoin.coins.base.loader import BaseLoader, KlineLoder, TradeLoader

logging.getLogger("fundrive").setLevel(logging.INFO)
logger = logging.getLogger("funcoin")
logger.setLevel(logging.INFO)


class FileProperty:
    def __init__(self, exchange_name, data_type="kline", timeframe="1m"):
        self.data_type = data_type
        self.timeframe = timeframe
        self.exchange_name = exchange_name

        self.freq = None
        self.end_date = None
        self.start_date = None
        self.par_format = None
        self.file_format = None

    def daily(self, ds):
        self.freq = "daily"
        self.file_format = self.file_format or "%Y%m%d"
        self.par_format = self.par_format or "%Y%m"
        self.start_date = datetime.strptime(ds, "%Y%m%d")
        self.end_date = self.start_date + timedelta(days=1)
        return self

    @property
    def partition(self):
        return self.start_date.strftime(self.par_format)

    @property
    def filename_prefix(self):
        return f"{self.exchange_name}_{self.data_type}_{self.freq}_{self.timeframe}-{self.start_date.strftime(self.file_format)}"

    @property
    def file_path_csv(self):
        return f"{self.filename_prefix}.csv"

    @property
    def file_path_tar(self):
        return f"{self.filename_prefix}.tar"


class LoadTask:
    def __init__(
        self,
        table: DriveTable,
        exchange: ccxt.Exchange,
    ):
        self.table = table
        self.exchange = exchange

    def download(self, loader: BaseLoader, file_pro: FileProperty) -> bool:
        logger.info(f"download for {file_pro.file_path_tar}")
        # 下载
        loader.load_symbols()
        # 压缩
        with tarfile.open(file_pro.file_path_tar, "w|xz") as tar:
            tar.add(file_pro.file_path_csv)
        self.table.upload(file=file_pro.file_path_tar, partition=file_pro.partition, overwrite=True)

        # 删除
        if os.path.exists(file_pro.file_path_csv):
            os.remove(file_pro.file_path_csv)
        if os.path.exists(file_pro.file_path_tar):
            os.remove(file_pro.file_path_tar)
        return True

    def download_kline(self, file_pro: FileProperty) -> bool:
        loader = KlineLoder(
            self.exchange,
            unix_start=int(file_pro.start_date.timestamp() * 1000),
            unix_end=int(file_pro.end_date.timestamp() * 1000),
            csv_path=file_pro.file_path_csv,
            timeframe=file_pro.timeframe,
        )
        return self.download(loader, file_pro)

    def download_trade(self, file_pro: FileProperty) -> bool:
        loader = TradeLoader(
            self.exchange,
            unix_start=int(file_pro.start_date.timestamp() * 1000),
            unix_end=int(file_pro.end_date.timestamp() * 1000),
            csv_path=file_pro.file_path_csv,
            timeframe=file_pro.timeframe,
        )
        return self.download(loader, file_pro)

    def run(self, days=365):
        self.table.update_partition_dict()
        self.table.update_partition_meta(refresh=True)

        start_day = datetime.now() - timedelta(days=1)
        file_pro = FileProperty(self.exchange.name.lower()).daily(start_day.strftime("%Y%m%d"))
        exists_data = dict([file["name"], file] for file in self.table.partition_meta())

        for i in range(days):
            start_day += timedelta(days=-1)
            file_pro.daily(start_day.strftime("%Y%m%d"))
            if file_pro.file_path_tar in exists_data.keys():
                logger.info(f"{file_pro.file_path_tar} exists, skip.")
                continue
            self.download_kline(file_pro)
