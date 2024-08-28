import logging
import os
from datetime import datetime, timedelta

import ccxt
from ccxt import Exchange
from funcoin.coins.base.load import LoadDataKline
from fundrive.core import BaseDrive
from fundrive.core.table import DriveTable
from fundrive.drives.webdav.drive import WebDavDrive
from funfile.compress import tarfile
from funsecret import read_secret

logger = logging.getLogger(__file__)


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
    def table_name(self):
        return f"{self.exchange_name}_{self.data_type}_{self.freq}_{self.timeframe}"

    @property
    def filename_prefix(self):
        return f"{self.table_name}-{self.start_date.strftime(self.file_format)}"

    @property
    def file_path_csv(self):
        return f"{self.filename_prefix}.csv"

    @property
    def file_path_tar(self):
        return f"{self.filename_prefix}.tar"

    def upload(self, drive: DriveTable):
        path = drive.file_path(
            partition=self.start_date.strftime(self.file_format),
            filename=self.file_path_tar,
        )
        drive.drive.upload_file(self.file_path_tar, path)

    def exists(self, drive: DriveTable):
        path = drive.file_path(
            partition=self.start_date.strftime(self.file_format),
            filename=self.file_path_tar,
        )
        return drive.drive.exist(path)


class LoadTask:
    def __init__(self, drive: BaseDrive):
        self.drive = drive

    def download_kline(self, file_pro: FileProperty) -> bool:
        table = DriveTable(
            db_name="funcoin", table_name=file_pro.table_name, drive=self.drive
        )
        if file_pro.exists(table):
            return False

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
        file_pro.upload(table)
        # 删除
        if os.path.exists(file_pro.file_path_csv):
            os.remove(file_pro.file_path_csv)
        return True


file = FileProperty(
    ccxt.binance(
        {
            "apiKey": read_secret("coin", "binance", "api_key"),
            "secret": read_secret("coin", "binance", "secret_key"),
        }
    )
).daily("20240203")
drive = WebDavDrive()
drive.login(None, None, None)
task = LoadTask(drive)
task.download_kline(file_pro=file)
