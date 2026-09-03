import os
from datetime import datetime, timedelta

import ccxt
from farlog import getLogger
from funfile.compress import tarfile
from funtable.table import DriveTable

from funcoin.coins.base.loader import BaseLoader, KlineLoder, TradeLoader

logger = getLogger("funcoin")


class FileProperty:
    """描述一份按日切分的行情数据文件的命名与分区规则。"""

    def __init__(
        self, exchange_name: str, data_type: str = "kline", timeframe: str = "1m"
    ) -> None:
        """
        Args:
            exchange_name: 交易所名称，如 `binance`。
            data_type: 数据类型，如 `kline`（K 线）。
            timeframe: K 线周期，如 `1m`。
        """
        self.data_type = data_type
        self.timeframe = timeframe
        self.exchange_name = exchange_name

        self.freq: str | None = None
        self.end_date: datetime | None = None
        self.start_date: datetime | None = None
        self.par_format: str | None = None
        self.file_format: str | None = None

    def daily(self, ds: str) -> "FileProperty":
        """设置为按天切分，`ds` 为 `%Y%m%d` 格式的日期字符串。"""
        self.freq = "daily"
        self.file_format = self.file_format or "%Y%m%d"
        self.par_format = self.par_format or "%Y%m"
        self.start_date = datetime.strptime(ds, "%Y%m%d")
        self.end_date = self.start_date + timedelta(days=1)
        return self

    @property
    def partition(self) -> str:
        """数据所属的分区（按月）。"""
        return self.start_date.strftime(self.par_format)

    @property
    def filename_prefix(self) -> str:
        """不含扩展名的文件名前缀。"""
        return f"{self.exchange_name}_{self.data_type}_{self.freq}_{self.timeframe}-{self.start_date.strftime(self.file_format)}"

    @property
    def file_path_csv(self) -> str:
        """CSV 文件路径。"""
        return f"{self.filename_prefix}.csv"

    @property
    def file_path_tar(self) -> str:
        """压缩后的 tar 文件路径。"""
        return f"{self.filename_prefix}.tar"


class LoadTask:
    """按日拉取行情数据、打包并上传到云存储表的任务编排器。"""

    def __init__(
        self,
        table: DriveTable,
        exchange: ccxt.Exchange,
    ) -> None:
        """
        Args:
            table: 数据落地的云存储表。
            exchange: ccxt 交易所客户端实例。
        """
        self.table = table
        self.exchange = exchange

    def download(self, loader: BaseLoader, file_pro: FileProperty) -> bool:
        """用给定 loader 拉取一天的数据，压缩后上传，并清理本地临时文件。

        Args:
            loader: 具体的数据加载器（K线/成交）。
            file_pro: 描述本次数据文件命名与分区的对象。

        Returns:
            上传是否成功；压缩/上传失败时会向上抛出原始异常。

        Raises:
            Exception: 压缩或上传过程中出现的任何异常（清理完本地临时文件后重新抛出）。
        """
        logger.info(f"download for {file_pro.file_path_tar}")
        # 下载
        loader.load_symbols()
        try:
            # 压缩
            with tarfile.open(file_pro.file_path_tar, "w|xz") as tar:
                tar.add(file_pro.file_path_csv)
            self.table.upload(
                file=file_pro.file_path_tar, partition=file_pro.partition, overwrite=True
            )
            return True
        finally:
            # 无论压缩/上传成功与否，本地临时文件都要清理，避免磁盘残留；
            # 但不吞异常，finally 结束后原始异常会继续向上传播。
            if os.path.exists(file_pro.file_path_csv):
                os.remove(file_pro.file_path_csv)
            if os.path.exists(file_pro.file_path_tar):
                os.remove(file_pro.file_path_tar)

    def download_kline(self, file_pro: FileProperty) -> bool:
        """拉取并上传一天的 K 线数据。"""
        loader = KlineLoder(
            self.exchange,
            unix_start=int(file_pro.start_date.timestamp() * 1000),
            unix_end=int(file_pro.end_date.timestamp() * 1000),
            csv_path=file_pro.file_path_csv,
            timeframe=file_pro.timeframe,
        )
        return self.download(loader, file_pro)

    def download_trade(self, file_pro: FileProperty) -> bool:
        """拉取并上传一天的逐笔成交数据。"""
        loader = TradeLoader(
            self.exchange,
            unix_start=int(file_pro.start_date.timestamp() * 1000),
            unix_end=int(file_pro.end_date.timestamp() * 1000),
            csv_path=file_pro.file_path_csv,
            timeframe=file_pro.timeframe,
        )
        return self.download(loader, file_pro)

    def run(self, days: int = 365) -> None:
        """从昨天开始往前回补 `days` 天的 K 线数据，已存在的分区跳过。"""
        self.table.update_partition_dict()
        self.table.update_partition_meta(refresh=True)

        start_day = datetime.now() - timedelta(days=1)
        file_pro = FileProperty(self.exchange.name.lower()).daily(
            start_day.strftime("%Y%m%d")
        )
        exists_data = dict([file["name"], file] for file in self.table.partition_meta())

        for i in range(days):
            start_day += timedelta(days=-1)
            file_pro.daily(start_day.strftime("%Y%m%d"))
            if file_pro.file_path_tar in exists_data.keys():
                logger.info(f"{file_pro.file_path_tar} exists, skip.")
                continue
            self.download_kline(file_pro)
