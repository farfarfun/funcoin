import ccxt
from fundrive.drives import OSSDrive
from funsecret import read_secret
from funtable.table import DriveTable

from funcoin.coins.constants import DEFAULT_DAYS
from funcoin.coins.table.load import LoadTask


def download_daily(days: int = DEFAULT_DAYS) -> None:
    """从 Binance 下载最近指定天数的日行情并上传到 OSS。

    Args:
        days: 从昨天往前回补的天数，已上传过的分区会跳过。
    """
    days = int(days)
    exchange = ccxt.binance(
        {
            "apiKey": read_secret("coin", "binance", "api_key"),
            "secret": read_secret("coin", "binance", "secret_key"),
        }
    )

    drive = OSSDrive()

    drive.login(
        access_key=read_secret("fundrive", "oss", "farfarfun", "access_key"),
        access_secret=read_secret("fundrive", "oss", "farfarfun", "access_secret"),
        endpoint=read_secret("fundrive", "oss", "farfarfun", "endpoint"),
        bucket_name="farfarfun",
    )

    table = DriveTable(table_fid="funcoin/binance_kline_daily_1m/", drive=drive)
    table.update_partition_meta()
    task = LoadTask(table=table, exchange=exchange)
    task.run(days=days)
