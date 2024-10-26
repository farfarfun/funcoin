import ccxt
from fundrive import OSSDrive
from fundrive.core.table import DriveTable
from funsecret import read_secret

from funcoin.coins.table.load import LoadTask


def download_daily(days=365, *arge, **kwargs):
    days = int(days)
    exchange = ccxt.binance(  # noqa: F821
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


# download_daily(days=3)
