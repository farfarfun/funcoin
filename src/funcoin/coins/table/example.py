import ccxt
from fundrive import AlipanDrive
from fundrive.core.table import DriveTable
from funsecret import read_secret

from funcoin.coins.table.load import LoadTask

exchange = ccxt.binance(  # noqa: F821
    {
        "apiKey": read_secret("coin", "binance", "api_key"),
        "secret": read_secret("coin", "binance", "secret_key"),
    }
)

# logging.basicConfig(level=logging.INFO)
drive = AlipanDrive()
drive.login()
table = DriveTable(table_fid="66bf1a293f124e5a2d2841eeb0fbb21b70ddb0ee", drive=drive)
task = LoadTask(table=table, exchange=exchange)
task.run()
