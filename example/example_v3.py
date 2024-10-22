# nohup funcoin download -days=3333  >>./logs/fundev/funcoin-$(date +%Y-%m-%d).log 2>&1 &
# 19517

from datetime import datetime

from ccxt import binance

from funcoin.coins.base.file import DataFileProperty
from funcoin.task.load import LoadTask


def download():
    ds = datetime.now().strftime("%Y-%m-%d")
    start, _ = LoadTask.parse_day(ds)
    _, end = LoadTask.parse_day(ds, 3650)

    print(start, end)
    file_pro = DataFileProperty(exchange=binance(), path="tmp")
    file_pro.file_format = "%Y%m%d"
    file_pro.change_data_type("kline")
    file_pro.change_timeframe("1m")
    file_pro.change_freq("daily")
    file_pro.load_daily(start, end)


download()
