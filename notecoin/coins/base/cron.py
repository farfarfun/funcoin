import logging

from ccxt import binance, okex
from notecoin.coins.base.file import DataFileProperty

logger = logging.getLogger()
logger.setLevel(logging.INFO)

path_root = '/home/bingtao/workspace/tmp'


def load_okex():
    file_pro = DataFileProperty(exchange=okex(), path=path_root)
    file_pro.change_data_type('trade')
    file_pro.change_timeframe('detail')
    file_pro.change_freq('daily')
    file_pro.load(total=3)
    # file_pro.change_freq('weekly')
    # file_pro.load(total=60)
    # file_pro.change_freq('monthly')
    # file_pro.load(total=12)

    file_pro.change_data_type('kline')
    file_pro.change_timeframe('1m')
    file_pro.change_freq('daily')
    file_pro.load(total=450)
    file_pro.change_freq('weekly')
    file_pro.load(total=60)


def load_binance():
    file_pro = DataFileProperty(exchange=binance(), path=path_root)
    file_pro.change_data_type('kline')
    file_pro.change_timeframe('1m')
    file_pro.change_freq('daily')
    file_pro.load(total=400)
    file_pro.change_freq('weekly')
    file_pro.load(total=55)

    file_pro.change_data_type('trade')
    file_pro.change_timeframe('detail')
    file_pro.change_freq('daily')
    file_pro.load(total=400)
    # file_pro.change_freq('weekly')
    # file_pro.load(total=60)
    # file_pro.change_freq('monthly')
    # file_pro.load(total=12)


# load_okex()
load_binance()
