import os

from ccxt import binance
from fastapi import APIRouter
from notebuild.tool.fastapi import add_api_routes, api_route
from notecoin.coins.base.file import DataFileProperty

path_root = '/home/bingtao/workspace/tmp'


class DownloadServer(APIRouter):
    def __init__(self, prefix='/download', *args, **kwargs):
        super(DownloadServer, self).__init__(prefix=prefix, *args, **kwargs)
        self.runing = False
        add_api_routes(self)

    @api_route('/kline', description="get value")
    def download_kline(self, freq='daily', timeframe='1m'):
        if self.runing:
            return {"status": "false", "msg": "is running"}
        self.runing = True

        os.rmdir(f'{path_root}/notecoin')
        file_pro = DataFileProperty(exchange=binance(), path=path_root)
        file_pro.change_data_type('kline')
        file_pro.change_timeframe(timeframe=timeframe)
        file_pro.change_freq(freq)
        file_pro.load(total=400)
        file_pro.change_freq('weekly')
        file_pro.load(total=55)

        self.runing = False

    @api_route('/trade', description="get value")
    def download_trade(self, freq='daily'):
        if self.runing:
            return {"status": "false", "msg": "is running"}
        self.runing = True

        os.rmdir(f'{path_root}/notecoin')
        file_pro = DataFileProperty(exchange=binance(), path=path_root)
        file_pro.change_data_type('trade')
        file_pro.change_timeframe('detail')
        file_pro.change_freq(freq)
        file_pro.load(total=400)

        self.runing = False

    @api_route('/auto', description="get value")
    def download_auto(self, freq='daily'):
        if self.runing:
            return {"status": "false", "msg": "is running"}
        self.runing = True

        file_pro = DataFileProperty(exchange=binance(), path=path_root)
        for i in range(1, 500):
            file_pro.change_data_type('kline')
            file_pro.change_timeframe("1m")
            file_pro.change_freq(freq)
            file_pro.load(total=i)
            file_pro.change_freq('weekly')
            file_pro.load(total=i//7)

            file_pro.change_data_type('trade')
            file_pro.change_timeframe('detail')
            file_pro.change_freq(freq)
            file_pro.load(total=i)
        self.runing = False

    def clear(self):
        path = f'{path_root}/notecoin'
        if os.path.exists(path):
            os.rmdir(path)

