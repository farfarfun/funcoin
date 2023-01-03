import logging
import time

from notecoin.database.base import session
from notecoin.okex.database.client import OkexClientAccountBalance
from notecoin.okex.websocket.channel import PublicChannel
from notecoin.okex.websocket.connect import PublicConnect
from notecoin.okex.websocket.handle import PublicTickers
from notetool.secret import read_secret

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def init():
    uri = "mysql+pymysql://notecoin:notecoin@127.0.0.1:3306/notecoin?charset=utf8&autocommit=true"
    # uri = f'sqlite:///{os.path.abspath(os.path.dirname(__file__))}/notecoin.db'
    read_secret(cate1='notecoin', cate2='dataset', cate3='db_path', value=uri)


channel_update_time = time.time()


def channel_listen():
    details = session.query(OkexClientAccountBalance).filter(OkexClientAccountBalance.eqUsd > 10).all()
    channels = []
    global channel_update_time
    if time.time() < channel_update_time + 10:
        return channels
    for _detail in details:
        detail = _detail.json()
        ccy = detail['ccy']
        if ccy == 'USDT':
            continue
        channels.append(PublicChannel.public_tickers().to_json())
    channel_update_time = time.time()
    return channels


connect = PublicConnect([PublicChannel.public_tickers().to_json()], channel_listen=channel_listen)
connect.add_handle(PublicTickers())
connect.run()
