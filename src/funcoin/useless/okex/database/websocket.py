import time

from funcoin.database.base import Base, BaseTable, session
from sqlalchemy import BIGINT, Integer, Column, Float, String


class OkexSocketPublicTickers(Base, BaseTable):
    __tablename__ = 'okex_socket_public_tickets'
    instType = Column(String(120), comment='产品类型', primary_key=True)
    instId = Column(String(120), comment='产品ID', primary_key=True)

    last = Column(Float, comment='最新成交价')
    lastSz = Column(Float, comment='最新成交的数量')

    askPx = Column(Float, comment='卖一价')
    askSz = Column(Float, comment='卖一价对应的量')
    bidPx = Column(Float, comment='买一价')
    bidSz = Column(Float, comment='买一价对应的量')

    open24h = Column(Float, comment='24小时开盘价')
    high24h = Column(Float, comment='24小时最高价')
    low24h = Column(Float, comment='24小时最低价')
    volCcy24h = Column(Float, comment='24小时成交量，以计价货币为单位')
    vol24h = Column(Float, comment='24小时成交量，以交易货币为单位')

    sodUtc0 = Column(Float, comment='UTC 0时开盘价')
    sodUtc8 = Column(Float, comment='UTC+8时开盘价')

    ts = Column(BIGINT, comment='数据产生时间，Unix时间戳的毫秒数格式')

    def __init__(self, *args, **kwargs):
        self.instType = kwargs.get("instType", "SPOT")
        self.instId = kwargs.get("instId", "")
        self.last = kwargs.get("last", 0)
        self.lastSz = kwargs.get("lastSz", 0)
        self.askPx = kwargs.get("askPx", 0)
        self.askSz = kwargs.get("askSz", 0)
        self.bidPx = kwargs.get("bidPx", 0)

        self.bidSz = kwargs.get("bidSz", 0)
        self.open24h = kwargs.get("open24h", 0)
        self.high24h = kwargs.get("high24h", 0)
        self.low24h = kwargs.get("low24h", 0)
        self.volCcy24h = kwargs.get("volCcy24h", 0)

        self.vol24h = kwargs.get("vol24h", 0)
        self.sodUtc0 = kwargs.get("sodUtc0", 0)
        self.sodUtc8 = kwargs.get("sodUtc8", 0)
        self.ts = kwargs.get("ts", 0)

    def __repr__(self):
        return f"instType{self.instType},instId:{self.instId}"


class OkexWebsocketChannels(Base, BaseTable):
    __tablename__ = 'okex_websocket_channels'
    channel_json = Column(String(120), comment='channel_json', primary_key=True)
    channel = Column(String(20), comment='channel')
    instType = Column(String(20), comment='instType')
    instId = Column(String(20), comment='instId')
    updateTime = Column(BIGINT, comment='updateTime')

    def __init__(self, *args, **kwargs):
        self.channel_json = kwargs.get("channel_json")
        self.channel = kwargs.get("channel")
        self.instType = kwargs.get("instType")
        self.instId = kwargs.get("instId")
        self.updateTime = int(time.time() * 1000)

    @staticmethod
    def delete_dated():
        try:
            update_time = int(round(time.time() * 1000)) - 60 * 60 * 1000
            # 先查询，后删除
            session.query(OkexWebsocketChannels).filter(OkexWebsocketChannels.updateTime < update_time).delete()
            session.commit()
            return {"delete success"}
        except Exception as e:
            session.rollback()
            session.commit()
            return {"delete error": str(e)}


class OkexWebsocketResponse(Base, BaseTable):
    __tablename__ = 'okex_websocket_response'
    id = Column(Integer, comment='id', autoincrement=True, primary_key=True)
    channel = Column(String(20), comment='channel')
    response = Column(String(2000), comment='response')
    updateTime = Column(Integer, comment='updateTime')

    def __init__(self, *args, **kwargs):
        self.channel = kwargs.get("channel")
        self.response = kwargs.get("response")
        self.updateTime = int(time.time() * 1000)
