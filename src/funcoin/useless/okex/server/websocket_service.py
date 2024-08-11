import logging

from fastapi import APIRouter
from darkbuild.tool.fastapi import add_api_routes, api_route
from funcoin.database.base import create_all, create_session
from funcoin.database.connect.base import BaseConnect
from funcoin.okex.database.client import OkexClientAccountBalance
from funcoin.okex.database.websocket import OkexWebsocketChannels
from funcoin.okex.websocket.channel import PublicChannel
from funcoin.okex.websocket.connect import PublicConnect
from funcoin.okex.websocket.handle import PublicTickers, ResponseHandel


class WebsocketService(APIRouter):
    def __init__(self, connect: BaseConnect = None, prefix='/websocket', *args, **kwargs):
        self.connect = connect or PublicConnect(channels=[PublicChannel.public_tickers().to_json()])
        self.connect.add_handle(PublicTickers())
        self.connect.add_handle(ResponseHandel())
        self.session = create_session()

        super(WebsocketService, self).__init__(prefix=prefix, *args, **kwargs)
        add_api_routes(self)

        self.connect.run()

    def update_channels_db(self):
        details = self.session.query(OkexClientAccountBalance).filter(OkexClientAccountBalance.eqUsd > 10).all()

        for _detail in details:
            detail = _detail.json()
            if detail['ccy'] == 'USDT':
                continue
            instId = f"{detail['ccy']}-USDT"
            param = {
                "channel_json": str(PublicChannel.public_tickers(instId).to_json()),
                "channel": "tickers",
                "instId": instId
            }
            self.session.merge(OkexWebsocketChannels(**param))
        create_all()
        self.session.commit()
        return {"update_db": len(details)}

    @api_route('/update/channel', description="get value")
    def update_channels(self):
        try:
            res = self.update_channels_db()

            new_channels = self.session.query(OkexWebsocketChannels).all()
            inst_list = [channel.instId for channel in new_channels]
            new_channels = [eval(channel.channel_json) for channel in new_channels]

            new_channels_str = [str(channel) for channel in new_channels]
            old_channels_str = [str(channel) for channel in self.connect.channels]

            if new_channels_str is not None and len(new_channels_str) > 0:
                if len(list(set(old_channels_str) - set(new_channels_str))) > 0 or len(
                        list(set(new_channels_str) - set(old_channels_str))) > 0:
                    logging.info(f"old:{old_channels_str},new:{new_channels_str}")
                    self.connect.channels = new_channels
                    self.connect.subscribe_restart()
                    res['subscribe'] = 'restart'

            res["channels_size"] = len(self.connect.channels)
            res["update"] = "success"
            res['inst_list'] = inst_list
        except Exception as e:
            self.session.rollback()
            res["update"] = "failed"
            res["error"] = str(e)
        return res
