import logging

from funcoin.database.base import create_all, create_session
from funcoin.okex.database.websocket import OkexSocketPublicTickers
from funcoin.okex.websocket.utils import get_local_timestamp


class BaseHandle:
    def __init__(self, channels, *args, **kwargs):
        self.channels = channels
        self.session = create_session()

    def solve(self, data) -> bool:
        data = eval(data)

        if 'event' in data:
            return False

        channel = data['arg']['channel']
        if len(self.channels) == 0 or channel in self.channels:
            return self.handle(data)

    def handle(self, data) -> bool:
        print(f"{get_local_timestamp()}\t{data['arg']['channel']}\t{len(str(data))}")
        return True


class PublicTickers(BaseHandle):
    def __init__(self, *args, **kwargs):
        create_all()
        super(PublicTickers, self).__init__(channels=['tickers'], *args, **kwargs)

    def handle(self, data) -> bool:
        try:
            for arg in data['data']:
                self.session.merge(OkexSocketPublicTickers(**arg))
                self.session.commit()
        except Exception as e:
            self.session.rollback()
            logging.error(f"error:{e}")
            return False
        return True


class ResponseHandel(BaseHandle):
    def __init__(self, *args, **kwargs):
        create_all()
        super(ResponseHandel, self).__init__(channels=[], *args, **kwargs)

    def handle(self, data) -> bool:
        try:
            res = {
                "channel": data['arg']['channel'],
                "response": str(data)
            }
            self.session.merge(OkexSocketPublicTickers(**res))
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logging.error(f"error:{e}")
            return False
        return True
