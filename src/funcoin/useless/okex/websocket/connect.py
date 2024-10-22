import base64
import hmac
import json
import logging
import time
from threading import Thread
from typing import List

from funcoin.okex.websocket.handle import BaseHandle
from funcoin.okex.websocket.utils import get_local_timestamp
from funtool.secret import read_secret

from websocket import WebSocket, WebSocketException, create_connection

ping_interval = 30


class BaseConnect:
    def __init__(
        self,
        url,
        channels,
        api_key=None,
        secret_key=None,
        passphrase=None,
        prefix="websocket",
        private=False,
        *args,
        **kwargs,
    ):
        self.url = url
        self.channels = channels
        self.api_key = api_key or read_secret(cate1="coin", cate2="okex", cate3="api_key")
        self.secret_key = secret_key or read_secret(cate1="coin", cate2="okex", cate3="secret_key")
        self.passphrase = passphrase or read_secret(cate1="coin", cate2="okex", cate3="passphrase")
        self.private = private
        self.ws: WebSocket = create_connection(self.url)
        self.handles: List[BaseHandle] = []

    def add_handle(self, handle: BaseHandle):
        self.handles.append(handle)

    def run(self):
        self.subscribe_start()

        def start_heartbeat():
            time.sleep(ping_interval)
            self.ping()

        Thread(target=start_heartbeat).start()

        def on_message():
            while True:
                try:
                    res = self.ws.recv()
                    self.handle_data(res)
                except (TimeoutError, WebSocketException):
                    try:
                        self.ping()
                    except Exception as e:
                        logging.warning(f"连接关闭，正在重连:{e}")
                        self.subscribe_restart()
                    continue

        Thread(target=on_message).start()
        time.sleep(5)

    def handle_data(self, res):
        for handle in self.handles:
            try:
                handle.solve(res)
            except Exception as e:
                logging.info(f"error:{e}")

    def ping(self):
        self.ws.send("ping")

    def subscribe_restart(self):
        self.subscribe_stop()
        self.subscribe_start()

    def subscribe_start(self):
        self.ws: WebSocket = create_connection(self.url)
        if self.private:
            timestamp = str(get_local_timestamp())
            message = timestamp + "GET" + "/users/self/verify"
            mac = hmac.new(
                bytes(self.secret_key, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256"
            )
            sign = base64.b64encode(mac.digest()).decode("utf-8")
            login_param = {
                "op": "login",
                "args": [{"apiKey": self.api_key, "passphrase": self.passphrase, "timestamp": timestamp, "sign": sign}],
            }
            login_str = json.dumps(login_param)
        else:
            login_str = json.dumps({"op": "subscribe", "args": self.channels})

        self.ws.send(login_str)
        logging.info(f"send: {login_str}")

    def subscribe_stop(self):
        self.ws: WebSocket = create_connection(self.url)
        sub_param = {"op": "unsubscribe", "args": self.channels}
        sub_str = json.dumps(sub_param)
        self.ws.send(sub_str)
        logging.info(f"send: {sub_str}")


class PublicConnect(BaseConnect):
    def __init__(self, channels, *args, **kwargs):
        super(PublicConnect, self).__init__(
            url="wss://ws.okx.com:8443/ws/v5/public", channels=channels, *args, **kwargs
        )


class PrivateConnect(BaseConnect):
    def __init__(self, channels, *args, **kwargs):
        super(PrivateConnect, self).__init__(
            url="wss://ws.okx.com:8443/ws/v5/private", private=True, channels=channels, *args, **kwargs
        )


class TradeConnect(PrivateConnect):
    def __init__(self, *args, **kwargs):
        super(TradeConnect, self).__init__(*args, **kwargs)
