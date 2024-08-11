import json
import os
import time

import requests
from funtool.tool.secret import read_secret
from wechatpy.enterprise import WeChatClient


class HuoBiMessage:
    def __init__(self, name="huobi", webhook=None, agent_id=None, secret=None, company_id=None):
        self.name = name
        # 机器人webhook
        self.webhook = read_secret("wechat", "farfarfun", "huobi", name, "webhook", value=webhook)
        # 应用ID
        self.agent_id = read_secret("wechat", "farfarfun", "huobi", name, "agent_id", value=agent_id)
        # 企业ID
        self.company_id = read_secret("wechat", "farfarfun", "huobi", name, "company_id", value=company_id)
        # 应用Secret
        self.secret = read_secret("wechat", "farfarfun", "huobi", name, "secret", value=secret)
        self.client = WeChatClient(corp_id=self.company_id, secret=self.secret)

    def send_msg(self, msg):
        self.check_token()
        data = {"msgtype": "text", "text": {"content": msg}}
        self.client.message.send(agent_id=self.agent_id, user_ids=["NiuLiangTao", "GuoYe"], party_ids=["3"], msg=data)

        self.send_to_qywx(msg)

    def send_to_qywx(self, msg):
        headers = {"Content-Type": "application/json"}
        data = {"msgtype": "text", "text": {"content": msg}}

        data = json.dumps(data)
        r = requests.post(url=self.webhook, headers=headers, data=data)

    def check_token(self):
        # 通行密钥
        access_token = None
        access_token = read_secret("wechat", "farfarfun", "huobi", self.name, "access_token")
        if not access_token:
            response = self.client.fetch_access_token()
            access_token = read_secret(
                "wechat",
                "farfarfun",
                "huobi",
                self.name,
                "access_token",
                value=response["access_token"],
                expire_time=response["expires_in"],
            )
            print(response)

        self.client.session.set(self.client.access_token_key, access_token)
        return access_token
