import json
import logging
from typing import Dict, Optional

import requests
from funcoin.okex.common import exceptions
from funcoin.okex import utils
from funcoin.okex.types import Response

GET = "GET"
POST = "POST"
DELETE = "DELETE"
API_URL = 'https://www.okex.com'
SERVER_TIMESTAMP_URL = '/api/general/v5/time'


class BaseClient(object):

    def __init__(self, api_key, api_secret_key, passphrase, use_server_time=False, test=False, first=False,
                 api_url=None, flag='1', proxy=None):
        self.API_KEY = api_key
        self.API_SECRET_KEY = api_secret_key
        self.PASSPHRASE = passphrase
        self.use_server_time = use_server_time
        self.flag = flag
        self.first = first
        self.test = test
        self.api_url = api_url or API_URL
        self.proxy = proxy

    def _request(self, method, uri, params, cursor=False):
        if method == GET:
            uri = uri + utils.parse_params_to_str(params)

        url = self.api_url + uri

        # 获取本地时间
        timestamp = utils.get_timestamp()

        # sign & header
        if self.use_server_time:
            # 获取服务器时间
            timestamp = self._get_timestamp()

        if isinstance(params, str):
            body = params if method == POST else ""
        elif isinstance(params, dict):
            body = json.dumps(params) if method == POST else ""
        else:
            body = json.dumps(params) if method == POST else ""
        sign = utils.sign(utils.pre_hash(timestamp, method, uri, str(body)), self.API_SECRET_KEY)
        header = utils.get_header(self.API_KEY, sign, timestamp, self.PASSPHRASE)

        if self.test:
            header['x-simulated-trading'] = '1'
        if self.first:
            print("url:", url)
            self.first = False

        logging.debug("url: " + url)
        logging.debug("body: " + body)

        # send request
        response = None
        if method == GET:
            if self.proxy is None:
                response = requests.get(url, headers=header)
            else:
                response = requests.get(url, headers=header, proxies=self.proxy)
        elif method == POST:
            if self.proxy is None:
                response = requests.post(url, data=body, headers=header)
            else:
                response = requests.post(url, data=body, headers=header, proxies=self.proxy)
        elif method == DELETE:
            if self.proxy is None:
                response = requests.delete(url, headers=header)
            else:
                response = requests.delete(url, headers=header, proxies=self.proxy)

        # exception handle
        if not str(response.status_code).startswith('2'):
            raise exceptions.OkexAPIException(response)
        try:
            res_header = response.headers
            if cursor:
                r = dict()
                try:
                    r['before'] = res_header['OK-BEFORE']
                    r['after'] = res_header['OK-AFTER']
                except Exception as e:
                    logging.warning(f"error:{e}")
                    pass
                return Response(response.json()), r
            else:
                return Response(response.json())

        except ValueError:
            raise exceptions.OkexRequestException('Invalid Response: %s' % response.text)

    def _request_without_params(self, method, request_path):
        return self._request(method, request_path, {})

    def _request_with_params(self, method, request_path, params, cursor=False):
        param = {}
        for k, v in params.items():
            if v is not None:
                param[k] = v
        return self._request(method, request_path, param, cursor)

    def _get_timestamp(self):
        url = self.api_url + SERVER_TIMESTAMP_URL
        response = requests.get(url, proxies=self.proxy)
        if response.status_code == 200:
            # return response.json()['ts']
            return response.json()['iso']
        else:
            return ""

    def set_api_url(self, url: str):
        self.api_url = url

    def set_proxy(self, proxy: Optional[Dict]):
        self.proxy = proxy


class Client(BaseClient):
    pass
