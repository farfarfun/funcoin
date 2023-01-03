import logging

from notecoin.huobi.connection.impl import (RestApiRequest, WebsocketManage,
                                            WebsocketRequest,
                                            WebSocketWatchDog, call_sync,
                                            call_sync_perforence_test)
from notecoin.huobi.constant.system import (ApiVersion, HttpMethod,
                                            WebSocketDefine,
                                            get_default_server_url)
from notecoin.huobi.utils.api_signature import create_signature
from notecoin.huobi.utils.huobi_api_exception import HuobiApiException
from notecoin.huobi.utils.url_params_builder import UrlParamsBuilder
from notetool.log import logger


class RestApiSyncClient(object):
    def __init__(self, api_key=None, secret_key=None, url=None, *args, **kwargs):
        """
        Create the request client instance.
        :param kwargs: The option of request connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: The URL name like "https://api.huobi.pro".
            performance_test: for performance test
            init_log: to init logger
        """
        self.__api_key = api_key
        self.__secret_key = secret_key
        self.__server_url = url or get_default_server_url(None)
        self.__init_log = kwargs.get("init_log", None)
        self.__performance_test = kwargs.get("performance_test", None)
        if self.__init_log and self.__init_log:
            logger.addHandler(logging.StreamHandler())

    def request_process(self, method, url, params):
        if self.__performance_test is not None and self.__performance_test is True:
            return self.request_process_performance(method, url, params)
        else:
            return self.request_process_product(method, url, params)

    def create_request(self, method, url, params):
        builder = UrlParamsBuilder()
        if params and len(params):
            if method in [HttpMethod.GET, HttpMethod.GET_SIGN]:
                for key, value in params.items():
                    builder.put_url(key, value)
            elif method in [HttpMethod.POST, HttpMethod.POST_SIGN]:
                for key, value in params.items():
                    builder.put_post(key, value)
            else:
                raise HuobiApiException(HuobiApiException.EXEC_ERROR, "[error] undefined HTTP method")

        if method == HttpMethod.GET:
            request = self.__create_request_by_get(url, builder)
        elif method == HttpMethod.GET_SIGN:
            request = self.__create_request_by_get_with_signature(url, builder)
        elif method == HttpMethod.POST_SIGN:
            request = self.__create_request_by_post_with_signature(url, builder)
        elif method == HttpMethod.POST:
            request = self.__create_request_by_post_with_signature(url, builder)
        else:
            raise HuobiApiException(HuobiApiException.INPUT_ERROR, "[Input] " + method + "  is invalid http method")

        return request

    def request_process_product(self, method, url, params):
        request = self.create_request(method, url, params)
        if request:
            return call_sync(request)

        return None

    def request_process_performance(self, method, url, params):
        request = self.create_request(method, url, params)
        if request:
            return call_sync_perforence_test(request)
        return None, 0, 0

    """
        for post batch operation, such as batch create orders[ /v1/order/batch-orders ]
        """

    def create_request_post_batch(self, method, url, params):
        builder = UrlParamsBuilder()
        if params and len(params):
            if method in [HttpMethod.POST, HttpMethod.POST_SIGN]:
                if isinstance(params, list):
                    builder.post_list = params
            else:
                raise HuobiApiException(HuobiApiException.EXEC_ERROR, "[error] undefined HTTP method")

        request = self.__create_request_by_post_with_signature(url, builder)

        return request

    """
    for post batch operation, such as batch create orders[ /v1/order/batch-orders ]
    """

    def request_process_post_batch(self, method, url, params):
        if self.__performance_test is not None and self.__performance_test is True:
            return self.request_process_post_batch_performance(method, url, params)
        else:
            return self.request_process_post_batch_product(method, url, params)

    def request_process_post_batch_product(self, method, url, params):
        request = self.create_request_post_batch(method, url, params)
        if request:
            return call_sync(request)

        return None

    def request_process_post_batch_performance(self, method, url, params):
        request = self.create_request_post_batch(method, url, params)
        if request:
            return call_sync_perforence_test(request)

        return None, 0, 0

    def __create_request_by_get(self, url, builder):
        request = RestApiRequest()
        request.method = "GET"
        request.host = self.__server_url
        request.header.update({'Content-Type': 'application/json'})
        request.url = url + builder.build_url()
        return request

    def __create_request_by_post_with_signature(self, url, builder):
        request = RestApiRequest()
        request.method = "POST"
        request.host = self.__server_url
        create_signature(self.__api_key, self.__secret_key, request.method, request.host + url, builder)
        request.header.update({'Content-Type': 'application/json'})
        if len(builder.post_list):  # specify for case : /v1/order/batch-orders
            request.post_body = builder.post_list
        else:
            request.post_body = builder.post_map
        request.url = url + builder.build_url()
        return request

    def __create_request_by_get_with_signature(self, url, builder):
        request = RestApiRequest()
        request.method = "GET"
        request.host = self.__server_url
        create_signature(self.__api_key, self.__secret_key, request.method, request.host + url, builder)
        request.header.update({"Content-Type": "application/x-www-form-urlencoded"})
        request.url = url + builder.build_url()
        return request


class SubscribeClient(object):
    subscribe_watch_dog = WebSocketWatchDog()

    def __init__(self, api_key=None, secret_key=None, **kwargs):
        """
        Create the subscription client to subscribe the update from server.

        :param kwargs: The option of subscription connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: Set the URI for subscription.
            init_log: to init logger
        """
        self.__api_key = api_key
        self.__secret_key = secret_key
        self.__uri = kwargs.get("url", WebSocketDefine.Uri)
        self.__init_log = kwargs.get("init_log", None)
        if self.__init_log and self.__init_log:
            logger.addHandler(logging.StreamHandler())

        self.__websocket_manage_list = list()

    def __create_websocket_manage(self, request):
        manager = WebsocketManage(self.__api_key, self.__secret_key, self.__uri, request)
        self.__websocket_manage_list.append(manager)
        manager.connect()
        SubscribeClient.subscribe_watch_dog.on_connection_created(manager)

    def create_request(self, subscription_handler, callback, error_handler, is_trade, is_mbp_feed=False):
        request = WebsocketRequest()
        request.subscription_handler = subscription_handler
        request.is_trading = is_trade
        request.is_mbp_feed = is_mbp_feed
        request.auto_close = False  # subscribe need connection. websocket request need close request.

        request.update_callback = callback
        request.error_handler = error_handler
        return request

    def create_request_v1(self, subscription_handler, callback, error_handler, is_trade=False):
        request = self.create_request(subscription_handler=subscription_handler, callback=callback,
                                      error_handler=error_handler, is_trade=is_trade)
        request.api_version = ApiVersion.VERSION_V1
        return request

    def create_request_v2(self, subscription_handler, callback, error_handler, is_trade=False):
        request = self.create_request(subscription_handler=subscription_handler, callback=callback,
                                      error_handler=error_handler, is_trade=is_trade)
        request.api_version = ApiVersion.VERSION_V2
        return request

    def execute_subscribe_v1(self, subscription_handler, callback, error_handler, is_trade=False):
        request = self.create_request_v1(subscription_handler, callback, error_handler, is_trade)
        self.__create_websocket_manage(request)

    def execute_subscribe_v2(self, subscription_handler, callback, error_handler, is_trade=False):
        request = self.create_request_v2(subscription_handler, callback, error_handler, is_trade)
        self.__create_websocket_manage(request)

    def execute_subscribe_mbp(self, subscription_handler, callback, error_handler, is_trade=False,
                              is_mbp_feed=True):
        request = self.create_request(subscription_handler, callback, error_handler, is_trade, is_mbp_feed)
        self.__create_websocket_manage(request)

    def unsubscribe_all(self):
        for websocket_manage in self.__websocket_manage_list:
            SubscribeClient.subscribe_watch_dog.on_connection_closed(websocket_manage)
            websocket_manage.close()
        self.__websocket_manage_list.clear()


class WebSocketReqClient(object):

    def __init__(self, api_key=None, secret_key=None, **kwargs):
        """
        Create the subscription client to subscribe the update from server.

        :param kwargs: The option of subscription connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: Set the URI for subscription.
            init_log: to init logger
        """
        self.__api_key = api_key
        self.__secret_key = secret_key
        self.__uri = kwargs.get("url", WebSocketDefine.Uri)
        self.__init_log = kwargs.get("init_log", None)
        if self.__init_log and self.__init_log:
            logger = logging.getLogger("huobi-client")
            logger.setLevel(level=logging.INFO)
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(handler)

    def __create_websocket_manage(self, request):
        manager = WebsocketManage(self.__api_key, self.__secret_key, self.__uri, request)
        manager.connect()

    def create_request(self, subscription_handler, callback, error_handler, is_trade=False, is_mbp_feed=False):
        request = WebsocketRequest()
        request.subscription_handler = subscription_handler
        request.is_trading = is_trade
        request.is_mbp_feed = is_mbp_feed
        request.auto_close = True  # for websocket request, auto close the connection after request.

        request.update_callback = callback
        request.error_handler = error_handler
        return request

    def execute_subscribe_v1(self, subscription_handler, callback, error_handler, is_trade=False):
        request = self.create_request(subscription_handler, callback, error_handler, is_trade)
        self.__create_websocket_manage(request)

    def execute_subscribe_mbp(self, subscription_handler, callback, error_handler, is_trade=False,
                              is_mbp_feed=True):
        request = self.create_request(subscription_handler, callback, error_handler, is_trade, is_mbp_feed)
        self.__create_websocket_manage(request)
