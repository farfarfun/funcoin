from funcoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from funcoin.huobi.constant import HttpMethod


class GenericClient(object):

    def __init__(self, *args, **kwargs):
        """
        Create the request client instance.
        :param kwargs: The option of request connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: The URL name like "https://api.huobi.pro".
            init_log: to init logger
        """
        self.__kwargs = kwargs
        self.rest_api_sync_client = RestApiSyncClient(*args, **kwargs)
        self.web_socket_req_client = WebSocketReqClient(*args, **kwargs)
        self.sub_socket_req_client = SubscribeClient(*args, **kwargs)

    def get_exchange_timestamp(self) -> int:
        """
        Get the timestamp from Huobi server. The timestamp is the Unix timestamp in millisecond.
        The count shows how many milliseconds passed from Jan 1st 1970, 00:00:00.000 at UTC.
        e.g. 1546300800000 is Thu, 1st Jan 2019 00:00:00.000 UTC.

        :return: The timestamp in UTC
        """
        channel = "/v1/common/timestamp"
        params = {}

        return self.rest_api_sync_client.request_process(HttpMethod.GET, channel, params)

    def get_exchange_currencies(self):
        """
        Get all the trading assets and currencies supported in huobi.
        The information of trading instrument, including base currency, quote precision, etc.

        :return: The information of trading currencies.
        """
        channel = "/v1/common/currencys"
        params = {}

        return self.rest_api_sync_client.request_process(HttpMethod.GET, channel, params)

    def get_exchange_symbols(self):
        """
        Get all the trading assets and currencies supported in huobi.
        The information of trading instrument etc.

        :return: The information of trading instrument.
        """
        channel = "/v1/common/symbols"
        params = {}

        return self.rest_api_sync_client.request_process(HttpMethod.GET, channel, params)

    def get_exchange_info(self):
        """
        Get all the trading assets and currencies supported in huobi.
        The information of trading instrument, including base currency, quote precision, etc.

        :return: The information of trading instrument and currencies.
        """

        ret = {
            "symbol_list": self.get_exchange_symbols(),
            "currencies": self.get_exchange_currencies()
        }
        return ret

    def get_reference_currencies(self, currency: 'str' = None, is_authorized_user: 'bool' = None) -> list:
        """
        Get all the trading assets and currencies supported in huobi.
        The information of trading instrument, including base currency, quote precision, etc.

        :param currency: btc, ltc, bch, eth, etc ...(available currencies in Huobi Global)
        :param is_authorized_user: is Authorized user? True or False
        :return: The information of trading instrument and currencies.
        """
        channel = "/v2/reference/currencies"
        params = {
            "currency": currency,
            "authorizedUser": is_authorized_user
        }

        return self.rest_api_sync_client.request_process(HttpMethod.GET, channel, params)

    def get_system_status(self) -> str:
        """
        get system status

        :return: system status.
        """
        channel = "/api/v2/summary.json"
        temp = self.rest_api_sync_client.__server_url
        self.rest_api_sync_client.__server_url = "https://status.huobigroup.com"
        res = self.rest_api_sync_client.request_process(HttpMethod.GET, channel, {})
        self.rest_api_sync_client.__server_url = temp
        return res

    def get_market_status(self):
        channel = "/v2/market-status"

        return self.rest_api_sync_client.request_process(HttpMethod.GET, channel, {})
