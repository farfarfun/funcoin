from funcoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from funcoin.huobi.constant import HttpMethod
from funcoin.huobi.utils import check_should_not_none, check_symbol


class EtfClient(object):

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

    def get_etf_swap_config(self, etf_name: 'str'):
        """
        Get the basic information of ETF creation and redemption, as well as ETF constituents,
        including max amount of creation, min amount of creation, max amount of redemption, min amount
        of redemption, creation fee rate, redemption fee rate, eft create/redeem status.

        :param etf_name: The symbol, currently only support hb10. (mandatory)
        :return: The etf configuration information.
        """
        check_symbol(etf_name)
        params = {
            "etf_name": etf_name
        }
        channel = "/etf/swap/config"
        return self.rest_api_sync_client.request_process(HttpMethod.GET, channel, params)

    def get_etf_swap_list(self, etf_name: 'str', offset: 'int', size: 'int') -> list:
        """
        Get past creation and redemption.(up to 100 records)

        :param etf_name: The symbol, currently only support hb10. (mandatory)
        :param offset: The offset of the records, set to 0 for the latest records. (mandatory)
        :param size: The number of records to return, the range is [1, 100]. (mandatory)
        :return: The swap history.
        """
        check_symbol(etf_name)
        params = {
            "etf_name": etf_name,
            "offset": offset,
            "limit": size
        }
        channel = "/etf/swap/list"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def post_etf_swap_in(self, etf_name: 'str', amount: 'int') -> None:
        """
        Order creation or redemption of ETF.

        :param etf_name: The symbol, currently only support hb10. (mandatory)
        :param amount: The amount to create or redemption. (mandatory)
        :return: No return
        """
        check_symbol(etf_name)
        check_should_not_none(amount, "amount")

        params = {
            "etf_name": etf_name,
            "amount": amount
        }
        channel = "/etf/swap/in"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def post_etf_swap_out(self, etf_name: 'str', amount: 'int') -> None:
        """
        Order creation or redemption of ETF.

        :param etf_name: The symbol, currently only support hb10. (mandatory)
        :param amount: The amount to create or redemption. (mandatory)
        :return: No return
        """

        check_symbol(etf_name)
        check_should_not_none(amount, "amount")

        params = {
            "etf_name": etf_name,
            "amount": amount
        }
        channel = "/etf/swap/out"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)
