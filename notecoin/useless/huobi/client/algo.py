from notecoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from notecoin.huobi.constant import (AlgoOrderStatus, AlgoOrderType,
                                     HttpMethod, OrderSide, OrderType,
                                     SortDesc)
from notecoin.huobi.utils import check_should_not_none
from notecoin.huobi.utils.input_checker import (check_symbol,
                                                check_time_in_force)


class AlgoClient(object):

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

    def create_order(self, account_id: 'int', symbol: 'str', order_side: OrderSide, order_type: OrderType,
                     client_order_id: 'str', stop_price: 'str', order_price: 'str' = None, order_size: 'str' = None,
                     order_value: 'str' = None, time_in_force: 'str' = None, trailing_rate: 'str' = None) -> int:
        """
        Make an algo order in huobi.
        :param account_id: Account id. (mandatory)
        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param order_side: the Order side, possible values: buy,sell. (mandatory)
        :param order_type: The order type, possible values: limit, market. (mandatory)
        :param stop_price: The stop price. (mandatory)
        :param order_price: The limit price of limit order, only needed for limit order.
                            (mandatory for buy-limit, sell-limit, buy-limit-maker and sell-limit-maker)
        :param order_size: The amount of market order only
        :param order_value: for market buy order only
        :param stop_price: Price for auto sell to get the max benefit
        :param time_in_force: gtc(invalid for orderType=market), boc(invalid orderType=market),ioc,
                              fok(invalid for orderType=market)
        :param trailing_rate: for trailing orders only
        :param client_order_id: unique Id which is user defined and must be unique in recent 24 hours
        """

        params = self.create_order_param_check(symbol, account_id, order_side, order_type, stop_price, order_price,
                                               order_size, order_value, time_in_force, trailing_rate, client_order_id)
        channel = "/v2/algo-orders"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def cancel_orders(self, client_order_ids):
        check_should_not_none(client_order_ids, "clientOrderIds")
        channel = "/v2/algo-orders/cancellation"
        params = {
            "clientOrderIds": client_order_ids
        }
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def get_open_orders(self, account_id: 'str' = None, symbol: 'str' = None, order_side: OrderSide = None,
                        order_type: AlgoOrderType = None, sort: SortDesc = None, limit: 'int' = 100,
                        from_id: 'int' = None):

        params = {
            "accountId": account_id,
            "symbol": symbol,
            "orderSide": order_side,
            "orderType": order_type,
            "sort": sort,
            "limit": limit,
            "fromId": from_id
        }
        channel = "/v2/algo-orders/cancellation"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_order_history(self, symbol: 'str', order_status: AlgoOrderStatus, account_id: 'str' = None,
                          order_side: 'OrderSide' = None, order_type: 'AlgoOrderType' = None, start_time: 'int' = None,
                          end_time: 'int' = None, sort: 'SortDesc' = SortDesc.DESC, limit: 'int' = 100,
                          from_id: 'int' = None):

        params = {
            "symbol": symbol,
            "accountId": account_id,
            "orderSide": order_side,
            "orderType": order_type,
            "orderStatus": order_status,
            "startTime": start_time,
            "endTime": end_time,
            "sort": sort,
            "limit": limit,
            "fromId": from_id
        }
        channel = "/v2/algo-orders/history"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_order(self, client_order_id: 'str'):
        params = {
            "clientOrderId": client_order_id
        }
        channel = "/v2/algo-orders/specific"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    @staticmethod
    def create_order_param_check(symbol, account_id, order_side, order_type, stop_price, order_price,
                                 order_size, order_value, time_in_force, trailing_rate, client_order_id):
        check_symbol(symbol)
        check_should_not_none(account_id, "accountId")
        check_should_not_none(order_type, "orderType")
        check_should_not_none(order_side, "orderSide")

        if order_type == OrderType.SELL_LIMIT \
                or order_type == OrderType.BUY_LIMIT \
                or order_type == OrderType.BUY_LIMIT_MAKER \
                or order_type == OrderType.SELL_LIMIT_MAKER:
            check_should_not_none(order_price, "orderPrice")

        if time_in_force is not None:
            check_time_in_force(time_in_force)

        if order_type in [OrderType.SELL_MARKET, OrderType.BUY_MARKET]:
            order_price = None

        params = {
            "accountId": account_id,
            "symbol": symbol,
            "orderPrice": order_price,
            "orderSide": order_side,
            "orderSize": order_size,
            "orderValue": order_value,
            "timeInForce": time_in_force,
            "orderType": order_type,
            "clientOrderId": client_order_id,
            "stopPrice": stop_price,
            "trailingRate": trailing_rate
        }

        return params
