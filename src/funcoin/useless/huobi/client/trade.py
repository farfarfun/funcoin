import time

from funcoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from funcoin.huobi.constant import (AccountType, HttpMethod, OrderSide,
                                     OrderSource, OrderState, OrderType,
                                     TransferFuturesPro)
from funcoin.huobi.utils import (check_currency, check_list, check_range,
                                  check_should_not_none, check_symbol,
                                  check_symbol_list, format_date,
                                  orders_update_channel,
                                  request_order_detail_channel,
                                  request_order_list_channel,
                                  trade_clearing_channel)


class TradeClient(object):

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
        self.trade_client = RestApiSyncClient(*args, **kwargs)
        self.trade_client_socket = WebSocketReqClient(*args, **kwargs)
        self.trade_client_socket_sub = SubscribeClient(*args, **kwargs)

    def get_fee_rate(self, symbols: str) -> list:
        """
        Get the candlestick/kline for the specified symbol. The data number is 150 as default.

        :param symbols: The symbol, like "btcusdt". To query hb10, put "hb10" at here. (mandatory)
        : interval: The candlestick/kline interval, MIN1, MIN5, DAY1 etc. (mandatory)
        : size: The start time of of requested candlestick/kline data. (optional)
        : start_time: The start time of of requested candlestick/kline data. (optional)
        : end_time: The end time of of requested candlestick/kline data. (optional)
        :return: The list of candlestick/kline data.
        """
        channel = "/v1/fee/fee-rate/get"
        check_symbol(symbols)

        params = {
            "symbols": symbols
        }
        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_transact_fee_rate(self, symbols: str) -> list:
        """
        The request of get transact fee rate list.

        :param symbols: The symbol, like "btcusdt,htusdt". (mandatory)
        :return: The transact fee rate list.
        """
        check_symbol(symbols)

        params = {
            "symbols": symbols
        }
        channel = "/v2/reference/transact-fee-rate"

        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def sub_order_update(self, symbols: str, callback, error_handler=None):
        """
        Subscribe order changing event. If a order is created, canceled etc,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(order_update_event: 'OrderUpdateEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return:  No return
        """
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(callback, "callback")

        def subscription(connection):
            for val in symbol_list:
                connection.send(orders_update_channel(val))
                time.sleep(0.01)

        self.trade_client_socket_sub.execute_subscribe_v2(subscription, callback, error_handler, is_trade=True)

    def req_order_list(self, symbol: str, account_id: int, callback, order_states: str,
                       order_types: str = None, start_date: str = None, end_date: str = None, from_id=None,
                       direct=None, size=None, client_req_id: str = None, error_handler=None):
        """
        request order list.

        :param client_req_id:  client_req_id:
        :param size:  size:
        :param direct:  direct:
        :param from_id:  from_id:
        :param end_date:  end_date:
        :param start_date:  start_date:
        :param account_id:  account_id:
        :param order_types:  order_types:
        :param symbol: The symbol, like "btcusdt".
        :param order_states: order status, can be one state or many state sepearted by comma,
                             such as "submitted,partial-filled,partial-canceled,filled,canceled,created"
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(candlestick_event: 'CandlestickEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return: No return
        """
        check_should_not_none(symbol, "symbol")
        check_should_not_none(order_states, "states")
        check_should_not_none(account_id, "account-d")
        check_should_not_none(callback, "callback")
        params = {
            "symbol": symbol,
            "account-id": account_id,
            "states": order_states,
            "types": order_types,
            "start-date": start_date,
            "end-date": end_date,
            "from": from_id,
            "direct": direct,
            "size": size,
            "client-req-id": client_req_id
        }

        def subscription(connection):
            connection.send(request_order_list_channel(symbol=symbol, account_id=account_id,
                                                       states_str=order_states, client_req_id=client_req_id,
                                                       more_key=params))

        self.trade_client_socket.execute_subscribe_v1(subscription, callback, error_handler, is_trade=True)

    def req_order_detail(self, order_id: str, callback, client_req_id: str = None, error_handler=None):
        """
        Subscribe candlestick/kline event. If the candlestick/kline is updated,
        server will send the data to client and onReceive in callback will be called.

        :param order_id: order_id:
         symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
         interval: The candlestick/kline interval, MIN1, MIN5, DAY1 etc.
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(candlestick_event: 'CandlestickEvent'):
                        pass
        :param client_req_id: client request ID
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return: No return
        """
        check_should_not_none(order_id, "order_id")
        check_should_not_none(callback, "callback")

        def subscription(connection):
            connection.send(request_order_detail_channel(order_id, client_req_id))

        self.trade_client_socket.execute_subscribe_v1(subscription, callback, error_handler, is_trade=True)

    def get_order(self, order_id: int):
        """
        Get the details of an order.

        :param order_id: The order id. (mandatory)
        :return: The information of order.
        """
        check_should_not_none(order_id, "order_id")

        params = {
            "order_id": order_id,
        }

        def get_channel():
            path = "/v1/order/orders/{}"
            return path.format(order_id)

        return self.trade_client.request_process(HttpMethod.GET_SIGN, get_channel(), params)

    def get_order_by_client_order_id(self, client_order_id):
        check_should_not_none(client_order_id, "clientOrderId")

        params = {
            "clientOrderId": client_order_id,
        }
        channel = "/v1/order/orders/getClientOrder"
        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_orders(self, symbol: str, order_state: OrderState, order_type: OrderType = None,
                   start_date: str = None, end_date: str = None, start_id: 'int' = None,
                   size: 'int' = None, direct=None) -> list:
        check_symbol(symbol)
        check_should_not_none(order_state, "order_state")
        start_date = format_date(start_date, "start_date")
        end_date = format_date(end_date, "end_date")

        params = {
            "symbol": symbol,
            "types": order_type,
            "start-date": start_date,
            "end-date": end_date,
            "from": start_id,
            "states": order_state,
            "size": size,
            "direct": direct
        }
        channel = "/v1/order/orders"

        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_open_orders(self, symbol: str, account_id: 'int', side: 'OrderSide' = None,
                        size: 'int' = None, from_id=None, direct=None) -> list:
        """
        The request of get open orders.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param account_id: account id (mandatory)
        :param side: The order side, buy or sell. If no side defined, will return all open orders of the account.
                    (optional)
        :param size: The number of orders to return. Range is [1, 500]. (optional)
        :param direct: 1:prev  order by ID asc from from_id, 2:next order by ID desc from from_id
        :param from_id: start ID for search
        :return: The orders information.
        """
        check_symbol(symbol)
        check_range(size, 1, 500, "size")
        check_should_not_none(account_id, "account_id")
        params = {
            "symbol": symbol,
            "account-id": account_id,
            "side": side,
            "size": size,
            "from": from_id,
            "direct": direct
        }
        channel = "/v1/order/openOrders"

        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_history_orders(self, symbol=None, start_time=None, end_time=None, size=None, direct=None) -> list:
        """
        Transfer Asset between Futures and Contract.

        :param direct:
        :param symbol: The target sub account uid to transfer to or from. (optional)
        :param start_time: The crypto currency to transfer. (optional)
        :param end_time: The amount of asset to transfer. (optional)
        :param size: The type of transfer, need be "futures-to-pro" or "pro-to-futures" (optional)
        :return: The Order list.
        """
        params = {
            "symbol": symbol,
            "start-time": start_time,
            "end-time": end_time,
            "size": size,
            "direct": direct
        }
        channel = "/v1/order/history"

        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_match_result(self, symbol: str, order_type: OrderSide = None, start_date: str = None,
                         end_date: str = None,
                         size: 'int' = None,
                         from_id: 'int' = None,
                         direct: str = None):
        """
        Search for the trade records of an account.

        :param direct:  direct
        :param symbol: The symbol, like "btcusdt" (mandatory).
        :param order_type: The types of order to include in the search (optional).
        :param start_date: Search starts date in format yyyy-mm-dd. (optional).
        :param end_date: Search ends date in format yyyy-mm-dd. (optional).
        :param size: The number of orders to return, range [1-100]. (optional).
        :param from_id: Search order id to begin with. (optional).
        :return:
        """

        check_symbol(symbol)
        start_date = format_date(start_date, "start_date")
        end_date = format_date(end_date, "end_date")
        check_range(size, 1, 100, "size")

        params = {
            "symbol": symbol,
            "start-date": start_date,
            "end-date": end_date,
            "types": order_type,
            "size": size,
            "from": from_id,
            "direct": direct
        }
        channel = "/v1/order/matchresults"

        return self.trade_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_match_results_by_order_id(self, order_id: int) -> list:
        """
        Get detail match results of an order.

        :param order_id: The order id. (mandatory)
        :return: The list of match result.
        """
        check_should_not_none(order_id, "order_id")

        params = {
            "order_id": order_id
        }

        def get_channel():
            path = "/v1/order/orders/{}/matchresults"
            return path.format(order_id)

        return self.trade_client.request_process(HttpMethod.GET_SIGN, get_channel(), params)

    @staticmethod
    def order_source_desc(account_type):
        default_source = "api"
        if account_type:
            if account_type == AccountType.MARGIN:
                return "margin-api"
        return default_source

    @staticmethod
    def create_order_param_check(symbol: str, account_id: int, order_type: OrderType, amount: float,
                                 price: float, source: str, client_order_id=None, stop_price=None, operator=None):
        check_symbol(symbol)
        check_should_not_none(account_id, "account_id")
        check_should_not_none(order_type, "order_type")
        check_should_not_none(amount, "amount")
        check_should_not_none(source, "source")

        if order_type == OrderType.SELL_LIMIT \
                or order_type == OrderType.BUY_LIMIT \
                or order_type == OrderType.BUY_LIMIT_MAKER \
                or order_type == OrderType.SELL_LIMIT_MAKER:
            check_should_not_none(price, "price")
        if order_type in [OrderType.SELL_MARKET, OrderType.BUY_MARKET]:
            price = None

        params = {
            "account-id": account_id,
            "amount": amount,
            "price": price,
            "symbol": symbol,
            "type": order_type,
            "source": source,
            "client-order-id": client_order_id,
            "stop-price": stop_price,
            "operator": operator
        }

        return params

    def create_order(self, symbol: str, account_id: 'int', order_type: 'OrderType', amount: 'float',
                     price: 'float', source: str, client_order_id=None, stop_price=None, operator=None) -> int:
        """
        Make an order in huobi.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param account_id: Account id. (mandatory)
        :param order_type: The order type. (mandatory)
        :param source: The order source. (mandatory)
                for spot, it's "api", see OrderSource.API
                for margin, it's "margin-api", see OrderSource.MARGIN_API
                for super margin, it's "super-margin-api", see OrderSource.SUPER_MARGIN_API
        :param amount: The amount to buy (quote currency) or to sell (base currency). (mandatory)
        :param price: The limit price of limit order, only needed for limit order.
                      (mandatory for buy-limit, sell-limit, buy-limit-maker and sell-limit-maker)
        :param client_order_id: unique Id which is user defined and must be unique in recent 24 hours
        :param stop_price: Price for auto sell to get the max benefit
        :param operator: the condition for stop_price, value can be "gte" or "lte",
                         gte – greater than and equal (>=), lte – less than and equal (<=)
        :return: The order id.
        """

        params = self.create_order_param_check(symbol, account_id, order_type, amount,
                                               price, source, client_order_id, stop_price, operator)
        channel = "/v1/order/orders/place"

        return self.trade_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def create_spot_order(self, symbol: str, account_id: 'int', order_type: 'OrderType', amount: 'float',
                          price: 'float', client_order_id=None, stop_price=None,
                          operator=None) -> int:
        order_source = OrderSource.API
        return self.create_order(symbol=symbol, account_id=account_id, order_type=order_type, amount=amount,
                                 price=price, source=order_source, client_order_id=client_order_id,
                                 stop_price=stop_price,
                                 operator=operator)

    def create_margin_order(self, symbol: str, account_id: 'int', order_type: 'OrderType', amount: 'float',
                            price: 'float', client_order_id=None, stop_price=None,
                            operator=None) -> int:
        order_source = OrderSource.MARGIN_API
        return self.create_order(symbol=symbol, account_id=account_id, order_type=order_type, amount=amount,
                                 price=price, source=order_source, client_order_id=client_order_id,
                                 stop_price=stop_price,
                                 operator=operator)

    def create_super_margin_order(self, symbol: str, account_id: 'int', order_type: 'OrderType', amount: 'float',
                                  price: 'float', client_order_id=None, stop_price=None,
                                  operator=None) -> int:
        order_source = OrderSource.SUPER_MARGIN_API
        return self.create_order(symbol=symbol, account_id=account_id, order_type=order_type, amount=amount,
                                 price=price, source=order_source, client_order_id=client_order_id,
                                 stop_price=stop_price,
                                 operator=operator)

    def cancel_order(self, symbol, order_id):
        check_symbol(symbol)
        check_should_not_none(order_id, "order_id")

        params = {
            "order_id": order_id
        }
        order_id = params["order_id"]

        def get_channel():
            path = "/v1/order/orders/{}/submitcancel"
            return path.format(order_id)

        return self.trade_client.request_process(HttpMethod.POST_SIGN, get_channel(), params)

    def cancel_orders(self, symbol, order_id_list):
        """
        Submit cancel request for cancelling multiple orders.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param order_id_list: The list of order id. the max size is 50. (mandatory)
        :return: No return
        """
        check_symbol(symbol)
        check_should_not_none(order_id_list, "order_id_list")
        check_list(order_id_list, 1, 50, "order_id_list")

        string_list = list()
        for order_id in order_id_list:
            string_list.append(str(order_id))

        params = {
            "order-ids": string_list
        }
        channel = "/v1/order/orders/batchcancel"
        return self.trade_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def cancel_open_orders(self, account_id, symbols: str = None, side=None, size=None):
        """
        Request to cancel open orders.

        :param account_id:  account_id
        :param symbols: The symbol, like "btcusdt".
        :param side: The order side, buy or sell. If no side defined, will cancel all open orders of the account.
                    (optional)
        :param size: The number of orders to cancel. Range is [1, 100]. (optional)
        :return: Status of batch cancel result.
        """
        check_should_not_none(account_id, "account_id")

        params = {
            "account-id": account_id,
            "symbol": symbols,
            "side": side,
            "size": size
        }
        channel = "/v1/order/orders/batchCancelOpenOrders"

        return self.trade_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def cancel_client_order(self, client_order_id) -> int:
        """
        Request to cancel open orders.

        :param client_order_id: user defined unique order id
        """
        check_should_not_none(client_order_id, "client-order-id")

        params = {
            "client-order-id": client_order_id
        }
        channel = "/v1/order/orders/submitCancelClientOrder"

        return self.trade_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def transfer_between_futures_and_pro(self, currency: str, amount: 'float',
                                         transfer_type: TransferFuturesPro) -> int:
        """
        Transfer Asset between Futures and Contract.

        :sub_uid: The target sub account uid to transfer to or from. (mandatory)
        :param currency: The crypto currency to transfer. (mandatory)
        :param amount: The amount of asset to transfer. (mandatory)
        :param transfer_type: The type of transfer, need be "futures-to-pro" or "pro-to-futures" (mandatory)
        :return: The order id.
        """
        check_currency(currency)
        check_should_not_none(currency, "currency")
        check_should_not_none(amount, "amount")
        check_should_not_none(transfer_type, "transfer_type")
        params = {
            "currency": currency,
            "amount": amount,
            "type": transfer_type

        }
        channel = "/v1/futures/transfer"

        return self.trade_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def batch_create_order(self, order_config_list) -> int:
        """
        Make an order in huobi.
        :param order_config_list: order config list, it can batch create orders, and each order config check as below
            : items as below
                : symbol: The symbol, like "btcusdt". (mandatory)
                : account_type: Account type. (mandatory)
                : order_type: The order type. (mandatory)
                : amount: The amount to buy (quote currency) or to sell (base currency). (mandatory)
                : price: The limit price of limit order, only needed for limit order.
                        (mandatory for buy-limit, sell-limit, buy-limit-maker and sell-limit-maker)
                : client_order_id: unique Id which is user defined and must be unique in recent 24 hours
                : stop_price: Price for auto sell to get the max benefit
                : operator: the condition for stop_price, value can be "gte" or "lte",
                            gte – greater than and equal (>=), lte – less than and equal (<=)
        :return: The order id.
        """

        check_should_not_none(order_config_list, "order_config_list")
        check_list(order_config_list, 1, 10, "create order config list")

        new_config_list = list()
        for item in order_config_list:
            new_item = self.create_order_param_check(
                item.get("symbol", None),
                item.get("account_id", None),
                item.get("order_type", None),
                item.get("amount", None),
                item.get("price", None),
                item.get("source", None),
                item.get("client_order_id", None),
                item.get("stop-price", None),
                item.get("operator", None))

            new_config_list.append(new_item)

        channel = "/v1/order/batch-orders"

        return self.trade_client.request_process_post_batch(HttpMethod.POST_SIGN, channel, new_config_list)

    def sub_trade_clearing(self, symbols: str, callback, error_handler=None):
        """
        Subscribe trade clearing by symbol

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
                        "*" for all symbols
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """
        check_should_not_none(symbols, "symbols")
        symbol_list = symbols.split(",")
        if "*" in symbol_list:
            symbol_list = ["*"]
        else:
            check_symbol_list(symbol_list)

        check_should_not_none(callback, "callback")

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(trade_clearing_channel(symbol))
                time.sleep(0.01)

        return self.trade_client_socket_sub.execute_subscribe_v2(subscription, callback, error_handler, is_trade=True)
