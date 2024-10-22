import json
import time

from funcoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from funcoin.huobi.constant import (CandlestickInterval, DepthStep,
                                     HttpMethod, MbpLevel)
from funcoin.huobi.utils import get_current_timestamp
from funcoin.huobi.utils.input_checker import (check_in_list, check_range,
                                                check_should_not_none,
                                                check_symbol,
                                                check_symbol_list)
from funcoin.huobi.utils.model import Response


class MarketClient(object):

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
        self.market_service = RestApiSyncClient(*args, **kwargs)
        self.market_service_sub = SubscribeClient(**kwargs)
        self.market_service_socket = WebSocketReqClient(**kwargs)

    def get_candlestick(self, symbol, period, size=200):
        """
        Get the candlestick/kline for the specified symbol. The data number is 150 as default.

        :param symbol: The symbol, like "btcusdt". To query hb10, put "hb10" at here. (mandatory)
        :param period: The candlestick/kline interval, MIN1, MIN5, DAY1 etc. (mandatory)
        :param size: The start time of of requested candlestick/kline data. (optional)
        :return: The list of candlestick/kline data.
        """
        check_symbol(symbol)
        check_should_not_none(period, "period")
        check_range(size, 1, 2000, "size")

        params = {
            "symbol": symbol,
            "period": period,
            "size": size
        }
        channel = "/market/history/kline"
        return self.market_service.request_process(HttpMethod.GET, channel, params)

    def sub_candlestick(self, symbols: str, interval: CandlestickInterval, callback, error_handler):
        """
        Subscribe candlestick/kline event. If the candlestick/kline is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param interval: The candlestick/kline interval, MIN1, MIN5, DAY1 etc.
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(candlestick_event: 'CandlestickEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
        between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return: No return
        """

        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(interval, "interval")
        check_should_not_none(callback, "callback")

        def kline_channel(symbol, _interval):
            channel = dict()
            channel["sub"] = "market." + symbol + ".kline." + _interval
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(kline_channel(symbol, interval))
                time.sleep(0.01)

        self.market_service_sub.execute_subscribe_v1(subscription, callback, error_handler)

    def req_candlestick(self, symbols: str, interval: CandlestickInterval, callback,
                        from_ts_second=None, end_ts_second=None, error_handler=None):
        """
        Subscribe candlestick/kline event. If the candlestick/kline is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param interval: The candlestick/kline interval, MIN1, MIN5, DAY1 etc.
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(candlestick_event: 'CandlestickEvent'):
                        pass
        :param from_ts_second : data from timestamp [it's second]
        :param end_ts_second : data util timestamp [it's second]
        :param error_handler: The error handler will be called if subscription failed or error happen
        between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return: No return
        """

        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(interval, "interval")
        check_should_not_none(callback, "callback")

        def request_kline_channel(symbol, _interval, _from_ts_second=None, to_ts_second=None):
            channel = dict()
            channel["req"] = "market." + symbol + ".kline." + _interval
            channel["id"] = str(get_current_timestamp())
            if _from_ts_second:
                channel["from"] = int(_from_ts_second)
            if to_ts_second:
                channel["to"] = int(to_ts_second)
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(request_kline_channel(symbol, interval, from_ts_second, end_ts_second))
                time.sleep(0.01)

        self.market_service_socket.execute_subscribe_v1(subscription, callback, error_handler)

    def get_price_depth(self, symbol: str, depth_type: str, depth_size: int = None):
        """
        Get the Market Depth of a symbol.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param depth_type: The tpye, like "step0" to "step5". (mandatory)
        :param depth_size: (optional) The maximum number of Market Depth step0 requested. range [1 - 150],
                           default is 150
                           The maximum number of Market Depth step1,step2,step3,step4,step5 requested.
                           size is in [5, 10, 20], default is 20.
        :return: Market Depth data.
        """
        channel = "/market/depth"

        check_symbol(symbol)
        check_in_list(depth_type, [DepthStep.STEP0, DepthStep.STEP1, DepthStep.STEP2, DepthStep.STEP3, DepthStep.STEP4,
                                   DepthStep.STEP5], "depth_type")
        params = {
            "symbol": symbol,
            "type": depth_type,
            # "depth": depth_size
        }

        ret_data = self.market_service.request_process(HttpMethod.GET, channel, params)

        if depth_size is not None:
            if (ret_data.bids is not None) and (len(ret_data.bids) > depth_size):
                ret_data.bids = ret_data.bids[0:depth_size]

            if (ret_data.asks is not None) and (len(ret_data.asks) > depth_size):
                ret_data.asks = ret_data.asks[0:depth_size]

        return ret_data

    @staticmethod
    def get_depth_step_list():
        return [DepthStep.STEP0, DepthStep.STEP1, DepthStep.STEP2, DepthStep.STEP3, DepthStep.STEP4, DepthStep.STEP5]

    @staticmethod
    def get_valid_depth_step(value, defalut_value):
        step_list = MarketClient.get_depth_step_list()
        if value in step_list:
            return value
        else:
            return defalut_value

    def sub_price_depth(self, symbols: str, depth_step: str, callback, error_handler=None):
        """
        Subscribe price depth event. If the price depth is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param depth_step: The depth precision, string from step0 to step5.
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        step = MarketClient.get_valid_depth_step(value=depth_step, defalut_value=DepthStep.STEP0)
        check_should_not_none(callback, "callback")

        def price_depth_channel(symbol, step_type=DepthStep.STEP0):
            channel = dict()
            channel["sub"] = "market." + symbol + ".depth." + step_type
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(price_depth_channel(symbol, step))
                time.sleep(0.01)

        self.market_service_sub.execute_subscribe_v1(subscription, callback, error_handler)

    def sub_price_depth_bbo(self, symbols: str, callback, error_handler=None):
        """
        Subscribe price depth event. If the price depth is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
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

        def price_depth_bbo_channel(symbol):
            channel = dict()
            channel["sub"] = "market." + symbol + ".bbo"
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(price_depth_bbo_channel(symbol))
                time.sleep(0.01)

        self.market_service_sub.execute_subscribe_v1(subscription, callback, error_handler)

    def req_price_depth(self, symbols: str, depth_step: str, callback, error_handler=None):
        """
        Subscribe price depth event. If the price depth is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param depth_step: The depth precision, string from step0 to step5.
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        step = MarketClient.get_valid_depth_step(value=depth_step, defalut_value=DepthStep.STEP0)
        check_should_not_none(callback, "callback")

        def request_price_depth_channel(symbol, step_type="step0"):
            channel = dict()
            channel["req"] = "market." + symbol + ".depth." + step_type
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(request_price_depth_channel(symbol, step))
                time.sleep(0.01)

        self.market_service_socket.execute_subscribe_v1(subscription, callback, error_handler)

    def get_market_detail(self, symbol: str):
        """
        Get trade statistics in 24 hours.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :return: Trade statistics.
        """
        channel = "/market/detail"

        check_symbol(symbol)

        params = {
            "symbol": symbol,
        }

        return self.market_service.request_process(HttpMethod.GET, channel, params)

    def sub_market_detail(self, symbols: str, callback, error_handler=None):
        """
        Subscribe 24 hours trade statistics event. If statistics is generated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(trade_statistics_event: 'TradeStatisticsEvent'):
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

        def market_detail_channel(_symbol):
            channel = dict()
            channel["sub"] = "market." + _symbol + ".detail"
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(market_detail_channel(symbol))
                time.sleep(0.01)

        self.market_service_sub.execute_subscribe_v1(subscription, callback, error_handler)

    def req_market_detail(self, symbols: str, callback, error_handler=None):
        """
        Subscribe 24 hours trade statistics event. If statistics is generated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(trade_statistics_event: 'TradeStatisticsEvent'):
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

        def request_market_detail_channel(_symbol):
            channel = dict()
            channel["req"] = "market." + _symbol + ".detail"
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(request_market_detail_channel(symbol))
                time.sleep(0.01)

        self.market_service_socket.execute_subscribe_v1(subscription, callback, error_handler)

    def get_market_trade(self, symbol: str) -> list:
        """
        Get the most recent trades with their price, volume and direction.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :return: The list of trade.
        """
        channel = "/market/trade"

        check_symbol(symbol)

        params = {
            "symbol": symbol,
        }

        return self.market_service.request_process(HttpMethod.GET, channel, params)

    def get_history_trade(self, symbol: str, size: 'int' = None) -> list:
        """
        Get the most recent trades with their price, volume and direction.

        :param symbol: The symbol, like "btcusdt". (mandatory)
        :param size: The number of historical trade requested, range [1 - 2000] (optional)
        :return: The list of trade.
        """
        channel = "/market/history/trade"

        check_symbol(symbol)
        check_range(size, 1, 2000, "size")

        params = {
            "symbol": symbol,
            "size": size
        }

        return self.market_service.request_process(HttpMethod.GET, channel, params)

    def sub_trade_detail(self, symbols: str, callback, error_handler=None):
        """
        Subscribe price depth event. If the price depth is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(trade_event: 'TradeEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error
        happen between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return:  No return
        """
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(callback, "callback")

        def trade_detail_channel(symbol):
            channel = dict()
            channel["sub"] = "market." + symbol + ".trade.detail"
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(trade_detail_channel(symbol))
                time.sleep(0.01)

        self.market_service_sub.execute_subscribe_v1(subscription, callback, error_handler)

    def req_trade_detail(self, symbols: str, callback, error_handler=None):
        """
        Subscribe price depth event. If the price depth is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(trade_event: 'TradeEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error
        happen between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return:  No return
        """
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(callback, "callback")

        def request_trade_detail_channel(symbol):
            channel = dict()
            channel["req"] = "market." + symbol + ".trade.detail"
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(request_trade_detail_channel(symbol))
                time.sleep(0.01)

        self.market_service_socket.execute_subscribe_v1(subscription, callback, error_handler)

    def get_market_detail_merged(self, symbol):
        check_symbol(symbol)
        params = {
            "symbol": symbol
        }
        channel = "/market/detail/merged"
        return self.market_service.request_process(HttpMethod.GET, channel, params)

    def get_market_tickers(self) -> Response:
        """
        get market tickers

        :return: market ticker list.
        """

        params = {}
        channel = "/market/tickers"
        return self.market_service.request_process(HttpMethod.GET, channel, params)

    """
    increase mbp(market by price)
    """

    def sub_mbp_increase(self, symbols: str, levels: 'int', callback, error_handler=None):
        """
        Subscribe mbp event. If the mbp is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param levels: level, 5，10，20，150. current only support 150
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
        between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """
        check_should_not_none(symbols, "symbol")
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(levels, "levels")
        check_should_not_none(callback, "callback")

        def mbp_increase_channel(symbol, level):
            channel = dict()
            channel["sub"] = "market.{symbol}.mbp.{level}".format(symbol=symbol, level=level)
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(mbp_increase_channel(symbol, levels))
                time.sleep(0.01)

        return self.market_service_sub.execute_subscribe_mbp(subscription, callback, error_handler)

    """
    subscribe full mbp(market by price)
    """

    def sub_mbp_full(self, symbols: str, levels: 'int', callback, error_handler=None):
        """
        Subscribe full mbp event. If the mbp is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param levels: level, 5，10，20
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
        between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """
        check_should_not_none(symbols, "symbol")
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(levels, "levels")
        check_in_list(levels, [MbpLevel.MBP5, MbpLevel.MBP10, MbpLevel.MBP20], "levels")
        check_should_not_none(callback, "callback")

        def mbp_full_channel(symbol, level):
            channel = dict()
            channel["sub"] = "market.{symbol}.mbp.refresh.{level}".format(symbol=symbol, level=level)
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(mbp_full_channel(symbol, levels))
                time.sleep(0.01)

        self.market_service_sub.execute_subscribe_v1(subscription, callback, error_handler)

    def req_mbp(self, symbols: str, levels: 'int', callback, auto_close=True, error_handler=None):
        """
        Subscribe mbp event. If the mbp is updated,
        server will send the data to client and onReceive in callback will be called.

        :param symbols: The symbols, like "btcusdt". Use comma to separate multi symbols, like "btcusdt,ethusdt".
        :param levels: level, 5，10，20，150. current only support 150
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param auto_close : close websocket connection after get data
        :param error_handler: The error handler will be called if subscription failed or error happen
        between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """
        check_should_not_none(symbols, "symbol")
        symbol_list = symbols.split(",")
        check_symbol_list(symbol_list)
        check_should_not_none(levels, "levels")
        check_should_not_none(callback, "callback")

        def request_mbp_channel(symbol, level):
            channel = dict()
            channel["req"] = "market.{symbol}.mbp.{level}".format(symbol=symbol, level=level)
            channel["id"] = str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            for symbol in symbol_list:
                connection.send(request_mbp_channel(symbol, levels))
                time.sleep(0.01)

        self.market_service_socket.execute_subscribe_mbp(subscription, callback, error_handler)
