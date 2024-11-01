class BaseChannel:
    def __init__(self, channel=None, *args, **kwargs):
        """
        :param channel: 频道名
        """
        self.json = {"channel": channel}

    def to_json(self):
        return self.json

    def put(self, key, value):
        if value is not None:
            self.json[key] = value


class PublicChannel(BaseChannel):
    def __init__(self, instType=None, instId=None, uly=None, *args, **kwargs):
        """
        :param instType: 产品类型
        :param instId: 产品ID
        :param uly: 合约标的指数
        """
        super(PublicChannel, self).__init__(*args, **kwargs)
        self.put("instType", instType)
        self.put("instId", instId)
        self.put("uly", uly)

    @staticmethod
    def public_instruments(instType="FUTURES"):
        """
        产品频道
        """
        return PublicChannel(channel="instruments", instType=instType)

    @staticmethod
    def public_tickers(instId="BTC-USDT"):
        """
        行情频道
        """
        return PublicChannel(channel="tickers", instId=instId)

    @staticmethod
    def public_open_interest(instId="BTC-USDT"):
        """
        持仓总量频道
        """
        return PublicChannel(channel="open-interest", instId=instId)

    @staticmethod
    def public_kline(channel="candle1m", instId="BTC-USDT"):
        """
        K线频道
        candle1Y
        candle6M candle3M candle1M
        candle1W
        candle1D candle2D candle3D candle5D
        candle12H candle6H candle4H candle2H candle1H
        candle30m candle15m candle5m candle3m candle1m
        candle1Yutc candle3Mutc candle1Mutc candle1Wutc candle1Dutc candle2Dutc candle3Dutc candle5Dutc candle12Hutc candle6Hutc
        """
        return PublicChannel(channel=channel, instId=instId)

    @staticmethod
    def public_trades(instId="BTC-USDT"):
        """
        交易频道
        """
        return PublicChannel(channel="trades", instId=instId)

    @staticmethod
    def public_estimated_price(instType='FUTURES', uly="BTC-USDT"):
        """
        预估交割/行权价格频道
        """
        return PublicChannel(channel="estimated-price", instType=instType, uly=uly)

    @staticmethod
    def public_mark_price(instId="BTC-USDT"):
        """
        标记价格频道
        """
        return PublicChannel(channel="mark-price", instId=instId)

    @staticmethod
    def public_mark_price_kline(channel="", instId="BTC-USDT"):
        """
        标记价格K线频道
        mark-price-candle1Y
        mark-price-candle6M
        mark-price-candle3M
        mark-price-candle1M
        mark-price-candle1W
        mark-price-candle1D
        mark-price-candle2D
        mark-price-candle3D
        mark-price-candle5D
        mark-price-candle12H
        mark-price-candle6H
        mark-price-candle4H
        mark-price-candle2H
        mark-price-candle1H
        mark-price-candle30m
        mark-price-candle15m
        mark-price-candle5m
        mark-price-candle3m
        mark-price-candle1m
        mark-price-candle1Yutc
        mark-price-candle3Mutc
        mark-price-candle1Mutc
        mark-price-candle1Wutc
        mark-price-candle1Dutc
        mark-price-candle2Dutc
        mark-price-candle3Dutc
        mark-price-candle5Dutc
        mark-price-candle12Hutc
        mark-price-candle6Hutc
        """
        return PublicChannel(channel=channel, instId=instId)

    @staticmethod
    def public_price_limit(instId="BTC-USDT"):
        """
        限价频道
        """
        return PublicChannel(channel="price-limit", instId=instId)

    @staticmethod
    def public_books(instId="BTC-USDT"):
        """
        深度频道
        """
        return PublicChannel(channel="books", instId=instId)

    @staticmethod
    def public_opt_summary(uly="BTC-USDT"):
        """
        期权定价频道
        """
        return PublicChannel(channel="opt-summary", uly=uly)

    @staticmethod
    def public_funding_rate(instId="BTC-USDT"):
        """
        资金费率频道
        """
        return PublicChannel(channel="funding-rate", instId=instId)

    @staticmethod
    def public_index_kline(channel, instId="BTC-USDT"):
        """
        指数K线频道
        index-candle1Y
        index-candle6M
        index-candle3M
        index-candle1M
        index-candle1W
        index-candle1D
        index-candle2D
        index-candle3D
        index-candle5D
        index-candle12H
        index-candle6H
        index-candle4H
        index -candle2H
        index-candle1H
        index-candle30m
        index-candle15m
        index-candle5m
        index-candle3m
        index-candle1m
        index-candle1Yutc
        index-candle3Mutc
        index-candle1Mutc
        index-candle1Wutc
        index-candle1Dutc
        index-candle2Dutc
        index-candle3Dutc
        index-candle5Dutc
        index-candle12Hutc
        index-candle6Hutc
        """
        return PublicChannel(channel=channel, instId=instId)

    @staticmethod
    def public_index_tickers(instId="BTC-USDT"):
        """
        指数行情频道
        """
        return PublicChannel(channel="index-tickers", instId=instId)

    @staticmethod
    def public_status():
        """
        指数行情频道
        """
        return PublicChannel(channel="status")


class PrivateChannel(BaseChannel):
    def __init__(self, ccy=None, instType=None, uly=None, instId=None, *args, **kwargs):
        """
        :param instType: 产品类型
        :param instId: 产品ID
        :param uly: 合约标的指数
        :param ccy: 币种
        """
        super(PrivateChannel, self).__init__(*args, **kwargs)
        self.put("instType", instType)
        self.put("instId", instId)
        self.put("ccy", ccy)
        self.put("uly", uly)

    @staticmethod
    def private_instruments(ccy="BTC"):
        """
        账户频道
        """
        return PrivateChannel(channel="account", ccy=ccy)

    @staticmethod
    def private_positions(instType="FUTURES", uly="BTC-USDT", instId="BTC-USDT"):
        """
        持仓频道
        """
        return PrivateChannel(channel="positions", instType=instType, uly=uly, instId=instId)

    @staticmethod
    def private_orders(instType="FUTURES", uly="BTC-USDT", instId="BTC-USDT"):
        """
        持仓频道
        """
        return PrivateChannel(channel="orders", instType=instType, uly=uly, instId=instId)

    @staticmethod
    def private_orders_algo(instType="FUTURES", uly="BTC-USDT", instId="BTC-USDT"):
        """
        持仓频道
        """
        return PrivateChannel(channel="orders-algo", instType=instType, uly=uly, instId=instId)


'''
交易 trade
'''

# 下单
# trade_param = {"id": "1512", "op": "order", "args": [{"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19777", "sz": "1"}]}
# 批量下单
# trade_param = {"id": "1512", "op": "batch-orders", "args": [
#         {"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19666", "sz": "1"},
#         {"side": "buy", "instId": "BTC-USDT", "tdMode": "isolated", "ordType": "limit", "px": "19633", "sz": "1"}
#     ]}
# 撤单
# trade_param = {"id": "1512", "op": "cancel-order", "args": [{"instId": "BTC-USDT", "ordId": "259424589042823169"}]}
# 批量撤单
# trade_param = {"id": "1512", "op": "batch-cancel-orders", "args": [
#         {"instId": "BTC-USDT", "ordId": "259432098826694656"},
#         {"instId": "BTC-USDT", "ordId": "259432098826694658"}
#     ]}
# 改单
# trade_param = {"id": "1512", "op": "amend-order", "args": [{"instId": "BTC-USDT", "ordId": "259432767558135808", "newSz": "2"}]}
# 批量改单
# trade_param = {"id": "1512", "op": "batch-amend-orders", "args": [
#         {"instId": "BTC-USDT", "ordId": "259435442492289024", "newSz": "2"},
#         {"instId": "BTC-USDT", "ordId": "259435442496483328", "newSz": "3"}
#     ]}
