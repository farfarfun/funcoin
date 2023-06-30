from funcoin.okex.client.base import BaseClient
from funcoin.okex.consts import GET


class MarketClient(BaseClient):

    def __init__(self, *args, **kwargs):
        super(MarketClient, self).__init__(*args, **kwargs)

    def get_tickers(self, instType, uly=None):
        uri = '/api/v5/market/tickers'
        if uly:
            params = {'instType': instType, 'uly': uly}
        else:
            params = {'instType': instType}
        return self._request_with_params(GET, uri, params)

    def get_ticker(self, instId):
        uri = '/api/v5/market/ticker'
        params = {'instId': instId}
        return self._request_with_params(GET, uri, params)

    def get_index_ticker(self, quoteCcy=None, instId=None):
        uri = '/api/v5/market/index-tickers'
        params = {'quoteCcy': quoteCcy, 'instId': instId}
        return self._request_with_params(GET, uri, params)

    def get_orderbook(self, instId, sz=None):
        uri = '/api/v5/market/books'
        params = {'instId': instId, 'sz': sz}
        return self._request_with_params(GET, uri, params)

    def get_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        uri = '/api/v5/market/candles'
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_history_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        uri = '/api/v5/market/history-candles'
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_index_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        uri = '/api/v5/market/index-candles'
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_mark_price_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        uri = '/api/v5/market/mark-price-candles'
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_trades(self, instId, limit=None):
        uri = '/api/v5/market/trades'
        params = {'instId': instId, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_volume(self):
        uri = '/api/v5/market/platform-24-volume'
        return self._request_without_params(GET, uri)

    def get_oracle(self):
        uri = '/api/v5/market/oracle'
        return self._request_without_params(GET, uri)

    def get_tier(self, instType=None, tdMode=None, uly=None, instId=None, ccy=None, tier=None):
        uri = '/api/v5/public/tier'
        params = {'instType': instType, 'tdMode': tdMode, 'uly': uly, 'instId': instId, 'ccy': ccy, 'tier': tier}
        return self._request_with_params(GET, uri, params)
