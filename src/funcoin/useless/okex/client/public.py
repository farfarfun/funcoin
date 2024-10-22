from funcoin.okex.client.base import BaseClient
from funcoin.okex.consts import GET


class PublicClient(BaseClient):

    def __init__(self, *args, **kwargs):
        super(PublicClient, self).__init__(*args, **kwargs)

    def get_instruments(self, instType, uly=None, instId=None):
        uri = '/api/v5/public/instruments'
        params = {'instType': instType, 'uly': uly, 'instId': instId}
        return self._request_with_params(GET, uri, params)

    def get_deliver_history(self, instType, uly, after=None, before=None, limit=None):
        uri = '/api/v5/public/delivery-exercise-history'
        params = {'instType': instType, 'uly': uly, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_open_interest(self, instType, uly=None, instId=None):
        uri = '/api/v5/public/open-interest'
        params = {'instType': instType, 'uly': uly, 'instId': instId}
        return self._request_with_params(GET, uri, params)

    def get_funding_rate(self, instId):
        uri = '/api/v5/public/funding-rate'
        params = {'instId': instId}
        return self._request_with_params(GET, uri, params)

    def funding_rate_history(self, instId, after=None, before=None, limit=None):
        uri = '/api/v5/public/funding-rate-history'
        params = {'instId': instId, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_price_limit(self, instId):
        uri = '/api/v5/public/price-limit'
        params = {'instId': instId}
        return self._request_with_params(GET, uri, params)

    def get_opt_summary(self, uly, expTime=None):
        uri = '/api/v5/public/opt-summary'
        params = {'uly': uly, 'expTime': expTime}
        return self._request_with_params(GET, uri, params)

    def get_estimated_price(self, instId):
        uri = '/api/v5/public/estimated-price'
        params = {'instId': instId}
        return self._request_with_params(GET, uri, params)

    def discount_interest_free_quota(self, ccy=None):
        uri = '/api/v5/public/discount-rate-interest-free-quota'
        params = {'ccy': ccy}
        return self._request_with_params(GET, uri, params)

    def get_system_time(self):
        uri = '/api/v5/public/time'
        return self._request_without_params(GET, uri)

    def get_liquidation_orders(self, instType, mgnMode=None, instId=None, ccy=None, uly=None,
                               alias=None, state=None, before=None, after=None, limit=None):
        uri = '/api/v5/public/liquidation-orders'
        params = {'instType': instType, 'mgnMode': mgnMode, 'instId': instId, 'ccy': ccy, 'uly': uly,
                  'alias': alias, 'state': state, 'before': before, 'after': after, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_mark_price(self, instType, uly=None, instId=None):
        uri = '/api/v5/public/mark-price'
        params = {'instType': instType, 'uly': uly, 'instId': instId}
        return self._request_with_params(GET, uri, params)

    def get_tier(self, instType, tdMode, uly=None, instId=None, ccy=None, tier=None):
        uri = '/api/v5/public/mark-price'
        params = {'instType': instType, 'tdMode': tdMode, 'uly': uly, 'instId': instId, 'ccy': ccy, 'tier': tier}
        return self._request_with_params(GET, uri, params)
