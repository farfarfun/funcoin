import pandas as pd
from notecoin.okex.client.base import BaseClient
from notecoin.okex.consts import GET, POST


class AccountClient(BaseClient):
    def __init__(self, *args, **kwargs):
        super(AccountClient, self).__init__(*args, **kwargs)

    def get_position_risk(self, instType=None):
        uri = '/api/v5/account/account-position-risk'
        params = {}
        if instType:
            params['instType'] = instType
        return self._request_with_params(GET, uri, params)

    def get_account(self, ccy=None):
        uri = '/api/v5/account/balance'
        params = {}
        if ccy:
            params['ccy'] = ccy
        res = self._request_with_params(GET, uri, params)
        res.dataframe_trans = lambda x: pd.DataFrame(x[0]['details'])
        return res


    def get_positions(self, instType=None, instId=None):
        uri = '/api/v5/account/positions'
        params = {}
        if instType:
            params['instType'] = instType
        if instId:
            params['instId'] = instId
        return self._request_with_params(GET, uri, params)

    def get_bills_detail(self, instType=None, ccy=None, mgnMode=None, ctType=None, type=None,
                         subType=None, after=None, before=None, limit=None):
        uri = '/api/v5/account/bills'
        params = {'instType': instType, 'ccy': ccy, 'mgnMode': mgnMode, 'ctType': ctType, 'type': type,
                  'subType': subType, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_bills_details(self, instType=None, ccy=None, mgnMode=None, ctType=None, type=None, subType=None, after=None,
                          before=None, limit=None):
        uri = '/api/v5/account/bills-archive'
        local_vars = locals()
        params = {}
        for var_name in ['instType', 'ccy', 'mgnMode', 'ctType', 'type', 'subType', 'after', 'before', 'limit']:
            var_value = local_vars.get(var_name)
            if var_value is not None:
                params[var_name] = var_value
        return self._request_with_params(GET, uri, params)

    def get_account_config(self):
        uri = '/api/v5/account/config'
        return self._request_without_params(GET, uri)

    def get_position_mode(self, posMode):
        uri = '/api/v5/account/set-position-mode'
        params = {'posMode': posMode}
        return self._request_with_params(POST, uri, params)

    def set_leverage(self, lever, mgnMode, instId=None, ccy=None, posSide=None):
        uri = '/api/v5/account/set-leverage'
        params = {'lever': lever, 'mgnMode': mgnMode, 'instId': instId, 'ccy': ccy, 'posSide': posSide}
        return self._request_with_params(POST, uri, params)

    def get_maximum_trade_size(self, instId, tdMode, ccy=None, px=None):
        uri = '/api/v5/account/max-size'
        params = {'instId': instId, 'tdMode': tdMode, 'ccy': ccy, 'px': px}
        return self._request_with_params(GET, uri, params)

    def get_max_avail_size(self, instId, tdMode, ccy=None, reduceOnly=None):
        uri = '/api/v5/account/max-avail-size'
        params = {'instId': instId, 'tdMode': tdMode, 'ccy': ccy, 'reduceOnly': reduceOnly}
        return self._request_with_params(GET, uri, params)

    def adjustment_margin(self, instId, posSide, type, amt):
        uri = '/api/v5/account/position/margin-balance'
        params = {'instId': instId, 'posSide': posSide, 'type': type, 'amt': amt}
        return self._request_with_params(POST, uri, params)

    def get_leverage(self, instId, mgnMode):
        uri = '/api/v5/account/leverage-info'
        params = {'instId': instId, 'mgnMode': mgnMode}
        return self._request_with_params(GET, uri, params)

    def get_max_load(self, instId, mgnMode, mgnCcy):
        uri = '/api/v5/account/max-loan'
        params = {'instId': instId, 'mgnMode': mgnMode, 'mgnCcy': mgnCcy}
        return self._request_with_params(GET, uri, params)

    def get_fee_rates(self, instType, instId=None, uly=None, category=None):
        uri = '/api/v5/account/trade-fee'
        params = {'instType': instType, 'instId': instId, 'uly': uly, 'category': category}
        return self._request_with_params(GET, uri, params)

    def get_interest_accrued(self, instId=None, ccy=None, mgnMode=None, after=None, before=None, limit=None):
        uri = '/api/v5/account/interest-accrued'
        params = {'instId': instId, 'ccy': ccy, 'mgnMode': mgnMode, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_interest_rate(self, ccy=None):
        uri = '/api/v5/account/interest-rate'
        params = {'ccy': ccy}
        return self._request_with_params(GET, uri, params)

    def set_greeks(self, greeksType):
        uri = '/api/v5/account/set-greeks'
        params = {'greeksType': greeksType}
        return self._request_with_params(POST, uri, params)

    def get_max_withdrawal(self, ccy=None):
        uri = '/api/v5/account/max-withdrawal'
        params = {}
        if ccy:
            params['ccy'] = ccy
        return self._request_with_params(GET, uri, params)
