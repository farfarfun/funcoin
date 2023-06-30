from funcoin.okex.client.base import BaseClient
from funcoin.okex.consts import GET, POST


class AssetClient(BaseClient):

    def __init__(self, *args, **kwargs):
        super(AssetClient, self).__init__(*args, **kwargs)

    def get_deposit_address(self, ccy):
        uri = '/api/v5/asset/deposit-address'
        params = {'ccy': ccy}
        return self._request_with_params(GET, uri, params)

    def get_balances(self, ccy=None):
        uri = '/api/v5/asset/balances'
        params = {'ccy': ccy}
        return self._request_with_params(GET, uri, params)

    def funds_transfer(self, ccy, amt, froms, to, type='0', subAcct=None, instId=None, toInstId=None):
        uri = '/api/v5/asset/transfer'
        params = {'ccy': ccy, 'amt': amt, 'from': froms, 'to': to, 'type': type, 'subAcct': subAcct, 'instId': instId,
                  'toInstId': toInstId}
        return self._request_with_params(POST, uri, params)

    def coin_withdraw(self, ccy, amt, dest, toAddr, pwd, fee):
        uri = '/api/v5/asset/withdrawal'
        params = {'ccy': ccy, 'amt': amt, 'dest': dest, 'toAddr': toAddr, 'pwd': pwd, 'fee': fee}
        return self._request_with_params(POST, uri, params)

    def get_deposit_history(self, ccy=None, state=None, after=None, before=None, limit=None):
        uri = '/api/v5/asset/deposit-history'
        params = {'ccy': ccy, 'state': state, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_withdrawal_history(self, ccy=None, state=None, after=None, before=None, limit=None):
        uri = '/api/v5/asset/withdrawal-history'
        params = {'ccy': ccy, 'state': state, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_currency(self):
        uri = '/api/v5/asset/currencies'
        return self._request_without_params(GET, uri)

    def purchase_redempt(self, ccy, amt, side):
        uri = '/api/v5/asset/purchase_redempt'
        params = {'ccy': ccy, 'amt': amt, 'side': side}
        return self._request_with_params(POST, uri, params)

    def get_bills(self, ccy=None, type=None, after=None, before=None, limit=None):
        uri = '/api/v5/asset/bills'
        params = {'ccy': ccy,         'type': type,         'after': after,         'before': before,         'limit': limit}
        return self._request_with_params(GET, uri, params)

        
